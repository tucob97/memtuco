// File: planner.rs
use crate::dbengine::*;
use crate::dberror::DbError;
use crate::tokenizer::*;
use std::fmt;

/// # The Query Planner (`planner.rs`)
///
/// /// This module is the core of the database's query processing logic. Its primary
/// /// responsibility is to take a structured `Statement` (the Abstract Syntax Tree or
/// /// AST from the parser) and convert it into a `PlanNode` tree. This tree represents
/// /// a logical plan of operations that the database executor can understand and run.
/// /// Each node in the tree is a specific relational algebra operation (like Scan,
/// /// Filter, Project, Join), and the tree structure defines the flow of data from
/// /// the raw tables up to the final result set.
///
/// ### Core Data Structures
///
/// /// The planner's output is built using two main recursive enums:
///
/// /// 1.  `PlanNode`: Represents a major data processing step. Data flows from the
/// ///     bottom of the `PlanNode` tree to the top.
/// /// 2.  `Expression`: Represents a computation within a node, such as
/// ///     evaluating a `WHERE` clause condition or calculating a value in the
/// ///     `SELECT` list.
// ```
///
/// ### Planning a Complex Query: An Example
///
/// /// **Example SQL Query:**
///
/// /// ```sql
/// /// SELECT
/// ///   departments.name,
/// ///   COUNT(employees.id),
/// ///   AVG(employees.salary)
/// /// FROM
/// ///   employees
/// /// JOIN
/// ///   departments ON employees.dept_id = departments.id
/// /// WHERE
/// ///   employees.status = 'active'
/// /// GROUP BY
/// ///   departments.name
/// /// HAVING
/// ///   COUNT(employees.id) > 10
/// /// ORDER BY
/// ///   employee_count DESC
/// /// LIMIT 5;
/// /// ```
///
/// ### The Planning Process: From AST to PlanNode Tree
///
/// /// The `plan_statement` function constructs the `PlanNode` tree from the outside-in,
/// /// starting from the data sources (`FROM`/`JOIN`) and wrapping them with successive
/// /// operations. The final plan is a nested tree where the outermost node is the last
/// /// operation performed.
///
/// /// Below is a detailed graph showing how the `plan` variable is built up step-by-step.
/// /// Read the graph from the bottom (Step 1) to the top (Step 9).
///
/// ///
/// /// /// +-----------------------------------------------------------------------------+
/// /// /// | Step 9: LIMIT                                                               |
/// /// /// | The final operation. Wraps the whole plan in a `Limit` node.                |
/// /// /// | `PlanNode::Limit { input: [previous plan], count: 5 }`                      |
/// /// /// +------------------------------------^----------------------------------------+
/// /// ///                                      |
/// /// /// +------------------------------------+----------------------------------------+
/// /// /// | Step 8: PROJECTION (The SELECT list)                                        |
/// /// /// | Selects and computes the final columns AFTER sorting. This defines the      |
/// /// /// | final output shape.                                                         |
/// /// /// | `PlanNode::Projection {                                                     |
/// /// /// |   input: [previous plan],                                                   |
/// /// /// |   expressions: [                                                            |
/// /// /// |     Expression::Column(departments.name),                                   |
/// /// /// |     Expression::Aggregate(COUNT(employees.id)),                             |
/// /// /// |     Expression::Aggregate(AVG(employees.salary))                            |
/// /// /// |   ]                                                                         |
/// /// /// | }`                                                                          |
/// /// /// +------------------------------------^----------------------------------------+
/// /// ///                                      |
/// /// /// +------------------------------------+----------------------------------------+
/// /// /// | Step 7: ORDER BY                                                            |
/// /// /// | Wraps the HAVING output in an `OrderBy` node. This happens BEFORE projection|
/// /// /// | to allow sorting by columns that may not be in the final SELECT list.       |
/// /// /// | `PlanNode::OrderBy { input: [previous plan], orderings: [(col, DESC)] }`    |
/// /// /// +------------------------------------^----------------------------------------+
/// /// ///                                      |
/// /// /// +------------------------------------+----------------------------------------+
/// /// /// | Step 6: HAVING                                                              |
/// /// /// | Filters the *groups* created by `GROUP BY`. This happens after aggregation. |
/// /// /// | `PlanNode::HavingFilter {                                                   |
/// /// /// |   input: [previous plan],                                                   |
/// /// /// |   predicate: Expression::Comparison { COUNT(employees.id) > 10 }            |
/// /// /// | }`                                                                          |
/// /// /// +------------------------------------^----------------------------------------+
/// /// ///                                      |
/// /// /// +------------------------------------+----------------------------------------+
/// /// /// | Step 5: GROUP BY                                                            |
/// /// /// | Aggregates rows into groups. The `collect_aggs` helper finds all `COUNT()`  |
/// /// /// | and `AVG()` functions from the SELECT and HAVING clauses.                   |
/// /// /// | `PlanNode::GroupBy {                                                        |
/// /// /// |   input: [previous plan],                                                   |
/// /// /// |   columns: [departments.name],                                              |
/// /// /// |   aggregates: [COUNT(employees.id), AVG(employees.salary)]                  |
/// /// /// | }`                                                                          |
/// /// /// +------------------------------------^----------------------------------------+
/// /// ///                                      |
/// /// /// +------------------------------------+----------------------------------------+
/// /// /// | Step 4: WHERE                                                               |
/// /// /// | Filters individual rows *before* they are grouped.                          |
/// /// /// | `PlanNode::Filter {                                                         |
/// /// /// |   input: [previous plan],                                                   |
/// /// /// |   predicate: Expression::Comparison { employees.status = 'active' }         |
/// /// /// | }`                                                                          |
/// /// /// +------------------------------------^----------------------------------------+
/// /// ///                                      |
/// /// /// +------------------------------------+----------------------------------------+
/// /// /// | Step 3: JOIN                                                                |
/// /// /// | Combines the two table scans based on the join condition.                   |
/// /// /// | `PlanNode::Join {                                                           |
/// /// /// |   left: [employees scan],                                                   |
/// /// /// |   right: [departments scan],                                                |
/// /// /// |   on_left: "employees.dept_id",                                             |
/// /// /// |   on_right: "departments.id"                                                |
/// /// /// | }`                                                                          |
/// /// /// +-------------------^----------------+----------------^-----------------------+
/// /// ///                     |                                 |
/// /// /// +-------------------+--------------+  +-------------+-------------------------+
/// /// /// | Step 2: Right Table Scan         |  | Step 1: Left Table Scan               |
/// /// /// | The `departments` table.         |  | The first table in the `FROM` clause. |
/// /// /// | `PlanNode::TableScan {           |  | `PlanNode::TableScan {                |
/// /// /// |   table: "departments"           |  |   table: "employees"                  |
/// /// /// | }`                               |  | }`                                    |
/// /// /// +----------------------------------+  +---------------------------------------+
///
/// ### Explanation of Key Planning Steps
///
/// /// 1.  **Index Scan Optimization**: The planner first checks the `WHERE` clause for
/// ///     a simple `column = literal` predicate. If a matching index exists on that
/// ///     column, it generates an `IndexScan` instead of a `TableScan` for much
/// ///     faster data retrieval. In our example, `employees.status = 'active'` does not use
/// ///     an index, so we start with a `TableScan`.
///
/// /// 2.  **`WHERE` Clause Planning**: The `condition_expr_to_expression` function is
/// ///     called to recursively convert the potentially complex `WHERE` clause from the
/// ///     parser's AST into a single `Expression` enum. This `Expression` is then
/// ///     placed inside a `Filter` node.
///
/// /// 3.  **`GROUP BY` and Aggregate Planning**: This is a two-stage process.
/// ///     -   The planner first walks through all expressions in the `SELECT` list and
/// ///         the `HAVING` clause to find every aggregate function used. The helper
/// ///         function `collect_aggs` performs this recursive search.
/// ///     -   It then creates a `GroupBy` node containing the grouping columns (e.g.,
/// ///         `departments.name`) and the complete list of unique aggregates it found. This ensures
/// ///         that even if `COUNT(employees.id)` is used in both `SELECT` and `HAVING`, it is
/// ///         only calculated once per group.
///
/// /// 4.  **Order of Operations**: The planner follows the logical order of SQL
/// ///     processing, which is crucial for correctness. The sequence is `FROM/JOIN` -> `WHERE`
/// ///     -> `GROUP BY` -> `HAVING` -> `ORDER BY` -> `SELECT` -> `LIMIT`. 
/// ///      The nested structure of the `PlanNode` tree naturally enforces this execution order.

// ══════════════════════════════════════ NODE FOR QUERY PLAN ══════════════════════════════════════

// The recursive operator tree
#[derive(Debug, Clone)]
pub enum PlanNode {
    TableScan { table: String },
    IndexScan { table: String, index_column: ColumnRef, expected_value: Value },
    Filter { input: Box<PlanNode>, predicate: Expression },
    Projection { input: Box<PlanNode>, expressions: Vec<Expression> },
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        on_left: ColumnRef,
        on_right: ColumnRef,
    },
    GroupBy {
        input: Box<PlanNode>,
        columns: Vec<ColumnRef>,
        aggregates: Vec<Expression>,
    },
    HavingFilter {
        input: Box<PlanNode>,
        predicate: ConditionExpr, // use the AST ConditionExpr directly
    },
    Distinct {
        input: Box<PlanNode>,
    },
    OrderBy {
        input: Box<PlanNode>,
        orderings: Vec<(Expression, bool)>,
    },
    Limit {
        input: Box<PlanNode>,
        count: usize,
    },

    CreateTable {
        table: String,
        columns: Vec<(String, String)>,
        primary_key: Option<String>,
        indexes: Vec<String>,
    },
    CreateIndex {
        table: String,
        column: String,
        index_name: Option<String>, // if you want to allow named indexes
    },
    Insert { table: String, columns: Vec<String>, values: Vec<Value> },
    Update { table: String, assignments: Vec<(ColumnRef, Value)>, predicate: Option<Expression> },
    Delete { table: String, predicate: Option<Expression> },
    Explain {
        inner: Box<PlanNode>,
    },
}

#[derive(Debug)]
pub enum QueryPlan {
    Select(PlanNode),
}

// Logical plan expressions
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Column(ColumnRef),
    Literal(Value),
    // Comparison expressions (e.g., =, <, >)
    Comparison {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    // Arithmetic expressions (e.g., +, -, *, /)
    ArithmeticOp {
        left: Box<Expression>,
        op: ArithmeticOp,
        right: Box<Expression>,
    },
    // Unary expressions (e.g., -expr)
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    // Logical expressions (AND, OR)
    LogicalOp {
        left: Box<Expression>,
        op: LogicalOperator,
        right: Box<Expression>,
    },
    // Aggregate functions (COUNT, SUM, etc.)
    Aggregate {
        func: AggFunc,
        arg: Option<Box<Expression>>,
    },
}

// Binary (comparison) and logical operators
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOperator {
    And,
    Or,
}

// Values for literals
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    BigInt(i64),
    String(String),
    // Add more variants as needed (e.g., Float, Boolean)
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::BigInt(i) => write!(f, "{}", i),
            Value::String(s) => write!(f, "{}", s),
        }
    }
}

// ══════════════════════════════════════ QUERY PLANNER ══════════════════════════════════════

pub struct QueryPlanner<'db> {
    db_eng: &'db DatabaseEngine,
}

impl<'db> QueryPlanner<'db> {
    pub fn new(db_eng: &'db DatabaseEngine) -> Self {
        QueryPlanner { db_eng }
    }

    pub fn plan_statement(&self, stmt: Statement) -> Result<PlanNode, DbError> {
        match stmt {
            // EXPLAIN
            Statement::Explain(boxed_inner) => {
                let child_plan = self.plan_statement(*boxed_inner)?;
                Ok(PlanNode::Explain {
                    inner: Box::new(child_plan),
                })
            }

            // SELECT
            Statement::Select(sel) => {
                // 1. Start with base scan (possibly IndexScan)
                let mut plan = if let Some(cond_expr) = &sel.condition_expr {
                    if let Some((col, val)) = extract_simple_predicate(cond_expr) {
                        if self.db_eng.catalog.index_exists(&sel.table, col.column.as_str()) {
                            PlanNode::IndexScan {
                                table: sel.table.clone(),
                                index_column: col,
                                expected_value: val,
                            }
                        } else {
                            PlanNode::TableScan { table: sel.table.clone() }
                        }
                    } else {
                        PlanNode::TableScan { table: sel.table.clone() }
                    }
                } else {
                    PlanNode::TableScan { table: sel.table.clone() }
                };

                // 2. Apply JOINs (nested joins)
                for join in &sel.joins {
                    let right_scan = PlanNode::TableScan {
                        table: join.table.clone(),
                    };
                    plan = PlanNode::Join {
                        left: Box::new(plan),
                        right: Box::new(right_scan),
                        on_left: join.left_column.clone(),
                        on_right: join.right_column.clone(),
                    };
                }

                // 3. WHERE → Filter
                if let Some(cond_expr) = &sel.condition_expr {
                    let predicate = condition_expr_to_expression(cond_expr);
                    plan = PlanNode::Filter {
                        input: Box::new(plan),
                        predicate,
                    };
                }

                // ─── 4) GROUP BY & pure‐aggregate support ───────────────────
                // 4a) build all SELECT‐list expressions
                let select_exprs: Vec<Expression> = sel.columns.iter()
                    .map(|item| match item {
                        SelectItem::Column(c) => Expression::Column(c.clone()),
                        SelectItem::Aggregate { func, arg } =>
                            Expression::Aggregate {
                                func: func.clone(),
                                arg:  arg.as_ref().map(|c| Box::new(Expression::Column(c.clone()))),
                            },
                        SelectItem::Expression(e) => expr_to_expression(e),
                    })
                    .collect();

                // 4b) extract *all* aggregate expressions
                let mut aggregate_list: Vec<Expression> = Vec::new();
                for e in &select_exprs {
                    collect_aggs(e, &mut aggregate_list);
                }

                // 4c) if GROUP BY was specified… or if ANY aggregates exist ⇒ GroupBy
                if let Some(gcols) = &sel.group_by {
                    plan = PlanNode::GroupBy {
                        input:      Box::new(plan),
                        columns:    gcols.clone(),
                        aggregates: aggregate_list.clone(),
                    };
                } else if !aggregate_list.is_empty() {
                    // pure‐aggregate ⇒ no grouping columns
                    plan = PlanNode::GroupBy {
                        input:      Box::new(plan),
                        columns:    Vec::new(),
                        aggregates: aggregate_list.clone(),
                    };
                }


                // 5. HAVING
                if let Some(having_expr) = &sel.having {
                    // Use the raw ConditionExpr for the HavingFilter node
                    plan = PlanNode::HavingFilter {
                        input: Box::new(plan),
                        predicate: having_expr.clone(),
                    };
                }

                // 6. ORDER BY
                if let Some(ast_orderings) = &sel.order_by {
                    let plan_orderings = ast_orderings.iter().map(|o| {
                        // reuse your expr_to_expression helper
                        let expr = expr_to_expression(&o.expr);
                        (expr, o.ascending)
                    }).collect();
                    plan = PlanNode::OrderBy {
                        input: Box::new(plan),
                        orderings: plan_orderings,
                    };
                }

                // 7. Projection (SELECT list)
                let projection_exprs: Vec<Expression> = sel
                    .columns
                    .iter()
                    .map(|item| match item {
                        SelectItem::Column(colref) => Expression::Column(colref.clone()),
                        SelectItem::Aggregate { func, arg } =>
                        // directly build the same Expression::Aggregate you made in expr_to_expression
                        Expression::Aggregate {
                            func: func.clone(),
                            arg: arg
                                .as_ref()
                                .map(|c| Box::new(Expression::Column(c.clone()))),
                        },
                        SelectItem::Expression(expr) => expr_to_expression(expr),
                    })
                    .collect();

                plan = PlanNode::Projection {
                    input: Box::new(plan),
                    expressions: projection_exprs,
                };

                // 8. DISTINCT
                if sel.distinct {
                    plan = PlanNode::Distinct {
                        input: Box::new(plan),
                    };
                }

                // 9. LIMIT
                if sel.limit > 0 {
                    plan = PlanNode::Limit {
                        input: Box::new(plan),
                        count: sel.limit,
                    };
                }

                Ok(plan)
            }

            // INSERT
            Statement::Insert(insert) => {
                let values: Vec<Value> = insert
                    .values
                    .into_iter()
                    .map(|v| {
                        if let Ok(i) = v.parse::<i64>() {
                            Value::BigInt(i)
                        } else {
                            Value::String(v)
                        }
                    })
                    .collect();

                Ok(PlanNode::Insert {
                    table: insert.table,
                    columns: insert.columns,
                    values,
                })
            }

            // UPDATE
            Statement::Update(update) => {
                let assignments: Vec<(ColumnRef, Value)> = update
                    .assignments
                    .into_iter()
                    .map(|(col, expr)| {
                        // Only allow literal assignments
                        if let Expr::Literal(raw) = expr {
                            let val = if let Ok(i) = raw.parse::<i64>() {
                                Value::BigInt(i)
                            } else {
                                Value::String(raw)
                            };
                            Ok((col, val))
                        } else {
                            Err(DbError::Planner("Only literal assignments supported in UPDATE".to_string()))
                        }
                    })
                    .collect::<Result<_,_>>()?;

                let predicate = update
                    .condition_expr
                    .as_ref()
                    .map(|ce| condition_expr_to_expression(ce));

                Ok(PlanNode::Update {
                    table: update.table,
                    assignments,
                    predicate,
                })
            }

            // DELETE
            Statement::Delete(delete) => {
                let predicate = delete
                    .condition_expr
                    .as_ref()
                    .map(|ce| condition_expr_to_expression(ce));
                Ok(PlanNode::Delete {
                    table: delete.table,
                    predicate,
                })
            }

            // CREATE TABLE
            Statement::Create(create) => Ok(PlanNode::CreateTable {
                table: create.table,
                columns: create.columns,
                primary_key: create.primary_key,
                indexes: create.indexes,
            }),

            // CREATE INDEX
            Statement::CreateIndex(create_index) => Ok(PlanNode::CreateIndex {
                table: create_index.table,
                column: create_index.column,
                index_name: create_index.index_name,
            }),
        }
    }
}


// ══════════════════════════════════════ GENERAL UTILITY FUNCTIONS ══════════════════════════════════════

// Helper to parse a column name string into ColumnRef
fn col(name: &str) -> ColumnRef {
    ColumnRef::parse(name)
}

/// Recursively convert an AST Expr into a logical-plan Expression.
/// Supports literals, column references, arithmetic, and unary operations.
pub fn expr_to_expression(ast: &Expr) -> Expression {
    match ast {
        Expr::Aggregate { func, arg } => Expression::Aggregate {
           func: func.clone(),
           arg: arg.as_ref().map(|c| Box::new(Expression::Column(c.clone()))),
        },
        Expr::Literal(raw) => {
            if let Ok(i) = raw.parse::<i64>() {
                Expression::Literal(Value::BigInt(i))
            } else {
                Expression::Literal(Value::String(raw.clone()))
            }
        }
        Expr::Column(col_ref) => Expression::Column(col_ref.clone()),
        Expr::BinaryOp { left, op, right } => {
            // Arithmetic expression
            let left_expr = expr_to_expression(left);
            let right_expr = expr_to_expression(right);
            Expression::ArithmeticOp {
                left: Box::new(left_expr),
                op: op.clone(),
                right: Box::new(right_expr),
            }
        }
        Expr::UnaryOp { op, expr } => {
            // Unary operation (e.g., negation)
            let inner = expr_to_expression(expr);
            Expression::UnaryOp {
                op: op.clone(),
                expr: Box::new(inner),
            }
        }
    }
}

/// Recursively convert an AST ConditionExpr into a logical-plan Expression.
/// Supports comparisons and logical AND/OR, allowing complex nested expressions
/// (e.g., (score - 10) * (level + 2) >= 500 AND status = 'active').
pub fn condition_expr_to_expression(cond: &ConditionExpr) -> Expression {
    match cond {
        ConditionExpr::Clause { left, operator, right } => {
            // Convert both left and right sides (they might be arithmetic or literals or columns).
            let left_expr = expr_to_expression(left);
            let right_expr = expr_to_expression(right);
            Expression::Comparison {
                left: Box::new(left_expr),
                op: operator.clone(),
                right: Box::new(right_expr),
            }
        }
        ConditionExpr::And(left, right) => Expression::LogicalOp {
            left: Box::new(condition_expr_to_expression(left)),
            op: LogicalOperator::And,
            right: Box::new(condition_expr_to_expression(right)),
        },
        ConditionExpr::Or(left, right) => Expression::LogicalOp {
            left: Box::new(condition_expr_to_expression(left)),
            op: LogicalOperator::Or,
            right: Box::new(condition_expr_to_expression(right)),
        },
    }
}


fn extract_simple_predicate(cond: &ConditionExpr) -> Option<(ColumnRef, Value)> {
    use ConditionExpr::*;
    use Expr::*;

    if let Clause { left, operator, right } = cond {
        // We only care about equality here
        if *operator != BinaryOperator::Equal {
            return None;
        }

        // Check for "column = literal"
        match (&*left, &*right) {
            (Column(col_ref), Literal(raw)) => {
                // Try to parse the raw literal as an integer first
                let val = if let Ok(i) = raw.parse::<i64>() {
                    Value::BigInt(i)
                } else {
                    Value::String(raw.clone())
                };
                return Some((col_ref.clone(), val));
            }
            // Also allow the literal to be on the left and column on the right:
            (Literal(raw), Column(col_ref)) => {
                let val = if let Ok(i) = raw.parse::<i64>() {
                    Value::BigInt(i)
                } else {
                    Value::String(raw.clone())
                };
                return Some((col_ref.clone(), val));
            }
            _ => {}
        }
    }
    None
}


/// Recursively collect every Aggregate(...) node inside `expr` into `out`.
fn collect_aggs(expr: &Expression, out: &mut Vec<Expression>) {
    match expr {
        // Found an aggregate: clone and collect it
        Expression::Aggregate { func, arg } => {
            out.push(Expression::Aggregate {
                func: func.clone(),
                arg: arg.clone(),
            });
        }

        // Unary operators (e.g. -expr, NOT expr): recurse into the single child
        Expression::UnaryOp { op: _, expr: inner } => {
            collect_aggs(inner, out);
        }

        // Binary or arithmetic ops: recurse both sides
        Expression::ArithmeticOp { left, op: _, right }
        | Expression::Comparison { left, op: _, right } => {
            collect_aggs(left,  out);
            collect_aggs(right, out);
        }
        _ => {}
    }
}



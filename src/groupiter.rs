//! Module implementing streaming relational operators: group_by, having, distinct

use std::collections::{HashMap, HashSet};
use crate::page::{Row, TableSchema, SqlValue};
use crate::planner::{Expression};
use crate::tokenizer::AggFunc;
use crate::dbengine::{eval_expr, evaluate_predicate};
use crate::dbengine::SchemaRowIter;
use crate::dbengine::RowIterator;
use crate::order::RowSorter;
use crate::dberror::DbError;
use crate::tokenizer::BinaryOperator;
use crate::dbengine::compare_sqlvalue; // This is a bool-returning function


/// ### Hash-based `group_by`
///
/// /// The `group_by` function calculates aggregates by consuming the entire input iterator
/// /// and building a hash map in memory.
///
/// /// -   Key: The grouping key is formed by concatenating the string representations
/// ///     of the values in the grouping columns for a given row.
/// /// -   Value: The value is a list of "accumulators," one for each aggregate
/// ///     function (`SUM`, `COUNT`, `AVG`, etc.).
///
/// /// For each row processed, we calculate its key. If the key is new, we create a new
/// /// set of accumulators. Then, we update the accumulators for that key with the
/// /// current row's data. After processing all input, we iterate through the hash map
/// /// and "finish" each accumulator to produce the final aggregate value for each group.
///
/// ///
/// /// /// Visualizing Hash Aggregation:
/// ///
/// /// ///                                        Input Rows
/// /// ///                                +---------------------------+
/// /// ///                                | { dept: "Eng", sal: 100 } |
/// /// ///                                | { dept: "HR",  sal: 50  } |
/// /// ///                                | { dept: "Eng", sal: 120 } |
/// /// ///                                +-------------+-------------+
/// /// ///                                              |
/// /// ///           +----------------------------------+------------------------------------+
/// /// ///           |                                                                       |
/// /// /// +---------v---------+                                                     +-------v---------+
/// /// /// | Key: ["Eng"]      |                                                     | Key: ["HR"]       |
/// /// /// +-------------------+                                                     +-----------------+
/// /// /// | Value:            | (HashMap<Vec<String>, Vec<Box<dyn AggAccumulator>>>)| Value:            |
/// /// /// | [ SumAcc: 220,    |                                                     | [ SumAcc: 50,     |
/// /// /// |   CountAcc: 2 ]   |                                                     |   CountAcc: 1 ]   |
/// /// /// +-------------------+                                                     +-------------------+


/// Hash-based GROUP BY: consumes input iterator, emits one Row per group
pub fn group_by<'a>(
    input: Box<dyn RowIterator + 'a>,      // change input type to RowIterator
    group_cols: Vec<usize>,
    aggregates: Vec<Expression>,
    output_schema: TableSchema,
) -> Result<Box<dyn RowIterator + 'a>, DbError> { // Changed return type to Result
    // Map from grouping key to per-aggregate accumulators
    let mut map: HashMap<Vec<String>, Vec<Box<dyn AggAccumulator>>> = HashMap::new();

    for row in input {
        // Build key by stringifying each grouping column
        let key: Vec<String> = group_cols.iter()
            .map(|&idx| row.values.get(idx).map(|v| v.to_string()).unwrap_or_default()) // Ensure this doesn't panic if index is out of bounds
            .collect();

        // Initialize accumulators if first time
        let accs = map.entry(key.clone()).or_insert_with(|| {
            aggregates.iter().cloned().map(|expr| create_acc(expr).unwrap_or_else(|e| { // Handle error for create_acc
                eprintln!("Error creating accumulator: {:?}", e);
                // Fallback to a dummy accumulator or re-panic in a controlled way if this should be a fatal error.
                // For now, creating a dummy count accumulator to avoid panicking during creation.
                Box::new(CountAcc { count: 0 }) 
            })).collect()
        });

        // Update each accumulator with this row
        for acc in accs.iter_mut() {
            acc.update(&row, &output_schema);
        }
    }

    // If there are no groups (i.e., map is empty),
    // and no grouping columns (scalar aggregate), emit a single row with each aggregate's default.
    let result_iter: Box<dyn Iterator<Item = Row>> = if map.is_empty() && group_cols.is_empty() {
        // Create one row with one value per aggregate, each .finish() on a fresh accumulator.
        let mut default_values: Vec<SqlValue> = Vec::new();
        for agg_expr in aggregates.iter() {
            let mut acc = create_acc(agg_expr.clone()).unwrap();
            default_values.push(acc.finish());
        }
        Box::new(std::iter::once(Row { values: default_values }))
    } else {
        Box::new(map.into_iter().map(move |(key, mut accs)| {
            // key columns as VARCHAR
            let mut values: Vec<SqlValue> = key.into_iter()
                .map(|s| SqlValue::VARCHAR(s.into_bytes()))
                .collect();
            // append each aggregate result
            for mut a in accs.drain(..) {
                values.push(a.finish());
            }
            Row { values }
        }))
    };

    Ok(Box::new(SchemaRowIter {
        schema: output_schema,
        inner: result_iter,
    }))

}


pub fn having<'a>(
    input: Box<dyn RowIterator + 'a>,
    predicate: Expression,
    schema: TableSchema,
) -> Box<dyn RowIterator + 'a> {
    let schema_clone = schema.clone();
    let filtered_iter = input.filter(move |row| {
        // evaluate_predicate returns Result<bool, DbError>
        evaluate_predicate(row, &schema_clone, &predicate).unwrap_or(false)
    });

    Box::new(SchemaRowIter {
        schema,
        inner: filtered_iter,
    })
}

/// ### Sorted `distinct`
///
/// /// The `distinct_sorted` operator leverages the external `RowSorter` to provide a
/// /// memory-efficient `DISTINCT` operation.

/// DISTINCT: eliminate duplicate rows
pub fn distinct_sorted<'a>(
    input: Box<dyn RowIterator + 'a>,
    schema: TableSchema,
    order_by: Vec<(Expression, bool)>,
) -> Result<Box<dyn RowIterator + 'a>, DbError> { // Changed return type to Result
    // 1) Build the composite sort key:
    //      first, the user’s ORDER BY expressions (with their asc/desc flag)
    //      then every projected column ascending to group duplicates
    let mut sort_exprs = order_by.clone();
    for col in &schema.name_col {
        let expr = Expression::Column(crate::tokenizer::ColumnRef {
            table: None,
            column: col.clone(),
        });
        if !sort_exprs.iter().any(|(e, _)| e == &expr) {
            sort_exprs.push((expr, true));
        }
    }

    // 2) Run a single on-disk sort by that key
    let sorter = RowSorter::new(schema.clone(), sort_exprs);
    let mut sorted_iter = sorter.sort(input)?.peekable(); // Propagate error from sorter.sort

    // 3) Walk the sorted stream, dropping adjacent duplicates
    let mut last_row: Option<Row> = None;
    let distinct_iter = std::iter::from_fn(move || {
        while let Some(candidate) = sorted_iter.peek() {
            if let Some(prev) = &last_row {
                if rows_equal(candidate, prev) {
                    // duplicate: skip it
                    sorted_iter.next();
                    continue;
                }
            }
            // new distinct row: consume & return it
            let next = sorted_iter.next();
            last_row = next.clone();
            return next;
        }
        None
    });

    Ok(Box::new(SchemaRowIter {
        schema,
        inner: distinct_iter,
    }))
}

/// Compare two rows for exact equality across all columns.
fn rows_equal(a: &Row, b: &Row) -> bool {
    a.values.len() == b.values.len()
        && a.values.iter().zip(&b.values).all(|(x, y)| x == y) // SqlValue equality should be fine
}


/// Trait for aggregate accumulators
pub trait AggAccumulator {
    fn update(&mut self, row: &Row, schema: &TableSchema); // Modified to not return Result for simplicity in Aggregators
    fn finish(&mut self) -> SqlValue;
}

/// Create accumulator from an Expression::Aggregate
fn create_acc(expr: Expression) -> Result<Box<dyn AggAccumulator>, DbError> { // Changed return type to Result
    match expr {
        Expression::Aggregate { func, arg } => match func {
            AggFunc::Count => Ok(Box::new(CountAcc { count: 0 })),
            AggFunc::Sum => Ok(Box::new(SumAcc { arg, sum: 0.0 })),
            AggFunc::Avg => Ok(Box::new(AvgAcc { arg, sum: 0.0, count: 0 })),
            AggFunc::Min => Ok(Box::new(MinAcc { arg, min: None })),
            AggFunc::Max => Ok(Box::new(MaxAcc { arg, max: None })),
            _ => Err(DbError::GroupIter(format!("Unsupported aggregate: {:?}", func))), // Replaced unimplemented!
        },
        _ => Err(DbError::GroupIter("create_acc: expected Aggregate expression".to_string())), // Replaced panic!
    }
}

/// COUNT accumulator (counts all rows)
struct CountAcc {
    count: u64,
}

impl AggAccumulator for CountAcc {
    fn update(&mut self, _row: &Row, _schema: &TableSchema) {
        self.count += 1;
    }
    fn finish(&mut self) -> SqlValue {
        SqlValue::UBIGINT(self.count)
    }
}

/// SUM accumulator (sums numeric values)
struct SumAcc {
    arg: Option<Box<Expression>>,
    sum: f64,
}

impl AggAccumulator for SumAcc {
    fn update(&mut self, row: &Row, schema: &TableSchema) {
        if let Some(expr) = &self.arg {
            // eval_expr returns Result, so we need to handle it.
            // If evaluation fails, we simply skip adding this value to the sum.
            if let Ok(val) = eval_expr(row, schema, expr) {
                match val {
                    SqlValue::INT(v) => self.sum += v as f64,
                    SqlValue::BIGINT(v) => self.sum += v as f64,
                    SqlValue::TINYINT(v) => self.sum += v as f64,
                    SqlValue::SMALLINT(v) => self.sum += v as f64,
                    SqlValue::UBIGINT(v) => self.sum += v as f64,
                    SqlValue::FLOAT(v) => self.sum += v as f64,
                    SqlValue::DOUBLE(v) => self.sum += v,
                    _ => {} // Ignore non-numeric values
                }
            }
        }
    }
    fn finish(&mut self) -> SqlValue {
        SqlValue::DOUBLE(self.sum)
    }
}

/// AVG accumulator (average numeric values)
struct AvgAcc {
    arg: Option<Box<Expression>>,
    sum: f64,
    count: u64,
}

impl AggAccumulator for AvgAcc {
    fn update(&mut self, row: &Row, schema: &TableSchema) {
        if let Some(expr) = &self.arg {
            if let Ok(val) = eval_expr(row, schema, expr) {
                match val {
                    SqlValue::INT(v) => {
                        self.sum += v as f64;
                        self.count += 1;
                    }
                    SqlValue::BIGINT(v) => {
                        self.sum += v as f64;
                        self.count += 1;
                    }
                    SqlValue::TINYINT(v) => {
                        self.sum += v as f64;
                        self.count += 1;
                    }
                    SqlValue::SMALLINT(v) => {
                        self.sum += v as f64;
                        self.count += 1;
                    }
                    SqlValue::UBIGINT(v) => {
                        self.sum += v as f64;
                        self.count += 1;
                    }
                    SqlValue::FLOAT(v) => {
                        self.sum += v as f64;
                        self.count += 1;
                    }
                    SqlValue::DOUBLE(v) => {
                        self.sum += v;
                        self.count += 1;
                    }
                    _ => {}
                }
            }
        }
    }
    fn finish(&mut self) -> SqlValue {
        if self.count == 0 {
            SqlValue::DOUBLE(0.0)
        } else {
            SqlValue::DOUBLE(self.sum / (self.count as f64))
        }
    }
}

/// MIN accumulator (minimum value)
struct MinAcc {
    arg: Option<Box<Expression>>,
    min: Option<SqlValue>,
}

impl AggAccumulator for MinAcc {
    fn update(&mut self, row: &Row, schema: &TableSchema) {
        if let Some(expr) = &self.arg {
            if let Ok(val) = eval_expr(row, schema, expr) {
                self.min = match &self.min {
                    None => Some(val),
                    Some(current_min) => {
                        if compare_sqlvalue(&val, current_min, &BinaryOperator::LessThan) {
                            Some(val)
                        } else {
                            Some(current_min.clone())
                        }
                    }
                };
            }
        }
    }
    fn finish(&mut self) -> SqlValue {
        self.min.take().unwrap_or_else(|| SqlValue::DOUBLE(0.0)) // Replaced unwrap with unwrap_or_else
    }
}

/// MAX accumulator (maximum value)
struct MaxAcc {
    arg: Option<Box<Expression>>,
    max: Option<SqlValue>,
}

impl AggAccumulator for MaxAcc {
    fn update(&mut self, row: &Row, schema: &TableSchema) {
        if let Some(expr) = &self.arg {
            if let Ok(val) = eval_expr(row, schema, expr) {
                self.max = match &self.max {
                    None => Some(val),
                    Some(current_max) => {
                        if compare_sqlvalue(&val, current_max, &BinaryOperator::GreaterThan) {
                            Some(val)
                        } else {
                            Some(current_max.clone())
                        }
                    }
                };
            }
        }
    }
    fn finish(&mut self) -> SqlValue {
        self.max.take().unwrap_or_else(|| SqlValue::DOUBLE(0.0)) // Replaced unwrap with unwrap_or_else
    }
}
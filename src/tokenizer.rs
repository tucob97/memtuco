// tokenizer.rs

use crate::dberror::DbError;
use std::iter::Peekable;
use std::str::Chars;

// ══════════════════════════════════════ TOKEN DEFINITION ══════════════════════════════════════

/// ------------------ TOKEN DEFINITIONS ------------------
/// SQL tokens.
#[derive(PartialEq, Debug, Clone)]
pub(crate) enum Token {
    Select,
    Create,
    Update,
    Delete,
    Insert,
    Into,
    Values,
    Set,
    Drop,
    From,
    Where,
    Group,
    Having,
    Limit,
    LogicOperator(LogicOperator),
    Primary,
    Key,
    Unique,
    Table,
    Database,
    Int,
    BigInt,
    Unsigned,
    Char,
    Varchar,
    Bool,
    True,
    False,
    Order,
    By,
    Distinct,
    Index,
    On,
    Start,
    Transaction,
    Rollback,
    Commit,
    Explain,
    Identifier(String),
    String(String),
    StringLiteral(String),
    NumberLiteral(String), // store raw digits/decimal as String
    Asterisk,
    Plus,    // '+'
    Minus,   // '-'
    Slash,   // '/'
    LeftParenthesis,
    RightParenthesis,
    Comma,
    Semicolon,
    BinaryOperator(BinaryOperator),
    Inner,
    Join,
    AggFunc(AggFunc),
    Dot,
    EOF,
}

#[derive(PartialEq, Debug, Clone)]
pub(crate) enum BinaryOperator {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    LessThanEqual,
    GreaterThanEqual,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(PartialEq, Debug, Clone)]
pub enum LogicOperator {
    And,
    Or,
}

// ══════════════════════════════════════ LEXER ══════════════════════════════════════


/// ------------------ LEXER ------------------
pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input: input.chars().peekable(),
        }
    }

    fn peek_char(&mut self) -> Option<char> {
        self.input.peek().copied()
    }

    fn next_token(&mut self) -> Result<Token, DbError> {
        self.skip_whitespace();

        match self.input.next() {
            Some('*') => Ok(Token::Asterisk),
            Some('+') => Ok(Token::Plus),
            Some('-') => Ok(Token::Minus),
            Some('/') => Ok(Token::Slash),
            Some(',') => Ok(Token::Comma),
            Some(';') => Ok(Token::Semicolon),
            Some('"') => Ok(self.string_literal()),
            Some('\'') => Ok(self.char_literal()),
            Some('$') => Ok(self.dollar_string_literal()),
            Some('(') => Ok(Token::LeftParenthesis),
            Some(')') => Ok(Token::RightParenthesis),
            Some('.') => Ok(Token::Dot),
            Some(c) if c.is_alphabetic() => Ok(self.identifier_or_keyword(c)),
            Some(c) if c.is_numeric() => Ok(self.number_literal(c)),
            None => Ok(Token::EOF),
            Some('=') => Ok(Token::BinaryOperator(BinaryOperator::Equal)),
            Some('>') => {
                if self.peek_char() == Some('=') {
                    self.input.next();
                    Ok(Token::BinaryOperator(BinaryOperator::GreaterThanEqual))
                } else {
                    Ok(Token::BinaryOperator(BinaryOperator::GreaterThan))
                }
            }
            Some('<') => {
                if self.peek_char() == Some('=') {
                    self.input.next();
                    Ok(Token::BinaryOperator(BinaryOperator::LessThanEqual))
                } else {
                    Ok(Token::BinaryOperator(BinaryOperator::LessThan))
                }
            }
            Some('!') if self.peek_char() == Some('=') => {
                self.input.next();
                Ok(Token::BinaryOperator(BinaryOperator::NotEqual))
            }
            Some(c) => Err(DbError::Tokenizer(format!("Unexpected character in lexer: {}", c))),
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.input.peek() {
            if c.is_whitespace() {
                self.input.next();
            } else {
                break;
            }
        }
    }

    fn string_literal(&mut self) -> Token {
        let mut s = String::new();
        while let Some(&ch) = self.input.peek() {
            self.input.next();
            if ch == '"' {
                break;
            }
            s.push(ch);
        }
        Token::Identifier(s)
    }

    fn char_literal(&mut self) -> Token {
        let mut s = String::new();
        while let Some(&ch) = self.input.peek() {
            self.input.next();
            if ch == '\'' {
                break;
            }
            s.push(ch);
        }
        Token::StringLiteral(s)
    }

    fn dollar_string_literal(&mut self) -> Token {
        let mut s = String::new();
        while let Some(&ch) = self.input.peek() {
            self.input.next();
            if ch == '$' {
                break;
            }
            s.push(ch);
        }
        Token::Identifier(s)
    }

    fn identifier_or_keyword(&mut self, first: char) -> Token {
        let mut ident = String::new();
        ident.push(first);

        while let Some(&ch) = self.input.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                ident.push(ch);
                self.input.next();
            } else {
                break;
            }
        }

        match ident.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "LIMIT" => Token::Limit,
            "TABLE" => Token::Table,
            "INDEX" => Token::Index,
            "INNER" => Token::Inner,
            "JOIN" => Token::Join,
            "ON" => Token::On,
            "GROUP" => Token::Group,
            "HAVING" => Token::Having,
            "AND" => Token::LogicOperator(LogicOperator::And),
            "OR" => Token::LogicOperator(LogicOperator::Or),
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "DISTINCT" => Token::Distinct,
            "COUNT" => Token::AggFunc(AggFunc::Count),
            "SUM" => Token::AggFunc(AggFunc::Sum),
            "AVG" => Token::AggFunc(AggFunc::Avg),
            "MIN" => Token::AggFunc(AggFunc::Min),
            "MAX" => Token::AggFunc(AggFunc::Max),
            "CREATE" => Token::Create,
            "UPDATE" => Token::Update,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "EXPLAIN" => Token::Explain,
            _ => Token::Identifier(ident),
        }
    }

    fn number_literal(&mut self, first: char) -> Token {
        let mut number = String::new();
        number.push(first);

        while let Some(&ch) = self.input.peek() {
            if ch.is_numeric() || ch == '.' {
                number.push(ch);
                self.input.next();
            } else {
                break;
            }
        }

        Token::NumberLiteral(number)
    }
}


// ══════════════════════════════════════ AST ══════════════════════════════════════

/// /// /// The `Parser` consumes a stream of `Token`s and uses a
/// /// /// Pratt Parsing algorithm (`parse_expr_prec`) to construct the AST. This
/// /// /// method correctly handles operator precedence, ensuring that `*` and `/`
/// /// /// are nested deeper in the tree than `+` and `-`.
/// ///
/// /// /// For `(age * hours + (days + 7) / 12)`, the resulting AST has this structure:
/// ///
/// /// /// +-------------------------------------------------------------------------+
/// /// /// | AST for: (age * hours + (days + 7) / 12)                                |
/// /// /// +-------------------------------------------------------------------------+
/// /// ///
/// /// ///     BinaryOp { op: Add }
/// /// ///     |
/// /// ///     +-- left:  BinaryOp { op: Multiply }
/// /// ///     |          |
/// /// ///     |          +-- left:  Column("age")
/// /// ///     |          |
/// /// ///     |          +-- right: Column("hours")
/// /// ///     |
/// /// ///     +-- right: BinaryOp { op: Divide }
/// /// ///                |
/// /// ///                +-- left:  BinaryOp { op: Add }
/// /// ///                |          |
/// /// ///                |          +-- left:  Column("days")
/// /// ///                |          |
/// /// ///                |          +-- right: Literal("7")
/// /// ///                |
/// /// ///                +-- right: Literal("12")
/// ///


/// ------------------ AST  ------------------

#[derive(Debug, Clone)]
pub enum SelectItem {
    Column(ColumnRef),
    Aggregate {
        func: AggFunc,
        arg: Option<ColumnRef>, // None for COUNT(*)
    },
    Expression(Expr),
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(String),
    Column(ColumnRef),
    BinaryOp {
        left: Box<Expr>,
        op: ArithmeticOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Aggregate { func: AggFunc, arg: Option<ColumnRef> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Plus,
    Minus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub columns: Vec<SelectItem>,
    pub distinct: bool,
    pub table: String,
    pub joins: Vec<JoinClause>,
    pub condition_expr: Option<ConditionExpr>,
    pub group_by: Option<Vec<ColumnRef>>,
    pub having: Option<ConditionExpr>,
    pub order_by: Option<Vec<Ordering>>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

impl ColumnRef {
    pub fn parse(name: &str) -> Self {
        let mut parts = name.splitn(2, '.');
        // This `unwrap` is safe because the parser constructs the `name` string from
        // tokens that are guaranteed to be non-empty. The only other call site passes
        // a non-empty string literal ("*").
        let first = parts.next().unwrap();
        if let Some(second) = parts.next() {
            ColumnRef {
                table: Some(first.to_string()),
                column: second.to_string(),
            }
        } else {
            ColumnRef {
                table: None,
                column: first.to_string(),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub table: String,
    pub left_column: ColumnRef,
    pub right_column: ColumnRef,
}

#[derive(Debug, Clone)]
pub struct CreateStatement {
    pub table: String,
    pub columns: Vec<(String, String)>, // (name, type)
    pub primary_key: Option<String>,
    pub indexes: Vec<String>,           // List of columns to index
}

#[derive(Debug, Clone)]
pub struct CreateIndexStatement {
    pub table: String,
    pub column: String,
    pub index_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    /// Each assignment is “ColumnRef = Expr”
    pub assignments: Vec<(ColumnRef, Expr)>,
    pub condition_expr: Option<ConditionExpr>,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>, // optional in SQL
    pub values: Vec<String>,  // must match columns
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub condition_expr: Option<ConditionExpr>,
}

#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Create(CreateStatement),
    Update(UpdateStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    CreateIndex(CreateIndexStatement),
    Explain(Box<Statement>),
}

#[derive(Debug, Clone)]
pub struct Ordering {
    pub expr: Expr,
    pub ascending: bool,
}

/// A boolean condition in WHERE/HAVING
#[derive(Debug, Clone)]
pub enum ConditionExpr {
    Clause {
        left: Expr,
        operator: BinaryOperator,
        right: Expr,
    },
    And(Box<ConditionExpr>, Box<ConditionExpr>),
    Or(Box<ConditionExpr>, Box<ConditionExpr>),
}

// ══════════════════════════════════════ PARSER ══════════════════════════════════════

/// ------------------ PARSER ------------------
pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current: Token,
}

impl<'a> Parser<'a> {
    pub fn new(mut lexer: Lexer<'a>) -> Result<Self, DbError> {
        let current = lexer.next_token()?;
        Ok(Self { lexer, current })
    }

    fn advance(&mut self) -> Result<(), DbError> {
        self.current = self.lexer.next_token()?;
        Ok(())
    }

    fn expect(&mut self, expected: &Token) -> Result<(), DbError> {
        if &self.current == expected {
            self.advance()?;
            Ok(())
        } else {
            Err(DbError::Parser(format!("Expected {:?}, found {:?}", expected, self.current)))
        }
    }

    pub fn expect_identifier(&mut self) -> Result<String, DbError> {
        match &self.current {
            Token::Identifier(name) => {
                let v = name.clone();
                self.advance()?;
                Ok(v)
            }
            other => Err(DbError::Parser(format!("Expected identifier, got {:?}", other))),
        }
    }

    fn parse_columnref(&mut self) -> Result<ColumnRef, DbError> {
        match &self.current {
            Token::Identifier(first) => {
                let mut name = first.clone();
                self.advance()?;
                if self.current == Token::Dot {
                    self.advance()?;
                    let col = self.expect_identifier()?;
                    name = format!("{}.{}", name, col);
                }
                Ok(ColumnRef::parse(&name))
            }
            other => Err(DbError::Parser(format!("Expected column name, got {:?}", other))),
        }
    }

    /// ── ENTRY POINT FOR SELECT PARSING ──
    pub fn parse_select(&mut self) -> Result<SelectStatement, DbError> {
        self.expect(&Token::Select)?;

        let distinct = if self.current == Token::Distinct {
            self.advance()?;
            true
        } else {
            false
        };

        // Parse SELECT items
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_select_item()?);
            if self.current == Token::Comma {
                self.advance()?;
            } else {
                break;
            }
        }

        self.expect(&Token::From)?;
        let table = match &self.current {
            Token::Identifier(name) => {
                let v = name.clone();
                self.advance()?;
                v
            }
            other => return Err(DbError::Parser(format!("Expected table name, got {:?}", other))),
        };

        // Parse JOIN clauses
        let mut joins = Vec::new();
        while self.current == Token::Join || self.current == Token::Inner {
            self.advance()?;
            let joined_table = match &self.current {
                Token::Identifier(t) => {
                    let v = t.clone();
                    self.advance()?;
                    v
                }
                other => return Err(DbError::Parser(format!("Expected table after JOIN, got {:?}", other))),
            };

            self.expect(&Token::On)?;
            let left_col = self.parse_columnref()?;
            self.expect(&Token::BinaryOperator(BinaryOperator::Equal))?;
            let right_col = self.parse_columnref()?;

            joins.push(JoinClause {
                table: joined_table,
                left_column: left_col,
                right_column: right_col,
            });
        }

        // WHERE
        let condition_expr = if self.current == Token::Where {
            self.advance()?;
            Some(self.parse_condition_expr()?)
        } else {
            None
        };

        // GROUP BY
        let group_by = if self.current == Token::Group {
            self.advance()?;
            self.expect(&Token::By)?;
            let mut group_cols = Vec::new();
            loop {
                group_cols.push(self.parse_columnref()?);
                if self.current == Token::Comma {
                    self.advance()?;
                } else {
                    break;
                }
            }
            Some(group_cols)
        } else {
            None
        };

        // HAVING
        let having = if self.current == Token::Having {
            self.advance()?;
            Some(self.parse_condition_expr()?)
        } else {
            None
        };

        // ORDER BY
        let order_by = if self.current == Token::Order {
            self.advance()?;
            self.expect(&Token::By)?;
            let mut orderings = Vec::new();
            loop {
                // NEW: parse any Expr
                let raw = self.parse_expr()?;
                let simplified = simplify_expr(raw);
                let ascending = match &self.current {
                    Token::Identifier(s) if s.eq_ignore_ascii_case("ASC") => { self.advance()?; true }
                    Token::Identifier(s) if s.eq_ignore_ascii_case("DESC") => { self.advance()?; false }
                    _ => true
                };
                orderings.push(Ordering { expr: simplified, ascending });

                if self.current == Token::Comma {
                    self.advance()?;
                } else {
                    break;
                }
            }
            Some(orderings)
        } else {
            None
        };

        // LIMIT
        let limit = if self.current == Token::Limit {
            self.advance()?;
            match &self.current {
                Token::NumberLiteral(raw) => {
                    // REFACTORED: Replaced `unwrap_or` with proper error handling to prevent panics
                    // on non-integer values like "1.5".
                    let v: usize = raw.parse().map_err(|e| {
                        DbError::Parser(format!("Invalid integer for LIMIT clause: '{}'. Error: {}", raw, e))
                    })?;
                    self.advance()?;
                    v
                }
                other => return Err(DbError::Parser(format!("Expected number after LIMIT, got {:?}", other))),
            }
        } else {
            0
        };

        if self.current == Token::Semicolon {
            self.advance()?;
        } else if self.current == Token::EOF {
            // okay
        } else {
            return Err(DbError::Parser(format!("Expected ';' or EOF, got {:?}", self.current)));
        }

        Ok(SelectStatement {
            distinct,
            columns,
            table,
            joins,
            condition_expr,
            group_by,
            having,
            order_by,
            limit,
        })
    }

    fn parse_select_item(&mut self) -> Result<SelectItem, DbError> {
        // If we see a lone '*', treat it as a ColumnRef("*")
        if self.current == Token::Asterisk {
            self.advance()?;
            return Ok(SelectItem::Expression(Expr::Column(ColumnRef::parse("*"))));
        }

        // new: parse anyExpr (columns, arithmetic, aggregates, etc.)
        let expr = self.parse_expr()?;
        let simplified = simplify_expr(expr);
        Ok(SelectItem::Expression(simplified))
    }

    /// ── CONDITION / WHERE PARSING ──

    fn parse_condition_expr(&mut self) -> Result<ConditionExpr, DbError> {
        // First parse one comparison—this always looks like <Expr> <comp> <Expr>
        let mut expr = self.parse_comparison()?;

        // Then handle any number of "AND"/"OR" …
        while let Token::LogicOperator(op) = &self.current {
            let logic_op = op.clone();
            self.advance()?;
            let right = self.parse_comparison()?;
            expr = match logic_op {
                LogicOperator::And => ConditionExpr::And(Box::new(expr), Box::new(right)),
                LogicOperator::Or => ConditionExpr::Or(Box::new(expr), Box::new(right)),
            };
        }
        Ok(expr)
    }

    fn parse_comparison(&mut self) -> Result<ConditionExpr, DbError> {
        // LEFT side = full arithmetic Expr
        let left_expr = self.parse_expr()?;
        let left_s = simplify_expr(left_expr);

        // comparison operator
        let operator = match &self.current {
            Token::BinaryOperator(op) => {
                let op_copy = op.clone();
                self.advance()?;
                op_copy
            }
            other => return Err(DbError::Parser(format!("Expected comparison operator, got {:?}", other))),
        };

        // RIGHT side = full arithmetic Expr
        let right_expr = self.parse_expr()?;
        let right_s = simplify_expr(right_expr);

        Ok(ConditionExpr::Clause {
            left: left_s,
            operator,
            right: right_s,
        })
    }

    /// ── EXPRESSION PARSING ──

    fn parse_expr(&mut self) -> Result<Expr, DbError> {
        self.parse_expr_prec(0)
    }

    fn parse_expr_prec(&mut self, min_prec: u8) -> Result<Expr, DbError> {
        let mut left = self.parse_primary_expr()?;

        loop {
            let op_opt = self.current_arithmetic_op();
            if op_opt.is_none() {
                break;
            }
            let op = op_opt.unwrap();
            let prec = Self::get_precedence(&op);
            if prec < min_prec {
                break;
            }
            // consume '+', '-', '*', or '/'
            self.advance()?;
            let right = self.parse_expr_prec(prec + 1)?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, DbError> {
        match &self.current {

            Token::AggFunc(func) => {
                let func = func.clone();
                self.advance()?;
                self.expect(&Token::LeftParenthesis)?;
                let arg = if self.current == Token::Asterisk {
                    self.advance()?;
                    None
                } else {
                    Some(self.parse_columnref()?)
                };
                self.expect(&Token::RightParenthesis)?;
                Ok(Expr::Aggregate { func, arg })
            }

            Token::Minus => {
                self.advance()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Minus,
                    expr: Box::new(self.parse_primary_expr()?),
                })
            }
            Token::Plus => {
                self.advance()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Plus,
                    expr: Box::new(self.parse_primary_expr()?),
                })
            }
            Token::NumberLiteral(raw) => {
                let s = raw.clone();
                self.advance()?;
                Ok(Expr::Literal(s))
            }
            Token::Identifier(_) => Ok(Expr::Column(self.parse_columnref()?)),
            Token::Asterisk => {
                self.advance()?;
                Ok(Expr::Column(ColumnRef::parse("*")))
            }
            Token::LeftParenthesis => {
                self.advance()?;
                let inner = self.parse_expr()?;
                self.expect(&Token::RightParenthesis)?;
                Ok(inner)
            }

            Token::StringLiteral(raw) => {
                let s = raw.clone();
                self.advance()?;
                Ok(Expr::Literal(s))
            }

            Token::String(raw) => {
                let s = raw.clone();
                self.advance()?;
                Ok(Expr::Literal(s))
            }

            other => Err(DbError::Parser(format!("Unexpected token in expression: {:?}", other))),
        }
    }

    fn current_arithmetic_op(&self) -> Option<ArithmeticOp> {
        match &self.current {
            Token::Plus => Some(ArithmeticOp::Add),
            Token::Minus => Some(ArithmeticOp::Subtract),
            Token::Asterisk => Some(ArithmeticOp::Multiply),
            Token::Slash => Some(ArithmeticOp::Divide),
            _ => None,
        }
    }

    fn get_precedence(op: &ArithmeticOp) -> u8 {
        match op {
            ArithmeticOp::Add | ArithmeticOp::Subtract => 1,
            ArithmeticOp::Multiply | ArithmeticOp::Divide => 2,
        }
    }

    /// ── TOP‐LEVEL parse_query ──

    pub fn parse_query(&mut self) -> Result<Statement, DbError> {
        match &self.current {
            Token::Explain => {
                self.advance()?;
                let inner = self.parse_query()?;
                match inner {
                    stmt @ (Statement::Select(_)
                    | Statement::Create(_)
                    | Statement::Update(_)
                    | Statement::Insert(_)
                    | Statement::Delete(_)
                    | Statement::CreateIndex(_)) => Ok(Statement::Explain(Box::new(stmt))),
                    other => Err(DbError::Parser(format!(
                        "EXPLAIN must be followed by a valid COMMAND, got: {:?}",
                        other
                    ))),
                }
            }
            Token::Select => Ok(Statement::Select(self.parse_select()?)),
            Token::Create => {
                self.advance()?;
                match &self.current {
                    Token::Table => Ok(Statement::Create(self.parse_create()?)),
                    Token::Index => Ok(Statement::CreateIndex(self.parse_create_index()?)),
                    other => Err(DbError::Parser(format!(
                        "Expected TABLE or INDEX after CREATE, got {:?}",
                        other
                    ))),
                }
            }
            Token::Insert => Ok(Statement::Insert(self.parse_insert()?)),
            Token::Update => Ok(Statement::Update(self.parse_update()?)),
            Token::Delete => Ok(Statement::Delete(self.parse_delete()?)),
            other => Err(DbError::Parser(format!("Unsupported statement type: {:?}", other))),
        }
    }

    /// ── CREATE / INSERT / UPDATE / DELETE ──

    fn parse_create(&mut self) -> Result<CreateStatement, DbError> {
        self.expect(&Token::Table)?;
        let table = self.expect_identifier()?;

        self.expect(&Token::LeftParenthesis)?;
        let mut columns = Vec::new();
        let mut primary_key = None;
        let mut indexes = Vec::new();

        loop {
            match &self.current {
                Token::Primary => {
                    self.advance()?;
                    self.expect(&Token::Key)?;
                    self.expect(&Token::LeftParenthesis)?;
                    let pk_col = self.expect_identifier()?;
                    self.expect(&Token::RightParenthesis)?;
                    primary_key = Some(pk_col);
                }
                Token::Index => {
                    self.advance()?;
                    self.expect(&Token::LeftParenthesis)?;
                    let idx_col = self.expect_identifier()?;
                    self.expect(&Token::RightParenthesis)?;
                    indexes.push(idx_col);
                }
                Token::Identifier(_) => {
                    let name = self.expect_identifier()?;
                    let col_type = match &self.current {
                        Token::Identifier(typ) => {
                            let t = typ.clone();
                            self.advance()?;
                            t
                        }
                        t => return Err(DbError::Parser(format!("Expected type after column name, got {:?}", t))),
                    };
                    columns.push((name, col_type));
                }
                _ => break,
            }
            if self.current == Token::Comma {
                self.advance()?;
            } else {
                break;
            }
        }

        self.expect(&Token::RightParenthesis)?;
        if self.current == Token::Semicolon {
            self.advance()?;
        }

        Ok(CreateStatement {
            table,
            columns,
            primary_key,
            indexes,
        })
    }

    fn parse_create_index(&mut self) -> Result<CreateIndexStatement, DbError> {
        self.expect(&Token::Index)?;

        let mut index_name = None;
        if let Token::Identifier(name) = &self.current {
            index_name = Some(name.clone());
            self.advance()?;
        }

        self.expect(&Token::On)?;
        let table = self.expect_identifier()?;

        self.expect(&Token::LeftParenthesis)?;
        let column = self.expect_identifier()?;
        self.expect(&Token::RightParenthesis)?;

        if self.current == Token::Semicolon {
            self.advance()?;
        }

        Ok(CreateIndexStatement {
            table,
            column,
            index_name,
        })
    }

    fn parse_insert(&mut self) -> Result<InsertStatement, DbError> {
        self.expect(&Token::Insert)?;
        self.expect(&Token::Into)?;

        let table = self.expect_identifier()?;
        let columns = if self.current == Token::LeftParenthesis {
            self.advance()?;
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_identifier()?);
                if self.current == Token::Comma {
                    self.advance()?;
                } else {
                    break;
                }
            }
            self.expect(&Token::RightParenthesis)?;
            cols
        } else {
            Vec::new()
        };

        self.expect(&Token::Values)?;
        self.expect(&Token::LeftParenthesis)?;

        let mut values = Vec::new();
        loop {
            let v = match &self.current {
                Token::StringLiteral(s) => {
                    let vv = s.clone();
                    self.advance()?;
                    vv
                }
                Token::NumberLiteral(raw) => {
                    let vv = raw.clone();
                    self.advance()?;
                    vv
                }
                Token::Identifier(s) => {
                    let vv = s.clone();
                    self.advance()?;
                    vv
                }
                other => return Err(DbError::Parser(format!("Expected value in VALUES, got {:?}", other))),
            };
            values.push(v);
            if self.current == Token::Comma {
                self.advance()?;
            } else {
                break;
            }
        }

        self.expect(&Token::RightParenthesis)?;
        if self.current == Token::Semicolon {
            self.advance()?;
        }

        Ok(InsertStatement {
            table,
            columns,
            values,
        })
    }

    fn parse_update(&mut self) -> Result<UpdateStatement, DbError> {
        self.expect(&Token::Update)?;
        let table = self.expect_identifier()?;
        self.expect(&Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let col = self.parse_columnref()?;
            self.expect(&Token::BinaryOperator(BinaryOperator::Equal))?;
            let expr = self.parse_expr()?;
            let simplified = simplify_expr(expr);
            assignments.push((col, simplified));

            if self.current == Token::Comma {
                self.advance()?;
            } else {
                break;
            }
        }

        let condition_expr = if self.current == Token::Where {
            self.advance()?; 
            Some(self.parse_condition_expr()?)
        } else {
            None
        };

        if self.current == Token::Semicolon {
            self.advance()?;
        }

        Ok(UpdateStatement {
            table,
            assignments,
            condition_expr,
        })
    }

    fn parse_delete(&mut self) -> Result<DeleteStatement, DbError> {
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;

        let table = self.expect_identifier()?;

        let condition_expr = if self.current == Token::Where {
            self.advance()?;
            Some(self.parse_condition_expr()?)
        } else {
            None
        };

        if self.current == Token::Semicolon {
            self.advance()?;
        }

        Ok(DeleteStatement { table, condition_expr })
    }
}

// ══════════════════════════════════════ EXPRESSION ══════════════════════════════════════

/// ------------------ EXPRESSION ANALISYS ------------------
/// Simplifies an expression by handling unary +/- patterns and basic arithmetic.
pub fn simplify_expr(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_simplified = simplify_expr(*left);
            let right_simplified = simplify_expr(*right);

            match (left_simplified, op, right_simplified) {
                (Expr::Literal(ref lit), ArithmeticOp::Subtract, expr_right) if lit == "0" => {
                    match unwrap_unary(expr_right) {
                        (sign, base_expr) => {
                            match (sign, base_expr) {
                                (1, expr) => Expr::UnaryOp {
                                    op: UnaryOp::Minus,
                                    expr: Box::new(expr),
                                },
                                (-1, expr) => expr,
                                _ => unreachable!(),
                            }
                        }
                    }
                }
                (left_expr, ArithmeticOp::Subtract, Expr::Literal(ref lit)) if lit == "0" => {
                    left_expr
                }
                (Expr::Literal(ref lit), ArithmeticOp::Add, right_expr) if lit == "0" => {
                    right_expr
                }
                (left_expr, ArithmeticOp::Add, Expr::Literal(ref lit)) if lit == "0" => {
                    left_expr
                }
                (left_expr, op, right_expr) => Expr::BinaryOp {
                    left: Box::new(left_expr),
                    op,
                    right: Box::new(right_expr),
                },
            }
        }

        Expr::UnaryOp { op, expr } => {
            let simplified_expr = simplify_expr(*expr);
            match simplified_expr {
                Expr::Literal(ref lit) => {
                    if let Ok(num) = lit.parse::<f64>() {
                        match op {
                            UnaryOp::Minus => Expr::Literal((-num).to_string()),
                            UnaryOp::Plus => Expr::Literal(num.to_string()),
                        }
                    } else {
                        Expr::UnaryOp {
                            op,
                            expr: Box::new(Expr::Literal(lit.clone())),
                        }
                    }
                }
                _ => Expr::UnaryOp {
                    op,
                    expr: Box::new(simplified_expr),
                },
            }
        }

        other => other,
    }
}

fn unwrap_unary(expr: Expr) -> (i32, Expr) {
    match expr {
        Expr::UnaryOp { op, expr: inner_expr } => {
            let (inner_sign, base) = unwrap_unary(*inner_expr);
            match op {
                UnaryOp::Minus => (-inner_sign, base),
                UnaryOp::Plus => (inner_sign, base),
            }
        }
        Expr::BinaryOp { left, op, right } => {
            if op == ArithmeticOp::Subtract {
                if let Expr::Literal(ref lit_str) = *left {
                    if lit_str == "0" {
                        let (inner_sign, base_expr) = unwrap_unary(*right);
                        return (-inner_sign, base_expr);
                    }
                }
            }
            (1, Expr::BinaryOp { left, op, right })
        }
        other => (1, other),
    }
}


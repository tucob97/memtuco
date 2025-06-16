//! Custom Error for memtuco RDBMS
//! This module came last and all other module is changed
//! for unify and customize error. So refactoring all code
//! was very painful. I hope that now error propagation make sense


use std::io;
use std::fmt;
use std::error::{Error};
use std::time;
use std::time::SystemTimeError;

#[derive(Debug)]
pub enum DbError {
    Io(std::io::Error),
    SysTime(std::time::SystemTimeError),
    Tokenizer(String),
    Parser(String),
    Transaction(String),
    DbEngine(String),
    Constant(String),
    Page(String),
    Planner(String),
    GroupIter(String),
    Order(String),
    BTree(String),
    Bintuco(String),
    Other(String), 

}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(e) => write!(f, "IO error: {}", e),
            DbError::SysTime(e) => write!(f, "Sys error: {}", e),
            DbError::Tokenizer(msg) => write!(f, "Tokenizer error: {}", msg),
            DbError::Parser(msg) => write!(f, "Parser error: {}", msg),
            DbError::Transaction(msg) => write!(f, "Transaction error: {}", msg),
            DbError::DbEngine(msg) => write!(f, "DbEngine error: {}", msg),
            DbError::Constant(msg) => write!(f, "Constant error: {}", msg),
            DbError::Page(msg) => write!(f, "Page error: {}", msg),
            DbError::Planner(msg) => write!(f, "Planner error: {}", msg),
            DbError::GroupIter(msg) => write!(f, "GroupIter error: {}", msg),
            DbError::Order(msg) => write!(f, "Order error: {}", msg),
            DbError::BTree(msg) => write!(f, "BTree error: {}", msg),
            DbError::Bintuco(msg) => write!(f, "Bintuco error: {}", msg),
            DbError::Other(msg) => write!(f, "Other error: {}", msg),
        }
    }
}

impl Error for DbError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DbError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::Io(e)
    }
}

impl From<time::SystemTimeError> for DbError {
    fn from(e: std::time::SystemTimeError) -> Self {
        DbError::SysTime(e)
    }
}
use std::iter;
use std::str::FromStr;
use std::collections::HashMap;
use std::fs::File;
use std::io::{ Read, Seek, SeekFrom, Write};
use std::fs::OpenOptions;
use crate::tokenizer::*;
use std::fmt;
use crate::bintuco;
use std::path::{Path, PathBuf};
use crate::dbengine::*;
use crate::planner::*;
use crate::constant::{OVERFLOW_SENTINEL, PAGE_CAPACITY, PAGE_SIZE, PAYLOAD_SIZE, OVERFLOW_KEY, PAYLOAD_MIN, PADDING_ROW_BYTES};
use crate::btree::*;
use crate::dberror::DbError;

/// ## Page Layout, Row Encoding, and OverflowPage
///
/// /// This module defines how data is physically laid out on disk. It handles the
/// /// structure of pages, how rows are serialized, and what happens when a row is
/// /// too large to fit on a single page.
///
/// ### Row and Cell Structure
///
/// /// -   `Row`: A `Row` is a simple `Vec<SqlValue>`, representing a single record.
/// /// -   `Cell`: A `Cell` is the storage unit for a row within a data page. It contains
/// ///     the `row_payload` (the serialized bytes of a `Row`) and an `overflow_page`
/// ///     pointer. If the row fits on the page, this pointer is 0.
///
/// ### Row Encoding with `bintuco`
///
/// /// Rows are serialized to a byte vector using the `bintuco` crate. This creates a
/// /// compact, binary representation of the `Row` struct. For example, a `Row`
/// /// containing `[SqlValue::INT(10), SqlValue::VARCHAR("hello")]` is converted into a
/// /// byte stream that encodes the enum variants and their associated data. This is
/// /// more space-efficient than storing data as text.
///
/// ### Overflow Page Logic
///
/// /// A standard page has a limited capacity. If a row's serialized byte stream is
/// /// larger than the `OVERFLOW_SENTINEL`, it cannot be stored in a single `Cell`.
/// /// In this case, the row is split across a primary `Cell` and a linked list of
/// /// `OverflowPage`s.
///
/// /// 1.  The first chunk of the row's data (up to the `OVERFLOW_SENTINEL` size) is
/// ///     stored in the `row_payload` of the main `Cell`.
/// /// 2.  The `overflow_page` field of this `Cell` is set to the page index of the
/// ///     first `OverflowPage`.
/// /// 3.  The rest of the row's data is chunked and stored in a series of `OverflowPage`s.
/// /// 4.  Each `OverflowPage` contains a chunk of the payload and a pointer to the
/// ///     next `OverflowPage` in the chain. The last page in the chain has its
/// ///     `overflow_page` pointer set to 0.
///
/// ///
/// /// /// Visualizing Overflow Storage:
/// ///
/// /// /// Storing a very large row that requires two overflow pages.
/// ///
/// /// /// TablePage (e.g., at page index 5)
/// /// /// +--------------------------------------------------------------------------+
/// /// /// | Header, other cells...                                                   |
/// /// /// +--------------------------------------------------------------------------+
/// /// /// | Cell for our large row:                                                  |
/// /// /// |   - overflow_page: 12  --------------------------------------------------->-+   
/// /// /// |   - row_payload: [ first chunk of bytes... ]                             |  |
/// /// /// +--------------------------------------------------------------------------+  |
/// /// ///                                                                               |
/// /// /// OverflowPage (at page index 12)                                               |
/// /// /// +--------------------------------------------------------------------------+  |
/// /// /// | - page_num: 12        <----------------------------------------------------<+
/// /// /// | - overflow_page: 13  ----------------------------------------------------->-+  
/// /// /// | - row_payload: [ second chunk of bytes... ]                              |  |
/// /// /// +--------------------------------------------------------------------------+  |
/// /// ///                                                                               |
/// /// /// OverflowPage (at page index 13)                                               |
/// /// /// +--------------------------------------------------------------------------+  |
/// /// /// | - page_num: 13        <----------------------------------------------------<+
/// /// /// | - overflow_page: 0 (End of chain)                                        | 
/// /// /// | - row_payload: [ final chunk of bytes... ]                               |
/// /// /// +--------------------------------------------------------------------------+
///
///+----------------------------------------------------------------------------------------+
///|                                     TablePage (1024 bytes)                             |
///+----------------------------------------------------------------------------------------+
///|                                   Serialized Data                                      |
///|                                    (Variable Size)                                     |
///|                                                                                        |
///|   +-----------------------------------+                                                |
///|   | index: usize                      | // Page ID                                     |
///|   | num_cell: u16                     | // Count of cells (rows) on this page          |
///|   | row_idnum: usize                  | // A counter of rows presents                  |
///|   | pointer_cell: HashMap<usize, u16> | // Maps a global RowId to an index in `veccell`|
///|   | veccell: Vec<Cell>                | // Vector containing the actual cell data      |
///|   +-----------------------------------+                                                |
///|                                                                                        |
///+----------------------------------------------------------------------------------------+
///|                                   Free Space (Padding)                                 |
///|                                (Remaining bytes up to 1024)                            |
///+----------------------------------------------------------------------------------------+
///
/// /// /// +--------------------------------------------------------------------------+
/// /// /// |                     OverflowPage (1024 bytes)                            |
/// /// /// +--------------------------------------------------------------------------+
/// /// /// | Serialized Data of the `OverflowPage` struct:                            |
/// /// /// |                                                                          |
/// /// /// | `page_num`: 12 (The index of this page itself)                           |
/// /// /// | `overflow_page`: 13 (Pointer to the *next* overflow page, or 0 if last)  |
/// /// /// | `row_payload`: [ A chunk of the large row's serialized bytes... ]        |
/// /// /// |                                                                          |
/// /// /// +--------------------------------------------------------------------------+
/// /// /// | Unused Space (Padding to 1024 bytes)                                     |
/// /// /// +--------------------------------------------------------------------------+


/// ### Free Space Management with a B-Tree
///
/// /// To efficiently find a page with enough space to insert a new row, each table
/// /// maintains a dedicated "free space" B-Tree (`page_vec` in the `TableRoot` struct).
/// /// This B-Tree does not store row data; instead, it indexes the amount of free space
/// /// available on each data page.
///
/// /// -   Key: The B-Tree key is an `SqlValue::UINT` representing the number of
/// ///     free bytes on a page.
/// /// -   Value: The `page` field of the `Triplet` stores the index of the data page.
///
/// /// When inserting a new row that requires `N` bytes, the `first_free_page` function
/// /// performs a `search_value_at_least(N)` on this B-Tree. This quickly finds a page
/// /// with `N` or more bytes free. If no such page exists, a new page is allocated.
/// /// After an insert, the page's old free-space entry is removed from the B-Tree and
/// /// a new entry with the updated (smaller) free space is inserted.
///
/// ///
/// /// /// Visualizing an Insert using the Free Space B-Tree:
/// ///
/// /// /// Goal: Insert a new row requiring 200 bytes of space.
/// ///
/// /// /// +--------------------------+
/// /// /// | INSERT INTO ...          |
/// /// /// | (Requires 200 bytes)     |
/// /// /// +------------+-------------+
/// /// ///              |
/// /// /// +------------v----------------------+
/// /// /// | TableRoot::insert_cell()          |
/// /// /// | Calls `first_free_page(200, ...)` |
/// /// /// +------------+----------------------+
/// /// ///              |
/// /// /// +------------v---------------------------------+
/// /// /// | Btree::search_value_at_least(key: UINT(200)) |
/// /// /// | on the `page_vec` B-Tree                     |
/// /// /// +------------+---------------------------------+
/// /// ///              | Search...
/// /// /// +------------v---------------------------------------------------------------+
/// /// /// |                  Free Space B-Tree (`page_vec`)                            |
/// /// /// +----------------------------------------------------------------------------+
/// /// /// | Root Node: [ Key: UINT(450), ChildPtr ]                                    |
/// /// /// |                               |                                            |
/// /// /// |      +------------------------+------------------+                         |
/// /// /// |      V                                           V                         |
/// /// /// | Leaf Node 1:                         Leaf Node 2:                          |
/// /// /// | [ Triplet{key: UINT(100), page: 7} ]  [ Triplet{key: UINT(500), page: 4} ] |
/// /// /// | [ Triplet{key: UINT(150), page: 2} ]  [ Triplet{key: UINT(800), page: 9} ] |
/// /// /// |                                      (Search finds this one first)         |
/// /// /// +----------------------------------------------------------------------------+
/// /// ///              |
/// /// ///              | Returns Triplet { key: UINT(500), page: 4 }
/// /// ///              |
/// /// /// +------------v--------------------------------------------+
/// /// /// | `first_free_page` returns page index 4.                 |
/// /// /// | The row is inserted into data page 4.                   |
/// /// /// | The `page_vec` is updated: the entry for page 4 is      |
/// /// /// | removed and a new one is inserted with key UINT(300).   |
/// /// /// +---------------------------------------------------------+
/// 
/// No keys in internal node are removed, so we remove only keys in leaf node.

/// /// ///
/// ### The Special Case of the `OVERFLOW_KEY`
///
/// /// The free space B-Tree has a special mechanism for recycling overflow pages.
/// /// An `OverflowPage` is a full-page structure, so it cannot be used for general
/// /// row storage and is therefore not tracked in the B-Tree based on its available
/// /// byte count.
///
/// /// Instead, when an overflow chain is deleted (e.g., during a `DELETE` or `UPDATE`
/// /// operation), the pages in that chain are marked for reuse by inserting a special
/// /// `Triplet` into the free space B-Tree:
///
/// /// `Triplet { key: SqlValue::UINT(OVERFLOW_KEY), page: <freed_page_index> }`
///
/// /// The `allocate_overflow_page` function leverages this. Before allocating a new
/// /// page from the end of the file, it first searches the free space B-Tree for an
/// /// entry with the exact `OVERFLOW_KEY`. If one is found, that page is recycled.
/// /// This prevents the data file from growing indefinitely if large rows are
/// /// frequently updated or deleted.
///
/// ///
/// /// /// Visualizing Overflow Page Recycling:
/// ///
/// /// /// 1. An overflow page at index 13 is part of a deleted row.
/// /// ///    The system inserts a special marker into the free-space B-Tree.
/// ///
/// /// /// +------------------------+
/// /// /// | DELETE or UPDATE frees |
/// /// /// | overflow page 13       |
/// /// /// +-----------+------------+
/// /// ///             |
/// /// /// +-----------v----------------------------------------------------+
/// /// /// | `page_vec`.insert( Triplet { key: OVERFLOW_KEY, page: 13 } )   |
/// /// /// +----------------------------------------------------------------+
/// ///
/// /// /// 2. A new large row needs an overflow page.
/// /// ///    The `allocate_overflow_page` function runs.
/// ///
/// /// /// +-----------------------------+
/// /// /// | `allocate_overflow_page()`  |
/// /// /// +-------------+---------------+
/// /// ///               |
/// /// /// +-------------v------------------------------------------------+
/// /// /// | 1. Search `page_vec` for a Triplet where key == OVERFLOW_KEY |
/// /// /// +-------------+------------------------------------------------+
/// /// ///               |
/// /// ///               | It finds `Triplet { key: OVERFLOW_KEY, page: 13 }`
/// /// ///               |
/// /// /// +-------------v----------------------------------------------+
/// /// /// | 2. Reset that Triplet with 0 space count from the B-Tree.  |
/// /// /// +-------------+----------------------------------------------+
/// /// ///               |
/// /// /// +-------------v---------------------------------------------+
/// /// /// | 3. Return the page index: 13. This page will be reused.   |
/// /// /// +-----------------------------------------------------------+



// ══════════════════════════════════════ SQL TYPE ══════════════════════════════════════

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum SqlValue {
    TINYINT(i8),
    SMALLINT(i16),
    INT(i32),
    BIGINT(i64),
    UTINYINT(u8),
    USMALLINT(u16),
    UINT(u32),
    UBIGINT(u64),
    FLOAT(f32),    
    DOUBLE(f64),
    CHAR(Vec<u8>),
    VARCHAR(Vec<u8>),
    //TEXT(String)  // Here to remember
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlColumnType {
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    UTINYINT,
    USMALLINT,
    UINT,
    UBIGINT,
    FLOAT,
    DOUBLE,
    CHAR,       
    VARCHAR,   
    //TEXT
}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::TINYINT(v) => write!(f, "{}", v),
            SqlValue::SMALLINT(v) => write!(f, "{}", v),
            SqlValue::INT(v) => write!(f, "{}", v),
            SqlValue::BIGINT(v) => write!(f, "{}", v),
            SqlValue::UTINYINT(v) => write!(f, "{}", v),
            SqlValue::USMALLINT(v) => write!(f, "{}", v),
            SqlValue::UINT(v) => write!(f, "{}", v),
            SqlValue::UBIGINT(v) => write!(f, "{}", v),
            SqlValue::FLOAT(v) => write!(f, "{}", v),
            SqlValue::DOUBLE(v) => write!(f, "{}", v),
            SqlValue::CHAR(v) => write!(f, "{}", String::from_utf8_lossy(v)),
            SqlValue::VARCHAR(v) => write!(f, "{}", String::from_utf8_lossy(v)),
        }
    }
}
impl FromStr for SqlColumnType {
    type Err = DbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "TINYINT"    => Ok(SqlColumnType::TINYINT),
            "SMALLINT"   => Ok(SqlColumnType::SMALLINT),
            "INT"        => Ok(SqlColumnType::INT),
            "BIGINT"    => Ok(SqlColumnType::BIGINT),
            "UINT"       => Ok(SqlColumnType::UINT),
            "UTINYINT"   => Ok(SqlColumnType::UTINYINT),
            "USMALLINT"  => Ok(SqlColumnType::USMALLINT),
            "UBIGINT"    => Ok(SqlColumnType::UBIGINT),
            "FLOAT"      => Ok(SqlColumnType::FLOAT),
            "DOUBLE"     => Ok(SqlColumnType::DOUBLE),
            "CHAR"       => Ok(SqlColumnType::CHAR),
            "VARCHAR"    => Ok(SqlColumnType::VARCHAR),
            _ => Err(DbError::Parser(format!("Unknown column type: {}", s))),
        }
    }
}

// ══════════════════════════════════════ ROW STRUCT ══════════════════════════════════════






///Row implementation
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<SqlValue>,
}

impl Row {
    pub fn to_bytes(&self) -> Result<Vec<u8>, DbError> {
        bintuco::encode_to_vec(self).map_err(|e| DbError::Bintuco(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DbError> {
        bintuco::decode_from_slice(bytes).map(|(row, _)| row).map_err(|e| DbError::Bintuco(e.to_string()))
    }
    pub fn pretty_string(&self) -> String {
        let mut s = String::new();
        for val in &self.values {
            let formatted = match val {
                SqlValue::CHAR(bytes) => format!(" {:<8} ", String::from_utf8_lossy(bytes)),
                SqlValue::VARCHAR(bytes) => format!(" {:<8} ", String::from_utf8_lossy(bytes)),
                // SqlValue::TEXT(s) => format!(" {:<8} ", s), // Uncomment if you have TEXT
                SqlValue::UTINYINT(n) => format!(" {:<8} ", n),
                SqlValue::USMALLINT(n) => format!(" {:<8} ", n),
                SqlValue::UINT(n) => format!(" {:<8} ", n),
                SqlValue::TINYINT(n) => format!(" {:<8} ", n),
                SqlValue::SMALLINT(n) => format!(" {:<8} ", n),
                SqlValue::INT(n) => format!(" {:<8} ", n),
                SqlValue::BIGINT(n) => format!(" {:<8} ", n),
                SqlValue::UBIGINT(n) => format!(" {:<8} ", n),
                SqlValue::FLOAT(f) => format!(" {:<8.3} ", f),
                SqlValue::DOUBLE(d) => format!(" {:<8.3} ", d),
            };
            s.push_str(&formatted);
        }
        s
    }
    pub fn pretty_print(&self) {
        for val in &self.values {
            match val {
                SqlValue::CHAR(bytes) => print!(" {:<5} ", String::from_utf8_lossy(bytes)),
                SqlValue::VARCHAR(bytes) => print!(" {:<5} ", String::from_utf8_lossy(bytes)),
                //SqlValue::TEXT(s) => print!(" {:<5} ", s),
                SqlValue::UTINYINT(n) => print!(" {:<5} ", n),
                SqlValue::USMALLINT(n) => print!(" {:<5} ", n),
                SqlValue::UINT(n) => print!(" {:<5} ", n),
                SqlValue::TINYINT(n) => print!(" {:<5} ", n),
                SqlValue::SMALLINT(n) => print!(" {:<5} ", n),
                SqlValue::INT(n) => print!(" {:<5} ", n),
                SqlValue::BIGINT(n) => print!(" {:<5} ", n),
                SqlValue::UBIGINT(n) => print!(" {:<5} ", n),
                SqlValue::FLOAT(f) => print!(" {:<5.3} ", f),   // adjust precision if needed
                SqlValue::DOUBLE(d) => print!(" {:<5.3} ", d), // adjust precision if needed
            }
        }
        println!();
    }
}

// ══════════════════════════════════════ CELL STRUCT ══════════════════════════════════════

/// Cell implementation [ pointer_to_overflow_page + payload]
#[derive(Debug, Clone)]
pub struct Cell {
    pub overflow_page: usize,
    pub row_payload: Vec<u8>,
}

impl Cell {
    pub fn new(row: &Row, overflow_page: usize) -> Result<Self, DbError> {
        let row_payload = row.to_bytes()?;
        Ok(Self {
            overflow_page,
            row_payload,
        })
    }

    pub fn new_over(row: &[u8], overflow_page: usize) -> Self {
        let row_payload = row.to_vec();
        Self {
            overflow_page,
            row_payload,
        }
    }

    pub fn decode_cell(&self) -> Result<Row, DbError> {
        Row::from_bytes(&self.row_payload)
    }

    pub fn payload_len(&self) -> usize {
        self.row_payload.len()
    }
}



// ══════════════════════════════════════ OVERFLOW PAGE ══════════════════════════════════════

/// Overflow Page struct [page_num + pointer_to_next_overflow_page + payload] = 1024 bytes
#[derive(Debug, Clone)]
pub struct OverflowPage{
    pub page_num: usize,
    pub overflow_page: usize,
    pub row_payload: Vec<u8>,
}


// ══════════════════════════════════════ TABLE SCHEMA  ══════════════════════════════════════

/// Table schema -> Table metadata
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<SqlColumnType>,
    pub name_col: Vec<String>,
    pub primary_key: Option<String>,
}

impl TableSchema {
    pub fn new(name: &str, columns: Vec<SqlColumnType>, name_col: Vec<&str>, pk: Option<&str>) -> Result<Self, DbError> {
        if columns.len() != name_col.len() {
            return Err(DbError::Page("Input count mismatch".to_string()));
        }
        let name_col_strings: Vec<String> = name_col.into_iter().map(|s| s.to_string()).collect();
    
        let primary_key = match pk {
            Some(key) => {
                if name_col_strings.contains(&key.to_string()) {
                    Some(key.to_string())
                } else {
                    return Err(DbError::Page(format!("Primary key '{}' not found in columns", key)));
                }
            }
            None => None,
        };

        Ok(Self {
            name: name.to_string(),
            columns,
            name_col: name_col_strings,
            primary_key,
        })
        }

}

impl TableSchema {
    pub fn get_columns(&self) -> &Vec<SqlColumnType> {
        &self.columns
    }
    pub fn get_columns_name(&self) -> &Vec<String> {
        &self.name_col
    }
    pub fn print_columns_name(&self) {
        println!("Table:");
        for name in &self.name_col {
            print!("    - {}", name);
        }
        print!(" ");
        println!("--------------------------------");
    }
}

impl TableSchema {
    pub fn set_row(&self, inputs: Vec<&str>) -> Result<Row, DbError> {
        if inputs.len() != self.columns.len() {
            return Err(DbError::Page("Input count mismatch".to_string()));
        }

        let mut values = Vec::new();

        for (col, inp) in self.columns.iter().zip(inputs) {
            let val = match col {
                SqlColumnType::TINYINT => {
                    let v = i8::from_str(inp).map_err(|_| DbError::Page(format!("Invalid TINYINT: {}", inp)))?;
                    SqlValue::TINYINT(v)
                }
                SqlColumnType::SMALLINT => {
                    let v = i16::from_str(inp).map_err(|_| DbError::Page(format!("Invalid SMALLINT: {}", inp)))?;
                    SqlValue::SMALLINT(v)
                }
                SqlColumnType::INT => {
                    let v = i32::from_str(inp).map_err(|_| DbError::Page(format!("Invalid INT: {}", inp)))?;
                    SqlValue::INT(v)
                }
                SqlColumnType::BIGINT => {
                    let v = i64::from_str(inp).map_err(|_| DbError::Page(format!("Invalid BIGINT: {}", inp)))?;
                    SqlValue::BIGINT(v)
                }
                SqlColumnType::UBIGINT => {
                    let v = u64::from_str(inp).map_err(|_| DbError::Page(format!("Invalid UBIGINT: {}", inp)))?;
                    SqlValue::UBIGINT(v)
                }
                SqlColumnType::UTINYINT => {
                    let v = u8::from_str(inp).map_err(|_| DbError::Page(format!("Invalid UTINYINT: {}", inp)))?;
                    SqlValue::UTINYINT(v)
                }
                SqlColumnType::USMALLINT => {
                    let v = u16::from_str(inp).map_err(|_| DbError::Page(format!("Invalid USMALLINT: {}", inp)))?;
                    SqlValue::USMALLINT(v)
                }
                SqlColumnType::UINT => {
                    let v = u32::from_str(inp).map_err(|_| DbError::Page(format!("Invalid UINT: {}", inp)))?;
                    SqlValue::UINT(v)
                }
                
                // If you added FLOAT and DOUBLE types:
                SqlColumnType::FLOAT => {
                    let v = f32::from_str(inp).map_err(|_| DbError::Page(format!("Invalid FLOAT: {}", inp)))?;
                    SqlValue::FLOAT(v)
                }
                SqlColumnType::DOUBLE => {
                    let v = f64::from_str(inp).map_err(|_| DbError::Page(format!("Invalid DOUBLE: {}", inp)))?;
                    SqlValue::DOUBLE(v)
                }
                SqlColumnType::CHAR => {
                    let bytes = inp.as_bytes().to_vec();
                    if bytes.len() > 255 {
                        return Err(DbError::Page("CHAR too long".to_string()));
                    } 
                    SqlValue::CHAR(bytes)
                }
                SqlColumnType::VARCHAR => {
                    let bytes = inp.as_bytes().to_vec();
                    if bytes.len() > 8000 {
                        return Err(DbError::Page("VARCHAR too long".to_string()));
                    }
                    SqlValue::VARCHAR(bytes)
                }
            };
            values.push(val);
        }

        Ok(Row { values })
    }

    // Not used ( Some RDBMS put variable lenght data at the end of the payload)
    /*
    pub fn reordered_columns(&self) -> Vec<SqlColumnType> {
        let mut fixed = vec![];
        let mut variable = vec![];

        for col in &self.columns {
            match col {
                SqlColumnType::UTINYINT
                | SqlColumnType::USMALLINT 
                | SqlColumnType::UINT
                | SqlColumnType::TINYINT
                | SqlColumnType::SMALLINT 
                | SqlColumnType::INT
                | SqlColumnType::BIGINT
                | SqlColumnType::UBIGINT  
                | SqlColumnType::FLOAT 
                | SqlColumnType::DOUBLE => fixed.push(col.clone()),

                SqlColumnType::CHAR
                | SqlColumnType::VARCHAR => variable.push(col.clone()),
            }
        }

        fixed.extend(variable);
        fixed
    }
    */

}

impl TableSchema {
    pub fn from_column_names(names: Vec<String>) -> Self {
        TableSchema {
            name: "__virtual__".to_string(),
            columns: vec![SqlColumnType::VARCHAR; names.len()],
            name_col: names,
            primary_key: None,
        }
    }
}


// ══════════════════════════════════════ DATABASE ROOT PAGE ══════════════════════════════════════


/// The "Root Page" of the database
#[derive(Debug, Clone)]
pub struct DatabasePage{
    pub i_magic: usize,    // magic value to understand endianess & db version
    pub db_folder: PathBuf,
    pub all_tableroot: Vec<TableRoot>,
}

impl DatabasePage {
    pub fn new_empty(db_folder: PathBuf) -> Self {
        Self {
            i_magic: 0xDEADBEEF,
            db_folder,
            all_tableroot: Vec::new(),
        }
    }

    pub fn print_table_schema(&self, name: &str) {
        if let Some(tab) = self.all_tableroot.iter().find(|t| t.tb_schema.name == name) {
            println!("Table Name: {}", tab.tb_schema.name);
            println!("Primary Key: {:?}", tab.tb_schema.primary_key);
            println!("Columns:");
            for (i, (col_name, col_type)) in tab.tb_schema.name_col.iter().zip(tab.tb_schema.columns.iter()).enumerate() {
                println!("  {}: {} - {:?}", i, col_name, col_type);
            }
        } else {
            println!("Table '{}' not found.", name);
        }
    }

    pub fn print_all_schema(&self) {
        for tab in &self.all_tableroot {
            println!("Table Name: {}", tab.tb_schema.name);
            print!("Columns:");
            for (i, col) in tab.tb_schema.columns.iter().enumerate() {
                print!("  {}: {:?}", i, col);
            }
            println!("\n");
        }
    }

}

impl DatabasePage {
    /// Insert a prebuilt `Row` into `table_name`, using a shared StorageManager.
    pub fn insert_into_table(
        &mut self,
        table_name: &str,
        row: Row,
        storage: &StorageManager,
    ) -> Result<(), DbError> {
        // 1) Find the matching TableRoot
        if let Some(table_root) = self
            .all_tableroot
            .iter_mut()
            .find(|t| t.tb_schema.name == table_name)
        {
            // 2) Delegate to insert_cell, passing the same storage
            table_root.insert_cell(row, storage)?;
            Ok(())
        } else {
            Err(DbError::Page(format!("Table '{}' not found", table_name)))
        }
    }
}


impl DatabasePage {
    /// Create a brand‐new table in this database.
    ///
    /// - `schema` describes its name, columns, and (optional) primary key.
    /// - `storage` is the shared StorageManager; we will register the two files:
    ///     • “<table>.meta_data.tucodb” for the B-tree metadata
    ///     • “<table>_data.tucodb”       for the actual rows
    pub fn add_table(
        &mut self,
        schema: TableSchema,
        storage: &mut StorageManager,
    ) -> Result<(), DbError> {
        // --- ADD THIS CHECK ---
        // 1) Check if a table with this name already exists.
        if self.all_tableroot.iter().any(|t| t.tb_schema.name == schema.name) {
            return Err(DbError::DbEngine(format!(
                "Table '{}' already exists.",
                schema.name
            )));
        }
        // --- END OF CHANGE ---
        
        // 1) Use TableRoot::new to create & register everything
        //    (it expects: name, columns, name_col, pk, base_dir, storage)
        let base_dir = &self.db_folder;
        let new_root = TableRoot::new(
            &schema.name,
            schema.columns.clone(),
            schema
                .name_col
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
            schema.primary_key.as_deref(),
            base_dir,
            storage,
        )?;

        // 2) Push this TableRoot into our DatabasePage’s list
        self.all_tableroot.push(new_root);
        Ok(())
    }
}



impl DatabasePage {
    /// Print the first `num_to_print` rows of `table_name`.
    /// Now takes `storage: &StorageManager` instead of opening files directly.
    pub fn print_head_cells(
        &self,
        table_name: &str,
        num_to_print: usize,
        storage: &StorageManager,
    ) {
        // 1) Find the TableRoot
        if let Some(table_root) = self
            .all_tableroot
            .iter()
            .find(|t| t.tb_schema.name == table_name)
        {
            // 2) Print column headers
            table_root.tb_schema.print_columns_name();

            // 3) Loop over row IDs from 0 up to min(row_id, num_to_print)
            let max_id = std::cmp::min(table_root.row_id, num_to_print);
            for rowid in 0..max_id {
                if let Ok(Some(row)) = self.read_id_cell(table_name, rowid, storage) {
                    print!("Row {}: ", rowid);
                    row.pretty_print();
                } else {
                    println!("Row {}: [unavailable]", rowid);
                }
            }
        } else {
            println!("Table '{}' not found.", table_name);
        }
    }
}

impl DatabasePage {
    /// Insert a new row (given as Vec<&str>) into `table_name`.
    /// Now takes `storage: &StorageManager` instead of `db_folder: &Path`.
    pub fn insert(
        &mut self,
        table_name: &str,
        input_row: Vec<&str>,
        storage: &StorageManager,
    ) -> Result<(), DbError> {
        // 1) Find the matching TableRoot
        if let Some(table_root) = self
            .all_tableroot
            .iter_mut()
            .find(|t| t.tb_schema.name == table_name)
        {
            // 2) Convert the raw strings to a Row
            let row = table_root.convert_raw_to_row(input_row)?;

            // 3) Delegate to the refactored insert_cell, passing storage
            table_root.insert_cell(row, storage)?;

            Ok(())
        } else {
            Err(DbError::Page(format!("Table '{}' not found", table_name)))
        }
    }
}


impl DatabasePage {
    /// Read a row by `rowid` from the given `table_name`, using a shared StorageManager.
    pub fn read_id_cell(
        &self,
        table_name: &str,
        rowid: usize,
        storage: &StorageManager,
    ) -> Result<Option<Row>, DbError> {
        // 1) Find the matching TableRoot
        if let Some(table_root) = self
            .all_tableroot
            .iter()
            .find(|t| t.tb_schema.name == table_name)
        {
            // 2) Delegate to the refactored `read_exact_cell`, passing `storage`
            return table_root.read_exact_cell(rowid, storage);
        }
        Err(DbError::Page(format!("Table '{}' not found", table_name)))
    }
}



impl DatabasePage {
    /// Search a table by key; returns the matching Row (if any).
    /// Now takes `storage: &StorageManager` instead of `db_folder: &Path`.
    pub fn search_by_key(
        &self,
        table_name: &str,
        key_name: &str,
        key_value_str: &str,
        storage: &StorageManager,
    ) -> Result<Option<Row>, DbError> {
        // 1) Locate the TableRoot for this table_name
        let table_root = self
            .all_tableroot
            .iter()
            .find(|tbl| tbl.name_root == table_name)
            .ok_or_else(|| DbError::Page(format!("Table '{}' not found", table_name)))?;

        // 2) Convert the raw string into a SqlValue, using that table’s schema
        let key_value = table_root.convert_to_sql_value(key_name, key_value_str)?;

        // 3) Perform the B-tree search (which has been refactored to take storage)
        let trip_opt = table_root.tab_search_by_key(key_name, key_value, storage)?;

        // 4) If found, use read_cell_with_page (refactored) to fetch the Row
        if let Some(trip) = trip_opt {
            let row = table_root.read_cell_with_page(trip.rowid, trip.page, storage)?;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
}



impl DatabasePage {
    /// Create a new secondary index on `column` in `table_name`,
    /// registering "<column>.idx", building the B-tree, and back-filling it.
    pub fn create_secondary_index(
        &mut self,
        table_name: &str,
        column: &str,
        storage: &mut StorageManager,
    ) -> Result<(), DbError> {
        // 1) find the table
        let tr = self
            .all_tableroot
            .iter_mut()
            .find(|tr| tr.tb_schema.name == table_name)
            .ok_or_else(|| DbError::Page(format!("CREATE INDEX: table '{}' not found", table_name)))?;

        // --- START OF CHANGES ---
        // 2) Check if the column exists in the table schema.
        if !tr.tb_schema.name_col.iter().any(|col_name| col_name == column) {
            return Err(DbError::DbEngine(format!(
                "Cannot create index: column '{}' does not exist in table '{}'.",
                column, table_name
            )));
        }

        // 3) Check if an index on this column already exists.
        if tr.index_vec.iter().any(|btree| btree.key_name == column) {
            return Err(DbError::DbEngine(format!(
                "Index on column '{}' for table '{}' already exists.",
                column, table_name
            )));
        }
        // --- END OF CHANGES ---

        // 2) register & open the new index file
        let idx_logical = format!("{}.idx", column);
        storage.register_file(&idx_logical)?;

        // 3) build an empty Btree
        let mut btree = Btree::new(column.to_string(), &self.db_folder, storage)?;

        // 4) scan every page of tr.name_file
        let handle = storage
            .get_handle(&tr.name_file)
            .ok_or_else(|| DbError::Page("data file must be registered".to_string()))?;
        let file_len = handle.file_len()? as usize;
        let num_pages = (file_len + PAGE_SIZE - 1) / PAGE_SIZE;

        for page_idx in 0..num_pages {
            let mut buf = vec![0u8; PAGE_SIZE];
            handle.read_at((page_idx * PAGE_SIZE) as u64, &mut buf)?;
            // decode or skip if this isn’t a data page
            let table_page: TablePage = match bintuco::decode_from_slice::<TablePage>(&buf) {
                Ok((tp, _)) => tp,
                Err(_) => continue,
            };
            for (&rowid, &cell_idx) in &table_page.pointer_cell {
                let cell = &table_page.veccell[cell_idx as usize];
                // reassemble overflow if needed...
                let row = if cell.overflow_page == 0 {
                    cell.decode_cell()?
                } else {
                    let mut payload = cell.row_payload.clone();
                    let mut next = cell.overflow_page;
                    while next != 0 {
                        let ovp = read_overflow_page(storage, &idx_logical, next)?;
                        let (chunk, _) = bintuco::decode_from_slice::<Vec<u8>>(&ovp.row_payload)?;
                        payload.extend(chunk);
                        next = ovp.overflow_page;
                    }
                    Row::from_bytes(&payload)?
                };

                // extract the key column's value
                let key_pos = tr
                    .tb_schema
                    .name_col
                    .iter()
                    .position(|c| c == column)
                    .ok_or_else(|| DbError::Page(format!("Column '{}' not in schema", column)))?;
                let key_val = row.values[key_pos].clone();

                // insert triplet
                let trip = Triplet {
                    key: key_val,
                    rowid,
                    page: page_idx,
                };
                btree.insert(trip, storage)?;
            }
        }

        // 5) stash it into the TableRoot
        tr.index_vec.push(btree);
        Ok(())
    }
}


impl DatabasePage {
    pub fn update(
        &mut self,
        table_name: &str,
        assignments: &[(String, Expression)],
        predicate: Option<&Expression>,
        storage: &mut StorageManager,
    ) -> Result<usize, DbError> {
        // 1) find the table root
        let tr_idx = self
            .all_tableroot
            .iter()
            .position(|t| t.tb_schema.name == table_name)
            .ok_or_else(|| DbError::Page(format!("Table '{}' not found", table_name)))?;
        let schema = self.all_tableroot[tr_idx].tb_schema.clone(); // Clone schema before mutable borrow

        let tr = &mut self.all_tableroot[tr_idx];

        // 2) build Vec<(col_idx, expr, type)> with explicit String‐error
        let assigns: Vec<(usize, Expression, SqlColumnType)> = assignments
            .iter()
            .map(|(col, expr)| {
                let idx = schema
                    .name_col
                    .iter()
                    .position(|c| c == col)
                    .ok_or_else(|| DbError::Page(format!("Column '{}' not found", col)))?;
                Ok((idx, expr.clone(), schema.columns[idx].clone()))
            })
            .collect::<Result<_, DbError>>()?;

        // 3) open file and compute number of pages
        let handle = storage
            .get_handle(&tr.name_file)
            .ok_or_else(|| DbError::Page("Data file not registered".to_string()))?;
        let file_len = handle.file_len()? as usize;
        let num_pages = (file_len + PAGE_SIZE - 1) / PAGE_SIZE;

        // 4) scan every page
        let mut updated = 0;
        let mut rows_to_update: Vec<(usize, usize, Row)> = Vec::new();

        for page_idx in 0..num_pages {
            // read raw bytes
            let mut buf = vec![0u8; PAGE_SIZE];
            if handle.read_at((page_idx * PAGE_SIZE) as u64, &mut buf).is_err() {
                continue;
            }

            // decode or skip if this isn’t a data page
            let table_page: TablePage = match bintuco::decode_from_slice::<TablePage>(&buf) {
                Ok((tp, _)) => tp,
                Err(_) => continue,
            };

            // for each row in the page...
            for (&rowid, &cell_idx) in &table_page.pointer_cell {
                let row = tr.read_cell_with_page(rowid, page_idx, storage)?;
                if predicate.map_or(true, |p| evaluate_predicate(&row, &schema, p).unwrap_or(false)) { //FIXME: unwrap
                    // build the updated Row
                    let mut new_row = row.clone();
                    for &(col_idx, ref expr, ref ty) in &assigns {
                        new_row.values[col_idx] = eval_expr_for_upd(&row, &schema, expr, ty)?;
                    }
                    rows_to_update.push((rowid, page_idx, new_row));
                }
            }
        }

        // Perform updates after iterating to avoid mutable borrow issues
        for (rowid, page_idx, new_row) in rows_to_update {
            tr.update_cell_with_page(rowid, page_idx, new_row, storage)?;
            updated += 1;
        }

        Ok(updated)
    }

    pub fn delete(
        &mut self,
        table_name: &str,
        predicate: Option<&Expression>,
        storage: &mut StorageManager,
    ) -> Result<usize, DbError> {
        // 1) find the table root
        let tr_idx = self
            .all_tableroot
            .iter()
            .position(|t| t.tb_schema.name == table_name)
            .ok_or_else(|| DbError::Page(format!("Table '{}' not found", table_name)))?;
        
        let schema = self.all_tableroot[tr_idx].tb_schema.clone(); // Clone schema

        let tr = &mut self.all_tableroot[tr_idx];

        // 2) open file and compute number of pages
        let handle = storage
            .get_handle(&tr.name_file)
            .ok_or_else(|| DbError::Page("Data file not registered".to_string()))?;
        let file_len = handle.file_len()? as usize;
        let num_pages = (file_len + PAGE_SIZE - 1) / PAGE_SIZE;

        // Collect rows to delete to avoid mutable borrow issues during iteration
        let mut rows_to_delete: Vec<(usize, usize)> = Vec::new();

        for page_idx in 0..num_pages {
            let mut buf = vec![0u8; PAGE_SIZE];
            if handle.read_at((page_idx * PAGE_SIZE) as u64, &mut buf).is_err() {
                continue;
            }

            let table_page: TablePage = match bintuco::decode_from_slice::<TablePage>(&buf) {
                Ok((tp, _)) => tp,
                Err(_) => continue,
            };

            for (&rowid, &cell_idx) in &table_page.pointer_cell {
                let row = tr.read_cell_with_page(rowid, page_idx, storage)?;
                if predicate
                    .map_or(true, |p| evaluate_predicate(&row, &schema, p).unwrap_or(false)) //FIXME: unwrap
                {
                    rows_to_delete.push((rowid, page_idx));
                }
            }
        }
        
        let mut deleted = 0;
        // Iterate and delete collected rows
        // Note: Deleting rows can modify page_vec (free space B-tree) and data pages.
        // This process might be inefficient for many deletions as it re-reads/writes
        // pages. For simplicity and correctness given the constraint, we perform
        // deletions sequentially.
        for (rowid, page_idx) in rows_to_delete {
            tr.delete_cell_with_page(rowid, page_idx, storage)?;
            deleted += 1;
        }

        Ok(deleted)
    }
}


// ══════════════════════════════════════ TABLE PAGE STRUCT ══════════════════════════════════════

/// Maybe you can doing better job, with offset and other attention
/// about the memory representation of this struct.
/// To make all simple, i force the cpu to make this align to 1024 bytes
#[repr(C, align(1024))]
#[derive(Debug, Clone)]
pub struct TablePage{
    pub index: usize,
    pub num_cell: u16,
    pub row_idnum: usize,
    pub pointer_cell: HashMap<usize, u16>, // Maps cell index to RowId
    pub veccell: Vec<Cell>,               // Actual stored cells
}
impl TablePage {
    pub fn append_cell(&mut self, cell: Cell, rowid: usize){
        let cell_index = self.veccell.len() as u16;
        self.veccell.push(cell);
        self.pointer_cell.insert(rowid, cell_index);
        self.num_cell +=1;
    }
    
    
}


// ══════════════════════════════════════ TABLE "ROOT" STRUCT ══════════════════════════════════════


/// Contain metadata about Table 
#[derive(Debug, Clone)]
pub struct TableRoot{
    pub tb_schema: TableSchema,
    pub name_root: String,
    pub name_file: String,
    pub page_num: usize,   // total number of pages
    pub row_id: usize,
    pub page_vec: Btree, 
    pub index_vec: Vec<Btree>, 

}

// Delegate function from TableSchema
impl TableRoot {

    // Delegates to self.tb_schema.get_columns()
    pub fn get_columns(&self) -> &Vec<SqlColumnType> {
        self.tb_schema.get_columns()
    }

    // Delegates to self.tb_schema.get_columns_name()
    pub fn get_columns_name(&self) -> &Vec<String> {
        self.tb_schema.get_columns_name()
    }

    // Delegates to self.tb_schema.set_row()
    pub fn set_row(&self, inputs: Vec<&str>) -> Result<Row, DbError> {
        self.tb_schema.set_row(inputs)
    }

    /*// Not used at the moment, see reordered_column() def
    // Delegates to self.tb_schema.reordered_columns()
    pub fn reordered_columns(&self) -> Vec<SqlColumnType> {
        self.tb_schema.reordered_columns()
    }
    */

}
impl TableRoot {
    /// Allocate (or reuse) an overflow page.  We look for
    /// a triplet with key free_space=OVERFLOW_KEY,
    /// and if found remove it and return that page number.
    /// Otherwise we bump `self.page_num`.
    pub fn allocate_overflow_page(&mut self, storage: & StorageManager) -> Result<usize, DbError> {
        let search_key = SqlValue::UINT(OVERFLOW_KEY);
        // Try to find an existing overflow page
        if let Ok(Some((_node, Some(triplet)))) =
            self.page_vec.search_value_at_least(search_key.clone(), storage)
        {
            if triplet.key == search_key {
                // Remove it so it won't be returned again
                let _ = self
                    .page_vec
                    .search_and_remove_value_at_least(search_key.clone(), storage)?;
                return Ok(triplet.page);
            }
        }
        // Otherwise allocate a brand-new page
        let new_idx = self.page_num;
        self.page_num += 1;
        Ok(new_idx)
    }

    pub fn convert_raw_to_row(&self, input_row: Vec<&str>) -> Result<Row, DbError> {
        if input_row.len() != self.tb_schema.columns.len() {
            return Err(DbError::Page(format!(
                "Column count mismatch. Expected {}, got {}",
                self.tb_schema.columns.len(),
                input_row.len()
            )));
        }
    
        let mut values = Vec::with_capacity(input_row.len());
    
        for (i, raw_val) in input_row.iter().enumerate() {
            let col_type = &self.tb_schema.columns[i];
            let sql_value = parse_sql_value(raw_val, col_type)?;
            values.push(sql_value);
        }
    
        Ok(Row { values })
    }
    

    pub fn convert_to_sql_value(&self, key_name: &str, key_value_str: &str) -> Result<SqlValue, DbError> {
        if let Some(pos) = self.tb_schema.name_col.iter().position(|name| name == key_name) {
            let col_type = &self.tb_schema.columns[pos];
            parse_sql_value(key_value_str, col_type)
        } else {
            Err(DbError::Page(format!("Column '{}' not found in schema", key_name)))
        }
    }

    
}


impl TableRoot {
    /// Insert a new row given raw string values into this table.
    /// Now takes a shared StorageManager so all I/O goes through it.
    pub fn insert(
        &mut self,
        input_row: Vec<&str>,
        storage: &StorageManager,
    ) -> Result<(), DbError> {
        // 1) Convert the raw strings into a Row via the schema
        let row = self.convert_raw_to_row(input_row)?;

        // 2) Delegate to the refactored insert_cell, passing the same StorageManager
        self.insert_cell(row, storage)?;

        Ok(())
    }
}



impl TableRoot {
    /// Create a new, empty TableRoot (with its own metadata B‐tree).
    ///
    /// - `name`: the table name (e.g. "users")
    /// - `columns`, `name_col`, `pk`: schema information
    /// - `base_dir`: the filesystem directory where all files live
    /// - `storage`: a StorageManager into which we will register any new files
    pub fn new(
        name: &str,
        columns: Vec<SqlColumnType>,
        name_col: Vec<&str>,
        pk: Option<&str>,
        base_dir: &Path,
        storage: &mut StorageManager,
    ) -> Result<Self, DbError> {
        // 1) Validate schema lengths
        if columns.len() != name_col.len() {
            return Err(DbError::Page("Input count mismatch".to_string()));
        }

        // 2) Build TableSchema
        let name_col_strings: Vec<String> = name_col.iter().map(|s| s.to_string()).collect();
        let primary_key = match pk {
            Some(key) => {
                if name_col_strings.contains(&key.to_string()) {
                    Some(key.to_string())
                } else {
                    return Err(DbError::Page(format!("Primary key '{}' not found", key)));
                }
            }
            None => None,
        };
        let tb_schema = TableSchema {
            name: name.to_string(),
            columns,
            name_col: name_col_strings.clone(),
            primary_key: primary_key.clone(),
        };

        // 3) Prepare the logical filenames:
        //    - metadata B‐tree: "<table>.meta.idx"
        //    - table data file:   "<table>.tbl"
        let meta_logical = format!("{}.meta.idx", name);
        let data_logical = format!("{}.tbl", name);

        // 4) Ensure both files exist on disk under `base_dir`, registering them:
        //    (a) metadata file:
        {
            // StorageManager::register_file will create the file if needed
            storage.register_file(&meta_logical)?;
        }
        //    (b) data file:
        {
            storage.register_file(&data_logical)?;
        }

        // 5) Build a new Btree for the free‐space index, keyed by "<table>.meta"
        let btree_key = format!("{}.meta", name);
        let page_vec =
            Btree::new(btree_key, base_dir, storage)?;
        
        // 7) Build the TableRoot struct
        let mut table_root = Self {
            tb_schema,
            name_root: name.to_string(),
            name_file: format!("{}.tbl", name), // used in insert/read
            page_num: 0,       // we’ve now allocated “page 0”
            row_id: 0,         // no rows inserted yet
            page_vec,
            index_vec: Vec::new(), // no secondary indexes yet
        };

        table_root.create_pk_btree(base_dir, storage)?;
        Ok(table_root)
    }

    /// Construct a TableRoot from an existing TableSchema.
    ///
    /// This is used when re‐loading an existing database: the files should already
    /// live on disk, and must have been registered with `storage` beforehand.
    pub fn from_schema(
        schema: TableSchema,
        base_dir: &Path,
        storage: &mut StorageManager,
    ) -> Result<Self, DbError> {
        let name = schema.name.clone();
        let data_logical = format!("{}.tbl", name);

        // 1) Ensure the data file is registered (already exists on disk)
        storage.register_file(&data_logical)?;

        // 2) B‐tree for primary key metadata: "<table>.meta.idx"
        let mut table_root = TableRoot {
            tb_schema: schema.clone(),
            name_root: name.clone(),
            name_file: format!("{}.tbl", name.clone()),
            page_num: 0,        // we will detect how many pages exist below
            row_id: 0,          // we can reconstruct row_id by scanning, but keep 0 for now
            page_vec: Btree::new(format!("{}.meta", name), base_dir, storage)?,
            index_vec: Vec::new(),
        };

        // 3) Call create_pk_btree to create or load any secondary indexes
        table_root.create_pk_btree(base_dir, storage)?;
        Ok(table_root)
    }

    /// Create a B‐tree for the primary key (if present in the schema).
    /// Registers and opens the index file via `storage`.
    pub fn create_pk_btree(
        &mut self,
        base_dir: &Path,
        storage: &mut StorageManager,
    ) -> Result<(), DbError> {
        // 1) If no primary key, nothing to do
        let pk_name = match &self.tb_schema.primary_key {
            Some(pk) => pk.clone(),
            None => return Ok(()),
        };

        // 2) Check that the primary key column exists in the schema
        let _key_pos = self
            .tb_schema
            .name_col
            .iter()
            .position(|c| c == &pk_name)
            .ok_or_else(|| DbError::Page(format!("Primary key '{}' not found", pk_name)))?;

        // 3) Register the index file: "<pk_name>.idx"
        let idx_logical = format!("{}.idx", pk_name);
        storage.register_file(&idx_logical)?;

        // 4) Create or re‐open that Btree:
        let btree_pk = Btree::new(pk_name.clone(), base_dir, storage)?;

        self.index_vec.push(btree_pk);
        Ok(())
    }
}


impl TableRoot {
    /// Finds the first page with ≥ `required_space` free bytes,
    /// or else allocates a brand-new page. Uses StorageManager.
    pub fn first_free_page(
        &mut self,
        required_space: usize,
        storage: &StorageManager,
    ) -> Result<(usize, bool), DbError> {
        // 1) Convert required_space → SqlValue (free space is stored as UINT)
        let search_key = SqlValue::UINT(required_space as u32);

        // 2) Ask the B-tree to find a triplet whose key ≥ search_key
        if let Some((_node, Some(triplet))) =
            self.page_vec.search_value_at_least(search_key.clone(), storage)?
        {
            // Found an existing page with enough free space
            return Ok((triplet.page, false));
        }

        // 3) No suitable page → allocate a new one
        let new_idx = self.page_num;
        self.page_num += 1;

        let new_triplet = Triplet {
            key: SqlValue::UINT(PAGE_CAPACITY as u32),
            rowid: 0,
            page: new_idx,
        };

        // Insert it into the B-tree (ignore errors)
        let _ = self.page_vec.insert(new_triplet, storage);

        Ok((new_idx, true))
    }
}


impl TableRoot {
    /// Refactored to use StorageManager instead of opening its own File.
    pub fn insert_cell(
        &mut self,
        row: Row,
        storage: &StorageManager,
    ) -> Result<(), DbError> {

        // 1) Determine which page to write into (or allocate a new one)
        let row_enc_len = row.to_bytes()?.len();
        let (idx_free, is_new) =
            self.first_free_page(std::cmp::min(row_enc_len + PADDING_ROW_BYTES , PAYLOAD_MIN as usize), storage)?;

        // 2) Grab the FileHandle for this table’s ".db" file.
        let logical_name = format!("{}", self.name_file);
        let handle = storage
            .get_handle(&logical_name)
            .ok_or_else(|| {
                DbError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Table data file not registered: {}", logical_name),
                ))
            })?;
        
        // 3) Load or initialize the TablePage struct at page index `idx_free`.
        let mut table_page: TablePage = if is_new {
            TablePage {
                index: idx_free,
                num_cell: 0,
                row_idnum: 0,
                pointer_cell: HashMap::new(),
                veccell: Vec::new(),
            }
        } else {
            // Read exactly PAGE_SIZE bytes from disk into a buffer...
            let mut buf = vec![0u8; PAGE_SIZE];
            handle.read_at((idx_free * PAGE_SIZE) as u64, &mut buf)?;
            // Decode a TablePage
            bintuco::decode_from_slice(&buf)
                .map_err(|e| DbError::Bintuco(e.to_string()))?.0
        };

        // 4) Insert either a normal cell or an overflow‐split cell
        if row_enc_len < OVERFLOW_SENTINEL {
            // No overflow page needed
            let cell = Cell::new(&row, 0)?;
            table_page.append_cell(cell, self.row_id);
            self.row_id += 1;
            
            // Re‐serialize and write back the updated TablePage
            let mut write_buf = vec![0u8; PAGE_SIZE];
            let encoded = bintuco::encode_to_vec(&table_page)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;
            write_buf[..encoded.len()].copy_from_slice(&encoded);

            // compute byte offset for this page
            let page_offset = (idx_free as u64) * (PAGE_SIZE as u64);

            // instead of handle.write_at(...), call write_page on StorageManager:
            storage.write_page(&self.name_file, page_offset, &write_buf)?;


            // Update the free‐space index in the B‐tree
            let free_count = PAGE_SIZE - encoded.len();
            let new_triplet = Triplet {
                key: SqlValue::UINT(free_count as u32),
                rowid: 0,
                page: idx_free,
            };
            if let Some((_node, Some(removed_triplet))) = self.page_vec.search_and_remove_value_at_least(
                SqlValue::UINT(std::cmp::min((row_enc_len + PADDING_ROW_BYTES) as u32, PAYLOAD_MIN)),
                storage,
            )? {
                 self.page_vec.insert(new_triplet, storage)?;
            } else {
                // If no page was removed, it means we allocated a new page,
                // so we still need to insert the free space info for the new page.
                self.page_vec.insert(new_triplet, storage)?;
            }
        } else {
            // Row is large → first fragment fits in main cell, rest goes to overflow pages
            let truncated = &row.to_bytes()?[..OVERFLOW_SENTINEL-2];
            let pointer_ovpage = self.allocate_overflow_page(storage)?;
            let cell = Cell::new_over(truncated, pointer_ovpage);
            table_page.append_cell(cell, self.row_id);
            self.row_id += 1;

            // Re‐serialize and write back the “main” TablePage
            let mut write_buf = vec![0u8; PAGE_SIZE];
            let encoded = bintuco::encode_to_vec(&table_page)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;
            write_buf[..encoded.len()].copy_from_slice(&encoded);

            // compute byte offset for this page
            let page_offset = (idx_free as u64) * (PAGE_SIZE as u64);

            // instead of handle.write_at(...), call write_page on StorageManager:
            storage.write_page(&self.name_file, page_offset, &write_buf)?;


            // Update free‐space index in B‐tree
            let free_count = PAGE_SIZE - encoded.len();
            let new_triplet = Triplet {
                key: SqlValue::UINT(free_count as u32),
                rowid: 0,
                page: idx_free,
            };
            if let Some((_node, Some(removed_triplet))) = self.page_vec.search_and_remove_value_at_least(
                SqlValue::UINT(PAYLOAD_MIN),
                storage,
            )? {
                self.page_vec.insert(new_triplet, storage)?;
            } else {
                self.page_vec.insert(new_triplet, storage)?;
            }

            // 5) Write out each overflow chunk in its own 1024‐byte page
            let full_bytes = row.to_bytes()?;
            let mut offset = OVERFLOW_SENTINEL-2;
            let mut current_ov = pointer_ovpage;
            while offset < full_bytes.len() {
                let end = usize::min(offset + PAYLOAD_SIZE, full_bytes.len());
                let chunk = &full_bytes[offset..end];
                offset += chunk.len();

                // Determine next overflow pointer (0 if this is the last chunk)
                let next_ov = if offset < full_bytes.len() {
                    self.allocate_overflow_page(storage)?
                } else {
                    0
                };

                // Pad chunk to PAYLOAD_SIZE, then bintuco‐serialize
                let mut payload_buf = vec![0u8; PAYLOAD_SIZE];
                payload_buf[..chunk.len()].copy_from_slice(chunk);
                let serialized = bintuco::encode_to_vec(&payload_buf)
                    .map_err(|e| DbError::Bintuco(e.to_string()))?;

                // Build an OverflowPage struct
                let ov_page = OverflowPage {
                    page_num: current_ov,
                    overflow_page: next_ov,
                    row_payload: serialized,
                };
                
                // Write this overflow page out at (current_ov * PAGE_SIZE) offset
                let mut ov_buf = vec![0u8; PAGE_SIZE];
                let enc = bintuco::encode_to_vec(&ov_page)
                    .map_err(|e| DbError::Bintuco(e.to_string()))?;
                ov_buf[..enc.len()].copy_from_slice(&enc);

                // compute byte offset for this overflow page
                let page_offset = (current_ov as u64) * (PAGE_SIZE as u64);

                // instead of handle.write_at(...), call write_page on StorageManager:
                storage.write_page(&self.name_file, page_offset, &ov_buf)?;


                current_ov = next_ov;
            }
        }

        // ################## BTREE ROUTINE ################
        // Now insert the new row’s key into any secondary B‐trees (indexes)
        for btree in &mut self.index_vec {
            if let Some(key_pos) =
                self.tb_schema.name_col.iter().position(|n| n == &btree.key_name)
            {
                let key_val = row
                    .values
                    .get(key_pos)
                    .ok_or_else(|| DbError::Page(format!("Key position missing in row")))?;
                let triplet = Triplet {
                    key: key_val.clone(),
                    rowid: self.row_id - 1,
                    page: idx_free,
                };
                btree.insert(triplet, storage)?;
            }
        }

        Ok(())
    }
}

impl TableRoot {
    /// Read the row with `rowid`, scanning every page via the shared StorageManager.
    pub fn read_exact_cell(
        &self,
        rowid: usize,
        storage: &StorageManager,
    ) -> Result<Option<Row>, DbError> {

        // 1) Get the FileHandle for this table’s file
        let logical_name = self.name_file.clone();
        let handle = storage.get_handle(&logical_name)
            .ok_or_else(|| DbError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "File not registered".to_string())))?;

        // 2) Iterate through every B-tree page index
        let pages_iter = self.page_vec.all_pages(storage)?;

        for page_res in pages_iter {
            let page_idx = page_res?;

            // 3) Read exactly 1024 bytes at offset = page_idx * PAGE_SIZE
            let mut buf = vec![0u8; PAGE_SIZE];
            handle.read_at((page_idx * PAGE_SIZE) as u64, &mut buf)?;

            // 4) Decode into a TablePage
            let table_page: TablePage = bintuco::decode_from_slice(&buf)
                .map_err(|e| DbError::Bintuco(e.to_string()))?.0;

            // 5) If this page maps our rowid, reconstruct the Row
            if let Some(&cell_idx) = table_page.pointer_cell.get(&rowid) {
                let cell = &table_page.veccell[cell_idx as usize];

                // No overflow → decode directly
                if cell.overflow_page == 0 {
                    return Ok(Some(cell.decode_cell()?));
                }

                // Otherwise, gather payload from this cell plus overflow chain
                let mut full_payload = cell.row_payload.clone();
                let mut next_ov = cell.overflow_page;

                while next_ov != 0 {
                    // Read one overflow page
                    let mut ov_buf = vec![0u8; PAGE_SIZE];
                    handle.read_at((next_ov * PAGE_SIZE) as u64, &mut ov_buf)?;

                    // Decode the OverflowPage struct
                    let ov_page: OverflowPage = bintuco::decode_from_slice(&ov_buf)
                        .map_err(|e| DbError::Bintuco(e.to_string()))?.0;

                    // Decode its row_payload (a Vec<u8> chunk)
                    let (chunk_bytes, _) = bintuco::decode_from_slice::<Vec<u8>>(&ov_page.row_payload)
                        .map_err(|e| DbError::Bintuco(e.to_string()))?;
                    full_payload.extend(chunk_bytes);
                    next_ov = ov_page.overflow_page;
                }

                return Ok(Some(Row::from_bytes(&full_payload)?));
            }
        }

        Ok(None)
    }
}


impl TableRoot {
    /// Read a row by its `rowid`, knowing it resides on `page_idx`.
    /// Uses StorageManager instead of opening its own File.
    pub fn read_cell_with_page(
        &self,
        rowid: usize,
        page_idx: usize,
        storage: &StorageManager,
    ) -> Result<Row, DbError> {

        // 1) Acquire the FileHandle for this table’s “.db” file
        let logical_name = self.name_file.clone();
        let handle = storage.get_handle(&logical_name)
            .ok_or_else(|| DbError::Page("Table data file not registered".to_string()))?;

        // 2) Read exactly one TablePage at (page_idx * PAGE_SIZE)
        let mut buf = vec![0u8; PAGE_SIZE];
        handle.read_at((page_idx * PAGE_SIZE) as u64, &mut buf)?;
        let table_page: TablePage = bintuco::decode_from_slice(&buf)
            .map_err(|e| DbError::Bintuco(e.to_string()))?.0;

        // 3) Find the cell index for our rowid
        if let Some(&cell_index) = table_page.pointer_cell.get(&rowid) {
            let cell = &table_page.veccell[cell_index as usize];

            // 4) If no overflow, decode immediately
            if cell.overflow_page == 0 {
                return cell.decode_cell();
            }

            // 5) Otherwise, gather full payload by following overflow chain
            let mut full_payload = cell.row_payload.clone();
            let mut next_ov = cell.overflow_page;

            while next_ov != 0 {
                // Read one overflow page at (next_ov * PAGE_SIZE)
                let mut ov_buf = vec![0u8; PAGE_SIZE];
                handle.read_at((next_ov * PAGE_SIZE) as u64, &mut ov_buf)?;

                // Decode that OverflowPage
                let ov_page: OverflowPage = bintuco::decode_from_slice(&ov_buf)
                    .map_err(|e| DbError::Bintuco(e.to_string()))?.0;

                // Decode its row_payload chunk and append
                let (chunk_bytes, _) = bintuco::decode_from_slice::<Vec<u8>>(&ov_page.row_payload)
                    .map_err(|e| DbError::Bintuco(e.to_string()))?;
                full_payload.extend(chunk_bytes);
                next_ov = ov_page.overflow_page;
            }

            return Row::from_bytes(&full_payload);
        }

        Err(DbError::Page(format!("Row ID {} not found in page {}", rowid, page_idx)))
    }
}


impl TableRoot {
    /// Search for a row by primary (or indexed) key. Uses StorageManager.
    pub fn tab_search_by_key(
        &self,
        key_name: &str,
        key_value: SqlValue,
        storage: &StorageManager,
    ) -> Result<Option<Triplet>, DbError> {
        // 1) Find the B-tree whose key_name matches
        for btree in &self.index_vec {
            if btree.key_name == key_name {
                // 2) Call the refactored Btree::search_value, passing storage
                return btree.search_value(key_value, storage)
                    .map(|opt_tuple| opt_tuple.and_then(|(_node, triplet_opt)| triplet_opt));
            }
        }

        Err(DbError::BTree(format!("No index found for key '{}'", key_name)))
    }
}


impl TableRoot {
    /// A “scan‐the‐entire‐.tbl‐file” iterator that yields every Row in this table,
    /// correctly handling overflow pages.
    pub fn row_iterator<'a>(
        &'a self,
        storage: &'a StorageManager,
        schema: TableSchema,
    ) -> Box<dyn RowIterator + 'a> {
        // Grab the logical filename (e.g. "mytable.tbl"):
        let table_file = self.name_file.clone();
        // Ask the StorageManager how big the file is, so we know how many pages to read:
        let handle = storage
            .get_handle(&table_file)
            .expect(&format!("File handle for {} not registered", table_file));
        let file_len = handle
            .file_len()
            .expect("Unable to get file length");
        // Compute how many full pages live in that file:
        let num_pages = ((file_len as usize) + PAGE_SIZE - 1) / PAGE_SIZE;

        //  For each page index 0..num_pages, read one 1024‐byte chunk, decode it,
        //  and emit all rows found on that page. If `read_at` or decoding fails,
        //  we skip that page entirely.
        //  Decode can fail because page is an overflowPage, or is corrupted.
        //  Maybe we can update the logic for check data integrity.
        //  If we want panic here, after database reboot
        //  an automatic recovery process start

        let raw_iter = (0..num_pages).flat_map(move |page_idx| {
            // -- Read raw bytes from disk:
            let mut buf = vec![0u8; PAGE_SIZE];
            let offset = (page_idx * PAGE_SIZE) as u64;
            if handle.read_at(offset, &mut buf).is_err() {
                // Could not read this page; return an empty iterator:
                return Vec::new().into_iter();
            }

            // -- Decode those bytes as a TablePage:
            let table_page: TablePage = match bintuco::decode_from_slice::<TablePage>(&buf) {
                Ok((tp, _)) => tp,
                Err(_) => {
                    // If decode fails, skip this page entirely:
                    // Again, maybe is an overflowPage, or is corrupted.
                    // We have to choice but if corrupted we can panic here
                    return Vec::new().into_iter();
                }
            };

            // -- For each cell in veccell, reconstruct the full payload (following overflow)
            //    and decode into a Row:
            let mut rows_on_page = Vec::with_capacity(table_page.veccell.len());
            for cell in &table_page.veccell {
                // Start with the payload stored in the main cell:
                let mut full_payload = cell.row_payload.clone();
                let mut next_ov = cell.overflow_page;

                // Follow overflow chain until next_ov == 0:
                while next_ov != 0 {
                    // Read the overflow page at (next_ov * PAGE_SIZE)
                    let mut ov_buf = vec![0u8; PAGE_SIZE];
                    if handle.read_at((next_ov * PAGE_SIZE) as u64, &mut ov_buf).is_err() {
                        break; // if we can't read, stop following chain
                    }

                    // Decode the OverflowPage struct
                    let ov_page: OverflowPage = match bintuco::decode_from_slice(&ov_buf) {
                        Ok((op, _)) => op,
                        Err(_) => break,
                    };

                    // Decode its `row_payload` chunk (which is itself a bintuco‐encoded Vec<u8>)
                    if let Ok((chunk_bytes, _)) =
                        bintuco::decode_from_slice::<Vec<u8>>(&ov_page.row_payload)
                    {
                        full_payload.extend(chunk_bytes);
                    } else {
                        break;
                    }

                    next_ov = ov_page.overflow_page;
                }

                // Finally, decode the assembled payload into a Row
                if let Ok(row) = Row::from_bytes(&full_payload) {
                    rows_on_page.push(row);
                }
            }

            rows_on_page.into_iter()
        });

        Box::new(SchemaRowIter {
            schema,
            inner: raw_iter,
        })
    }
}



impl TableRoot {
    /// Delete a specific row in‐place: free any overflow chain,
    /// remove the cell from its page, update free-space and secondary indexes.
    pub fn delete_cell_with_page(
        &mut self,
        rowid: usize,
        page_idx: usize,
        storage: &StorageManager,
    ) -> Result<(), DbError> {
        // 0) grab the filename early
        let logical = self.name_file.clone();

        // 1) Load & decode the TablePage
        let handle = storage
            .get_handle(&logical)
            .ok_or_else(|| DbError::Page(format!("Data file not registered: {}", logical)))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        handle.read_at((page_idx * PAGE_SIZE) as u64, &mut buf)?;
        let (mut page, _) = bintuco::decode_from_slice::<TablePage>(&buf)
            .map_err(|e| DbError::Bintuco(e.to_string()))?;

        // 2) Locate the cell and reconstruct the full old row (for index removal)
        let &cell_idx = page
            .pointer_cell
            .get(&rowid)
            .ok_or_else(|| DbError::Page(format!("Row {} not found on page {}", rowid, page_idx)))?;
        let old_cell = page.veccell[cell_idx as usize].clone();

        let old_row: Row = {
            let mut bytes = old_cell.row_payload.clone();
            let mut nxt = old_cell.overflow_page;
            while nxt != 0 {
                let ovp = read_overflow_page(storage, &logical, nxt)?;
                let (chunk, _) = bintuco::decode_from_slice::<Vec<u8>>(&ovp.row_payload)?;
                bytes.extend(chunk);
                nxt = ovp.overflow_page;
            }
            Row::from_bytes(&bytes)?
        };

        // 3) Free the old overflow chain pages
        {
            let mut nxt = old_cell.overflow_page;
            while nxt != 0 {
                // read pointer to next
                let ovp = read_overflow_page(storage, &logical, nxt)
                    .unwrap_or(OverflowPage { // This unwrap is problematic if it can fail for a recoverable error
                        page_num: nxt,
                        overflow_page: 0,
                        row_payload: Vec::new(),
                    });
                let this_pg = nxt;
                nxt = ovp.overflow_page;

                // zero it out as an empty TablePage
                let empty = TablePage {
                    index: this_pg,
                    num_cell: 0,
                    row_idnum: 0,
                    pointer_cell: HashMap::new(),
                    veccell: Vec::new(),
                };
                store_table_page(storage, &logical, &empty)?;

                // re-insert as a free OVERFLOW_KEY slot
                let free_trip = Triplet {
                    key: SqlValue::UINT(OVERFLOW_KEY),
                    rowid: 0,
                    page: this_pg,
                };
                let _ = self.page_vec.insert(free_trip, storage);
            }
        }

        // 4) Remove old secondary‐index entries
        for idx in &mut self.index_vec {
            if let Some(pos) = self.tb_schema.name_col.iter().position(|c| c == &idx.key_name) {
                let _ = idx.search_and_remove_value_at_least(old_row.values[pos].clone(), storage);
            }
        }

        // 5) Delete the cell from the TablePage
        page.pointer_cell.remove(&rowid);
        page.veccell.remove(cell_idx as usize);
        page.num_cell -= 1;
        // shift any later pointers down by one
        for (_rid, ptr) in page.pointer_cell.iter_mut() {
            if *ptr > cell_idx {
                *ptr -= 1;
            }
        }

        // 6) Write back the updated TablePage
        store_table_page(storage, &logical, &page)?;

        // 7) Update free-space B-tree for the modified data page
        let enc = bintuco::encode_to_vec(&page)?;
        let free_now = PAGE_SIZE - enc.len();
        // remove the old entry
        let old_req = SqlValue::UINT(std::cmp::min((enc.len() + PADDING_ROW_BYTES) as u32, PAYLOAD_MIN));
        if let Some((_node, Some(_))) =
            self.page_vec.search_and_remove_value_at_least(old_req.clone(), storage)?
        {
            let trip = Triplet {
                key: SqlValue::UINT(free_now as u32),
                rowid: 0,
                page: page_idx,
            };
            let _ = self.page_vec.insert(trip, storage);
        }

        Ok(())
    }
}


impl TableRoot {
    /// Update a row in-place: completely free any old overflow chain,
    /// then either pack the new_row on-page or build a fresh overflow chain.
    pub fn update_cell_with_page(
        &mut self,
        rowid: usize,
        page_idx: usize,
        new_row: Row,
        storage: &StorageManager,
    ) -> Result<(), DbError> {
        // clone filename early so we drop any &self borrow
        let logical = self.name_file.clone();

        // Read & decode the TablePage
        let handle = storage
            .get_handle(&logical)
            .ok_or_else(|| DbError::Page(format!("Data file not registered: {}", logical)))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        handle.read_at((page_idx * PAGE_SIZE) as u64, &mut buf)?;
        let (mut page, _) = bintuco::decode_from_slice::<TablePage>(&buf)
            .map_err(|e| DbError::Bintuco(e.to_string()))?;

        // Locate cell index
        let &cell_idx = page
            .pointer_cell
            .get(&rowid)
            .ok_or_else(|| DbError::Page(format!("Row {} not found on page {}", rowid, page_idx)))?;
        let old_cell = page.veccell[cell_idx as usize].clone();

        // Fully reconstruct the old Row (main + overflow) 
        let old_row: Row = {
            let mut bytes = old_cell.row_payload.clone();
            let mut nxt = old_cell.overflow_page;
            while nxt != 0 {
                let ovp = read_overflow_page(storage, &logical, nxt)?;
                // decode the Vec<u8> chunk from bintuco
                let (chunk, _) = bintuco::decode_from_slice::<Vec<u8>>(&ovp.row_payload)?;
                bytes.extend(chunk);
                nxt = ovp.overflow_page;
            }
            Row::from_bytes(&bytes)?
        };

        // Free the old overflow‐chain pages (mark them with OVERFLOW_KEY)
        {
            let mut nxt = old_cell.overflow_page;
            while nxt != 0 {
                let ovp = read_overflow_page(storage, &logical, nxt)
                    .unwrap_or(OverflowPage { // This unwrap needs to be handled
                        page_num: nxt,
                        overflow_page: 0,
                        row_payload: Vec::new(),
                    });
                let this_pg = nxt;
                nxt = ovp.overflow_page;

                // zero it out
                let empty = TablePage {
                    index: this_pg,
                    num_cell: 0,
                    row_idnum: 0,
                    pointer_cell: HashMap::new(),
                    veccell: Vec::new(),
                };
                store_table_page(storage, &logical, &empty)?;

                // re‐insert as a “free overflow slot”
                let free_trip = Triplet {
                    key: SqlValue::UINT(OVERFLOW_KEY),
                    rowid: 0,
                    page: this_pg,
                };
                let _ = self.page_vec.insert(free_trip, storage);
            }
        }

        // Remove old secondary‐index entries for this row
        for idx in &mut self.index_vec {
            if let Some(pos) = self.tb_schema.name_col.iter().position(|c| c == &idx.key_name)
            {
                let _ = idx.search_and_remove_value_at_least(old_row.values[pos].clone(), storage);
            }
        }

        // Encode & write the new row
        let new_bytes = new_row.to_bytes()?;
        const MAIN_CAP: usize = OVERFLOW_SENTINEL - 2;

        if new_bytes.len() <= MAIN_CAP {
            // no overflow chain needed
            page.veccell[cell_idx as usize] = Cell::new(&new_row, 0)?;
        } else {
            // build a fresh overflow chain
            let main = &new_bytes[..MAIN_CAP];
            let mut offset = MAIN_CAP;
            let mut cur_ov = self.allocate_overflow_page(storage)?;
            page.veccell[cell_idx as usize] = Cell::new_over(main, cur_ov);

            while offset < new_bytes.len() {
                let end = usize::min(offset + PAYLOAD_SIZE, new_bytes.len());
                let chunk = &new_bytes[offset..end];
                offset = end;

                let next_ov = if offset < new_bytes.len() {
                    self.allocate_overflow_page(storage)?
                } else {
                    0
                };

                let (payload) =
                    bintuco::encode_to_vec(&chunk.to_vec())?;  // serialize chunk;
                let ovp = OverflowPage {
                    page_num: cur_ov,
                    overflow_page: next_ov,
                    row_payload: payload,
                };
                store_overflow_page(storage, &logical, &ovp)?;
                cur_ov = next_ov;
            }
        }

        // Write back the updated TablePage itself
        store_table_page(storage, &logical, &page)?;

        // Update free-space B-tree for this data page
        let enc = bintuco::encode_to_vec(&page)?;
        let free_now = PAGE_SIZE - enc.len();
        let trip = Triplet {
            key: SqlValue::UINT(free_now as u32),
            rowid: 0,
            page: page_idx,
        };
        let _ = self.page_vec.insert(trip, storage);

        // Insert new secondary-index entries
        for idx in &mut self.index_vec {
            if let Some(pos) = self.tb_schema.name_col.iter().position(|c| c == &idx.key_name)
            {
                let trip = Triplet {
                    key: new_row.values[pos].clone(),
                    rowid,
                    page: page_idx,
                };
                let _ = idx.insert(trip, storage);
            }
        }

        Ok(())
    }
}


// ══════════════════════════════════════ GENERAL UTILITY FUNCTION ══════════════════════════════════════


pub fn parse_sql_value(raw: &str, col_type: &SqlColumnType) -> Result<SqlValue, DbError> {
    match col_type {
        SqlColumnType::TINYINT => raw.parse::<i8>().map(SqlValue::TINYINT)
            .map_err(|_| DbError::Page(format!("Invalid TINYINT: '{}'", raw))),
        SqlColumnType::SMALLINT => raw.parse::<i16>().map(SqlValue::SMALLINT)
            .map_err(|_| DbError::Page(format!("Invalid SMALLINT: '{}'", raw))),
        SqlColumnType::INT => raw.parse::<i32>().map(SqlValue::INT)
            .map_err(|_| DbError::Page(format!("Invalid INT: '{}'", raw))),
        SqlColumnType::BIGINT => raw.parse::<i64>().map(SqlValue::BIGINT)
            .map_err(|_| DbError::Page(format!("Invalid BIGINT: '{}'", raw))),
        SqlColumnType::UBIGINT => raw.parse::<u64>().map(SqlValue::UBIGINT)
            .map_err(|_| DbError::Page(format!("Invalid UBIGINT: '{}'", raw))),
        SqlColumnType::UTINYINT => raw.parse::<u8>().map(SqlValue::UTINYINT)
            .map_err(|_| DbError::Page(format!("Invalid UTINYINT: '{}'", raw))),
        SqlColumnType::USMALLINT => raw.parse::<u16>().map(SqlValue::USMALLINT)
            .map_err(|_| DbError::Page(format!("Invalid USMALLINT: '{}'", raw))),
        SqlColumnType::UINT => raw.parse::<u32>().map(SqlValue::UINT)
            .map_err(|_| DbError::Page(format!("Invalid UINT: '{}'", raw))),
        SqlColumnType::FLOAT => raw.parse::<f32>().map(SqlValue::FLOAT)
            .map_err(|_| DbError::Page(format!("Invalid FLOAT: '{}'", raw))),
        SqlColumnType::DOUBLE => raw.parse::<f64>().map(SqlValue::DOUBLE)
            .map_err(|_| DbError::Page(format!("Invalid DOUBLE: '{}'", raw))),
        SqlColumnType::CHAR => {
            let bytes = raw.as_bytes().to_vec();
            if bytes.len() > 255 {
                Err(DbError::Page(format!("Input too long for CHAR (max 255): '{}'", raw)))
            } else {
                Ok(SqlValue::CHAR(bytes))
            }
        }
        SqlColumnType::VARCHAR => {
            let bytes = raw.as_bytes().to_vec();
            if bytes.len() > 8000 {
                Err(DbError::Page(format!("Input too long for VARCHAR (max 8000): '{}'", raw)))
            } else {
                Ok(SqlValue::VARCHAR(bytes))
            }
        }
    }
}

/// Read one TablePage at `page_index` from the given logical file,
/// using StorageManager instead of opening a File yourself.
pub fn read_table_page(
    storage: &StorageManager,
    logical_name: &str,   // e.g., "users.db"
    page_index: usize,
) -> Result<TablePage, DbError> {
    
    // Get the shared handle
    let handle = storage.get_handle(logical_name).ok_or_else(|| {
        DbError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not registered: {}", logical_name),
        ))
    })?;

    // Read exactly PAGE_SIZE bytes at the correct offset
    let mut buf = vec![0u8; PAGE_SIZE];
    handle.read_at((page_index * PAGE_SIZE) as u64, &mut buf)?;

    // Decode into a TablePage
    let (page, _) = bintuco::decode_from_slice(&buf)
        .map_err(|e| DbError::Bintuco(e.to_string()))?;
    Ok(page)
}

/// Write one TablePage back to disk at its `page.index`,
/// using StorageManager + logical filename.
/// Returns how many zero‐padding bytes were written (free count).
pub fn store_table_page(
    storage: &StorageManager,
    logical_name: &str,   // e.g., "users.db"
    page: &TablePage,
) -> Result<usize, DbError> {
    // 1) Get the shared handle
    let handle = storage.get_handle(logical_name).ok_or_else(|| {
        DbError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not registered: {}", logical_name),
        ))
    })?;

    // 2) Serialize the TablePage
    let encoded = bintuco::encode_to_vec(page)
        .map_err(|e| DbError::Bintuco(e.to_string()))?;
    if encoded.len() > PAGE_SIZE {
        return Err(DbError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Serialized TablePage exceeds page size: {} > {}",
                encoded.len(),
                PAGE_SIZE
            ),
        )));
    }

    // 3) Zero-pad to exactly PAGE_SIZE and write at (page.index * PAGE_SIZE)
    let mut buf = vec![0u8; PAGE_SIZE];
    buf[..encoded.len()].copy_from_slice(&encoded);

    // compute the page’s byte offset
    let page_offset = (page.index as u64) * (PAGE_SIZE as u64);

    // now call write_page on StorageManager (instead of handle.write_at)
    storage.write_page(&logical_name, page_offset, &buf)?;

    // 4) Count zero‐padding bytes
    let zero_count = buf[encoded.len()..].iter().filter(|&&b| b == 0).count();
    Ok(zero_count)
}

/// Read one OverflowPage from disk at `page_index` via StorageManager.
pub fn read_overflow_page(
    storage: &StorageManager,
    logical_name: &str,  // e.g., "users.db" (same file as rows)
    page_index: usize,
) -> Result<OverflowPage, DbError> {
    // 1) Get the shared handle
    let handle = storage.get_handle(logical_name).ok_or_else(|| {
        DbError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not registered: {}", logical_name),
        ))
    })?;

    // 2) Read exactly PAGE_SIZE bytes at offset
    let mut buf = vec![0u8; PAGE_SIZE];
    handle.read_at((page_index * PAGE_SIZE) as u64, &mut buf)?;

    // 3) Decode into an OverflowPage
    let (page, _) = bintuco::decode_from_slice(&buf)
        .map_err(|e| DbError::Bintuco(e.to_string()))?;
    Ok(page)
}

/// Write one OverflowPage at `page.page_num`, using StorageManager.
pub fn store_overflow_page(
    storage: &StorageManager,
    logical_name: &str,  // e.g., "users.db"
    page: &OverflowPage,
) -> Result<(), DbError> {
    // 1) Get the shared handle
    let handle = storage.get_handle(logical_name).ok_or_else(|| {
        DbError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not registered: {}", logical_name),
        ))
    })?;

    // 2) Serialize OverflowPage
    let encoded = bintuco::encode_to_vec(page)
        .map_err(|e| DbError::Bintuco(e.to_string()))?;
    if encoded.len() > PAGE_SIZE {
        return Err(DbError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Serialized OverflowPage exceeds page size: {} > {}",
                encoded.len(),
                PAGE_SIZE
            ),
        )));
    }

    // 3) Zero‐pad and write at (page.page_num * PAGE_SIZE)
    let mut buf = vec![0u8; PAGE_SIZE];
    buf[..encoded.len()].copy_from_slice(&encoded);

    // compute byte offset for this page
    let page_offset = (page.page_num as u64) * (PAGE_SIZE as u64);

    // instead of handle.write_at(...), call write_page on your StorageManager:
    storage.write_page(&logical_name, page_offset, &buf)?;

    Ok(())
}



pub fn eval_expr_for_upd( // ATTENTION Different from dbengine.rs 
        row: &Row,
        schema: &TableSchema,
        expr: &Expression,
        target_type: &SqlColumnType,
    ) -> Result<SqlValue, DbError> {
        match expr {
            Expression::Column(c) => {
                let pos = schema.name_col.iter().position(|n| n == &c.column)
                    .ok_or_else(|| DbError::Planner(format!("Column '{}' not found in schema for update expression", c.column)))?;
                Ok(row.values[pos].clone())
            }
            Expression::Literal(v) => Ok(parse_literal_as_column_type(v, target_type)?),
            Expression::ArithmeticOp { left, op, right } => {
                let l = eval_expr_for_upd(row, schema, left, target_type)?;
                let r = eval_expr_for_upd(row, schema, right, target_type)?;
                let xf = to_f64_or_nan(&l);
                let yf = to_f64_or_nan(&r);
                let rf = match op {
                    ArithmeticOp::Add => xf + yf,
                    ArithmeticOp::Subtract => xf - yf,
                    ArithmeticOp::Multiply => xf * yf,
                    ArithmeticOp::Divide => {
                        if yf == 0.0 {
                            return Err(DbError::Planner("Division by zero in UPDATE expression".to_string()));
                        }
                        xf / yf
                    },
                };
                Ok(SqlValue::DOUBLE(rf))
            }
            Expression::UnaryOp { op, expr } => {
                let v = eval_expr_for_upd(row, schema, expr, target_type)?;
                let vf = to_f64_or_nan(&v);
                let rf = if *op == UnaryOp::Minus { -vf } else { vf };
                Ok(SqlValue::DOUBLE(rf))
            }
            _ => Err(DbError::Planner(format!("Unsupported UPDATE expression: {:?}", expr))),
        }
}



// ══════════════════════════════════════ TEST ══════════════════════════════════════


#[cfg(test)]
mod bin_test {
    use super::*;
    #[test]
    fn test_row_roundtrip() {
        let row = Row {
            values: vec![
                SqlValue::TINYINT(-10),
                SqlValue::SMALLINT(-1000),
                SqlValue::INT(-1_000_000),
                SqlValue::UTINYINT(250),
                SqlValue::USMALLINT(65500),
                SqlValue::UINT(123_456_789),
                SqlValue::FLOAT(3.1415),
                SqlValue::DOUBLE(-2.718281828),
                SqlValue::CHAR(b"abc".to_vec()),
                SqlValue::VARCHAR(b"hello".to_vec()),
            ],
        };

        // Serialize
        let bytes = row.to_bytes().unwrap();
        println!("{:?}", bytes);
        // Deserialize
        let decoded = Row::from_bytes(&bytes).unwrap();

        // Check equality
        assert_eq!(row.values, decoded.values);

        // Optionally, print both for debugging
        println!("Original:");
        row.pretty_print();
        println!("Decoded:");
        decoded.pretty_print();
    }
}
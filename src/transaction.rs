//!
//! Implements a TCP server handling a custom binary protocol.
//!
//! == Binary Protocol Definition ==
//!
//! All packets share the same structure:
//! [OpCode (1 byte)] [Payload Length (4 bytes, u32-be)] [Payload (variable bytes)]
//!
//! === Client-to-Server OpCodes ===
//! | OpCode | Command   | Payload Description                    |
//! |--------|-----------|----------------------------------------|
//! | 0x01   | Begin     | (empty)                                |
//! | 0x02   | Commit    | (empty)                                |
//! | 0x03   | Rollback  | (empty)                                |
//! | 0x04   | Query     | encoded SQL query string               |
//! | 0x05   | Info      | (empty)                                |
//! | 0x06   | Debug     | (empty)                                |
//! | 0x07   | Load      | (empty)                                |
//! | 0x08   | Export    | (empty)                                |
//! 
//! === Server-to-Client OpCodes ===
//! | OpCode | Response    | Payload Description                                    |
//! |--------|-------------|--------------------------------------------------------|
//! | 0x80   | Ok          | Optional success message.                              |
//! | 0x81   | Error       | error description.                                     |
//! | 0x82   | ResultSet   | Custom binary format (see `send_result_set` docs).     |
//!
//! TransactionEngine Module
//!


use crate::dbengine::{DatabaseEngine, RowIterator};
use crate::dberror::DbError;
use crate::page::{Row, SqlColumnType, TableSchema};
use crate::planner::{PlanNode, QueryPlanner};
use crate::tokenizer::{Lexer, Parser};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::thread;

// --- Binary Protocol OpCodes ---
const OP_BEGIN: u8 = 0x01;
const OP_COMMIT: u8 = 0x02;
const OP_ROLLBACK: u8 = 0x03;
const OP_QUERY: u8 = 0x04;
const OP_INFO: u8 = 0x05;
const OP_DEBUG: u8 = 0x06;
const OP_LOAD: u8 = 0x07;
const OP_EXPORT: u8 = 0x08;

// Server -> Client
const RESP_OK: u8 = 0x80;
const RESP_ERR: u8 = 0x81;
const RESP_RESULT_SET: u8 = 0x82;


/// /// ### Client/Server Network Protocol (Visualized)
/// 
/// ///
/// /// /// The protocol, defined in `transaction.rs`, uses length-prefixed framing to
/// /// /// send messages over TCP. A result set is sent as a single, large message.
/// ///
/// /// /// **1. Overall Message Frame:**
/// /// /// +----------------------------------------------------------------------+
/// /// /// | [ 0x82 ]  [ 00 00 00 2D ]    [ ... 45 bytes of payload ... ]         |
/// /// /// +----------+-----------------+-----------------------------------------+
/// /// /// | OpCode   | Payload Length  | Payload Data                            |
/// /// /// | (1 byte) | (u32, big-e)    | (schema info + rows)                    |
/// /// /// +----------------------------------------------------------------------+
/// ///
/// /// /// **2. Payload for one row `("Alice", 30.0)` from an example query:**
/// /// ///    (Assuming this is the first and only row in this message)
/// /// /// +--------------------------------------------------------------------------------------------------+
/// /// /// | [ 02 00 00 00 ] [ 04 00 00 00 6E 61 6D 65 0C ] [ 0B 00 00 00 64 6F ... 65 0A ] [ 01 00 00 00 ]     |
/// /// /// +---------------+----------------------------------+------------------------------+-----------------+
/// /// /// | Column Count  | Col 1: "name" (VARCHAR)          | Col 2: "doubled_age" (DOUBLE)| Row Count       |
/// /// /// | = 2 (u32)     | (len, bytes, type_tag)           | (len, bytes, type_tag)       | = 1 (u32)       |
/// /// /// +--------------------------------------------------------------------------------------------------+
/// /// /// |                                                                                                  |
/// /// /// | **Row 1 Data (bintuco-encoded):** |
/// /// /// | [ 1A 00 00 00 ] [ 02 00 00 00 09 05 00 00 00 41 6C 69 63 65 07 00 00 00 00 00 00 3E 40 ]           |
/// /// /// +---------------+----------------------------------------------------------------------------------+
/// /// /// | Row Byte Len  | bintuco encoded `Row { values: [VARCHAR("Alice"), DOUBLE(30.0)] }`                |
/// /// /// | = 26 (u32)    |                                                                                  |
/// /// /// +--------------------------------------------------------------------------------------------------+
/// ///
/// ///
/// ///



enum CommandResponse {
    Ok(String),
    ResultSet(TableSchema, Vec<Row>),
}

pub mod TransactionEngine {
    use super::*;

    pub fn start(db_path: &str, port: u16, address: &str) {
        let db_engine = match DatabaseEngine::open(db_path) {
            Ok(engine) => engine,
            Err(e) => {
                eprintln!("FATAL: Failed to open database: {}", e);
                return;
            }
        };
        let db = Arc::new(RwLock::new(db_engine));
        let bind_addr = format!("{}:{}", address, port);
        let listener = match TcpListener::bind(&bind_addr) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("FATAL: Could not bind to {}: {}", bind_addr, e);
                return;
            }
        };
        println!("[Server] Listening on {}", bind_addr);
    
        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                println!("[Server] Accepted raw connection from: {}", stream.peer_addr().unwrap());
                let db_clone = db.clone();
                thread::spawn(move || {
                    handle_client(stream, db_clone);
                });
            }
        }
    }

    fn handle_client(mut stream: TcpStream, db: Arc<RwLock<DatabaseEngine>>) {
        let peer_addr = stream.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "unknown".to_string());
        println!("[Server] Client connected: {}", peer_addr);
        let mut txn_guard: Option<RwLockWriteGuard<DatabaseEngine>> = None;

        loop {
            let (opcode, payload) = match read_packet(&mut stream) {
                Ok(packet) => packet,
                Err(_) => { println!("[Server] Client {} disconnected.", peer_addr); break; }
            };

            let result = process_command(opcode, payload, &db, &mut txn_guard);
            let response_result = match result {
                Ok(CommandResponse::Ok(msg)) => send_ok(&mut stream, &msg),
                Ok(CommandResponse::ResultSet(schema, rows)) => {
                    match send_result_set(&mut stream, schema, rows) {
                        Ok(()) => Ok(()),
                        Err(e) => send_error(&mut stream, &e.to_string()),
                    }
                }
                Err(e) => send_error(&mut stream, &e.to_string()),
            };

            if response_result.is_err() {
                eprintln!("[Server] Failed to send response to client {}.", peer_addr);
                break;
            }
        }

        // Roll back any active transaction on disconnect
        if let Some(mut guard) = txn_guard.take() {
            println!("[Server] Rolling back active transaction for disconnected client {}.", peer_addr);
            let _ = guard.rollback_transaction();
            guard.end_transaction();
            let _ = guard.recovery();
        }
    }

    fn process_command<'a>(
        opcode: u8,
        payload: Vec<u8>,
        db: &'a Arc<RwLock<DatabaseEngine>>,
        txn_guard: &mut Option<RwLockWriteGuard<'a, DatabaseEngine>>,
    ) -> Result<CommandResponse, DbError> {
        // Commands that do not need a transaction
        if opcode == OP_INFO {
            let info = if txn_guard.is_some() { "Mode: Writer (in transaction)" } else { "Mode: Reader" };
            return Ok(CommandResponse::Ok(info.to_string()));
        }

        // Explicit transaction commands
        if opcode == OP_BEGIN {
            if txn_guard.is_some() {
                return Err(DbError::DbEngine("Already in a transaction".to_string()));
            }
            let mut guard = db.write().map_err(|e| DbError::DbEngine(format!("Lock error: {}", e)))?;
            guard.begin_transaction()?;
            *txn_guard = Some(guard);
            return Ok(CommandResponse::Ok("BEGIN".to_string()));
        }
        if opcode == OP_COMMIT {
            let mut guard = txn_guard.take().ok_or_else(|| DbError::DbEngine("Not in a transaction".to_string()))?;
            guard.commit_transaction()?;
            guard.end_transaction();
            guard.recovery()?;
            return Ok(CommandResponse::Ok("COMMIT".to_string()));
        }
        if opcode == OP_ROLLBACK {
            let mut guard = txn_guard.take().ok_or_else(|| DbError::DbEngine("Not in a transaction".to_string()))?;
            guard.rollback_transaction()?;
            guard.end_transaction();
            guard.recovery()?;
            return Ok(CommandResponse::Ok("ROLLBACK".to_string()));
        }

        // Read-only and special commands (No write lock needed if not in transaction)
        if (opcode == OP_QUERY && (payload_is_select(&payload) || payload_is_explain(&payload))) || opcode == OP_EXPORT {
            if let Some(guard) = txn_guard.as_ref() {
                return execute_read_command(opcode, payload, guard);
            } else {
                let guard = db.read().map_err(|e| DbError::DbEngine(format!("Lock error: {}", e)))?;
                return execute_read_command(opcode, payload, &guard);
            }
        }

        // Write commands: explicit or auto-commit
        if let Some(guard) = txn_guard.as_mut() {
            // inside BEGIN...COMMIT block
            return execute_write_and_wrap(guard, opcode, payload);
        } else {
            // auto-commit single statement
            let mut guard = db.write().map_err(|e| DbError::DbEngine(format!("Lock error: {}", e)))?;
            guard.begin_transaction()?;
            let outcome = execute_write_and_wrap(&mut guard, opcode, payload);
            match outcome {
                Ok(response) => {
                    guard.commit_transaction()?;
                    guard.end_transaction();
                    guard.recovery()?;
                    Ok(response)
                }
                Err(e) => {
                    let _ = guard.rollback_transaction();
                    guard.end_transaction();
                    guard.recovery()?;
                    Err(e)
                }
            }
        }
    }

    fn execute_read_command(
        opcode: u8,
        payload: Vec<u8>,
        guard: &impl std::ops::Deref<Target = DatabaseEngine>,
    ) -> Result<CommandResponse, DbError> {
        if opcode == OP_EXPORT {
            let args_str = String::from_utf8(payload).map_err(|e| DbError::Parser(e.to_string()))?;
            let parts: Vec<&str> = args_str.splitn(2, ' ').collect();
            if parts.len() != 2 {
                return Err(DbError::Parser("Invalid EXPORT syntax.".to_string()));
            }
            guard.export_query_csv(parts[1], parts[0])?;
            Ok(CommandResponse::Ok(format!("Exported to {}", parts[0])))
        } else {
            let query = String::from_utf8(payload).map_err(|e| DbError::Parser(e.to_string()))?;
            // EXPLAIN support!
            if query.trim_start().to_uppercase().starts_with("EXPLAIN") {
                let mut parser = Parser::new(Lexer::new(&query))?;
                let stmt = parser.parse_query()?;
                let planner = QueryPlanner::new(guard);
                let plan = planner.plan_statement(stmt)?;
                // Pretty-print or debug-print the plan as you wish:
                let plan_lines = pretty_explain_lines(&plan);
                let explain_output = format!("-----QUERY PLAN-----\n{}", plan_lines.join("\n"));
                return Ok(CommandResponse::Ok(explain_output));
            }
            // Normal SELECT or other read
            let mut parser = Parser::new(Lexer::new(&query))?;
            let stmt = parser.parse_query()?;
            let planner = QueryPlanner::new(guard);
            let plan = planner.plan_statement(stmt)?;
            let mut iter = guard.execute_plan(&plan)?;
            let schema = iter.schema().clone();
            let rows = iter.collect();
            Ok(CommandResponse::ResultSet(schema, rows))
        }
    }

    fn execute_write_and_wrap(
        guard: &mut RwLockWriteGuard<DatabaseEngine>,
        opcode: u8,
        payload: Vec<u8>,
    ) -> Result<CommandResponse, DbError> {
        execute_write_command(opcode, payload, guard)
    }

    fn execute_write_command(
        opcode: u8,
        payload: Vec<u8>,
        guard: &mut RwLockWriteGuard<DatabaseEngine>,
    ) -> Result<CommandResponse, DbError> {
        let msg = match opcode {
            OP_QUERY => {
                let query = String::from_utf8(payload).map_err(|e| DbError::Parser(e.to_string()))?;
                let mut parser = Parser::new(Lexer::new(&query))?;
                let stmt = parser.parse_query()?;
                let planner = QueryPlanner::new(&**guard);
                let plan = planner.plan_statement(stmt)?;
                guard.execute_write_plan(&plan)?;
                match plan {
                    PlanNode::Insert { .. } => "INSERT OK",
                    PlanNode::Update { .. } => "UPDATE OK",
                    PlanNode::Delete { .. } => "DELETE OK",
                    PlanNode::CreateTable { .. } => "CREATE TABLE OK",
                    PlanNode::CreateIndex { .. } => "CREATE INDEX OK",
                    _ => "WRITE OK",
                }
                .to_string()
            }
            OP_LOAD => {
                let args = String::from_utf8(payload).map_err(|e| DbError::Parser(e.to_string()))?;
                let parts: Vec<&str> = args.split_whitespace().collect();
                if parts.len() < 3 {
                    return Err(DbError::Parser("Invalid LOAD syntax. Use: LOAD <tbl> <file.csv> <force> [pk]".to_string()));
                }
                let table_name = parts[0];
                let force = parts[2].parse::<u8>().map_err(|_| DbError::Parser("Invalid 'force' value, must be 0 or 1.".to_string()))?;
                guard.reload_catalog();
                if guard.catalog.contains_table(table_name) && force == 0 {
                    return Err(DbError::DbEngine(format!("Table '{}' already exists.", table_name)));
                }
                let skipped = guard.load_csv_pk(table_name, parts[1], parts.get(3).copied(), force)?;
                let mut msg = format!("LOAD OK: Loaded {} into table {}", parts[1], table_name);
                if skipped > 0 {
                    msg.push_str(&format!(" ({} rows skipped due to invalid data)", skipped));
                }
                msg
            }
            OP_DEBUG => {
                guard.print_catalog();
                guard.print_debug_btree();
                "DEBUG OK".to_string()
            }
            _ => return Err(DbError::DbEngine("Unknown OpCode for write path".to_string())),
        };
        Ok(CommandResponse::Ok(msg))
    }

    fn payload_is_select(payload: &[u8]) -> bool {
        payload.get(..6).map_or(false, |s| s.eq_ignore_ascii_case(b"SELECT"))
    }
    fn payload_is_explain(payload: &[u8]) -> bool {
        payload.get(..7).map_or(false, |s| s.eq_ignore_ascii_case(b"EXPLAIN"))
    }

    fn read_packet(stream: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
        let mut opcode_buf = [0u8; 1];
        stream.read_exact(&mut opcode_buf)?;
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf);
        let mut payload = vec![0u8; len as usize];
        if len > 0 { stream.read_exact(&mut payload)?; }
        Ok((opcode_buf[0], payload))
    }

    fn send_packet(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> io::Result<()> {
        stream.write_all(&[opcode])?;
        stream.write_all(&(payload.len() as u32).to_be_bytes())?;
        stream.write_all(payload)
    }

    fn send_ok(stream: &mut TcpStream, msg: &str) -> io::Result<()> { send_packet(stream, RESP_OK, msg.as_bytes()) }
    fn send_error(stream: &mut TcpStream, msg: &str) -> io::Result<()> { send_packet(stream, RESP_ERR, msg.as_bytes()) }

    fn send_result_set(stream: &mut TcpStream, schema: TableSchema, rows: Vec<Row>) -> Result<(), DbError> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(schema.columns.len() as u32).to_be_bytes());
        for i in 0..schema.columns.len() {
            payload.extend_from_slice(&(schema.name_col[i].len() as u32).to_be_bytes());
            payload.extend_from_slice(schema.name_col[i].as_bytes());
            payload.push(match schema.columns[i] {
                SqlColumnType::TINYINT=>1, SqlColumnType::SMALLINT=>2, SqlColumnType::INT=>3,
                SqlColumnType::BIGINT=>4, SqlColumnType::UTINYINT=>5, SqlColumnType::USMALLINT=>6,
                SqlColumnType::UINT=>7, SqlColumnType::UBIGINT=>8, SqlColumnType::FLOAT=>9,
                SqlColumnType::DOUBLE=>10, SqlColumnType::CHAR=>11, SqlColumnType::VARCHAR=>12,
            });
        }
        payload.extend_from_slice(&(rows.len() as u32).to_be_bytes());
        for row in &rows {
            let row_bytes = crate::bintuco::encode_to_vec(row)?;
            payload.extend_from_slice(&(row_bytes.len() as u32).to_be_bytes());
            payload.extend_from_slice(&row_bytes);
        }
        send_packet(stream, RESP_RESULT_SET, &payload)?;
        Ok(())
    }
}

fn pretty_explain_lines(plan: &PlanNode) -> Vec<String> {
    match plan {
        PlanNode::Explain { inner, .. } => pretty_explain_lines(inner),
        PlanNode::Projection { input, expressions, .. } => {
            let mut lines = pretty_explain_lines(input);
            lines.push(format!("-->Projection {:?}", expressions));
            lines
        }
        PlanNode::OrderBy { input, orderings, .. } => {
            let mut lines = pretty_explain_lines(input);
            lines.push(format!("-->OrderBy {:?}", orderings));
            lines
        }
        PlanNode::TableScan { table, .. } => {
            vec![format!("-->TableScan {}", table)]
        }
        PlanNode::Filter { input, predicate, .. } => {
            let mut lines = pretty_explain_lines(input);
            lines.push(format!("-->Filter {:?}", predicate));
            lines
        }
        PlanNode::Limit { input, count, .. } => {
            let mut lines = pretty_explain_lines(input);
            lines.push(format!("-->Limit {}", count));
            lines
        }
        PlanNode::Insert { table, .. } => vec![format!("-->Insert into {}", table)],
        PlanNode::Delete { table, .. } => vec![format!("-->Delete from {}", table)],
        PlanNode::Update { table, .. } => vec![format!("-->Update {}", table)],
        PlanNode::CreateTable { table, .. } => vec![format!("-->CreateTable {}", table)],
        _ => vec![format!("-->{:?}", plan)], // IndexScan, CreateIndex ecc..
    }
}

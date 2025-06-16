//! An interactive command-line client for TucoDB.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::fmt;


const SQL_COMMANDS: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE",
    "EXPLAIN",
];



#[derive(Debug)]
enum SqlValue {
    TINYINT(i8), SMALLINT(i16), INT(i32), BIGINT(i64), UTINYINT(u8),
    USMALLINT(u16), UINT(u32), UBIGINT(u64), FLOAT(f32), DOUBLE(f64),
    CHAR(Vec<u8>), VARCHAR(Vec<u8>),
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
            SqlValue::CHAR(v) | SqlValue::VARCHAR(v) => write!(f, "{}", String::from_utf8_lossy(v)),
        }
    }
}

// --- Protocol OpCodes (must match server) ---
const OP_BEGIN: u8 = 0x01;
const OP_COMMIT: u8 = 0x02;
const OP_ROLLBACK: u8 = 0x03;
const OP_QUERY: u8 = 0x04;
const OP_INFO: u8 = 0x05;
const OP_LOAD: u8 = 0x07;
const OP_DEBUG: u8 = 0x06;
const OP_EXPORT: u8 = 0x08;

const RESP_OK: u8 = 0x80;
const RESP_ERR: u8 = 0x81;
const RESP_RESULT_SET: u8 = 0x82;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _bin = args.next();

    let mut address = "127.0.0.1".to_string();
    let mut port = "7878".to_string();

    match (args.next(), args.next()) {
        (None, _) => {} // default
        (Some(arg1), None) => {
            if arg1.chars().all(|c| c.is_ascii_digit()) {
                port = arg1;
            } else {
                address = arg1;
            }
        }
        (Some(arg1), Some(arg2)) => {
            address = arg1;
            port = arg2;
        }
    }

    let addr = format!("{}:{}", address, port);
    println!("Connecting to TucoDB at {}...", addr);
    let mut stream = TcpStream::connect(&addr)?;
    println!("Connected! Type 'QUIT' to exit.");


    let mut rl = DefaultEditor::new()?;
    let _ = rl.load_history(".tuco_history");

    loop {
        let readline = rl.readline("memtuco> ");
        let line = match readline {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => { eprintln!("Readline Error: {}", err); break; }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        rl.add_history_entry(trimmed)?;
        
        let upper_cmd = trimmed.to_uppercase();
        if upper_cmd == "QUIT" || upper_cmd == "EXIT" { break; }
        
        // Handle client-side commands like CLEAR
        if upper_cmd == "CLEAR" {
            // ANSI escape code to clear the screen
            print!("\x1B[2J\x1B[H");
            io::stdout().flush()?;
            continue;
        }
        
        let (opcode, payload) = if upper_cmd.starts_with("LOAD ") {
            (OP_LOAD, trimmed["LOAD ".len()..].as_bytes().to_vec())
        } else if upper_cmd.starts_with("EXPORT ") {
            (OP_EXPORT, trimmed["EXPORT ".len()..].as_bytes().to_vec())
        } else if upper_cmd == "BEGIN" { (OP_BEGIN, vec![]) }
        else if upper_cmd == "COMMIT" { (OP_COMMIT, vec![]) }
        else if upper_cmd == "ROLLBACK" { (OP_ROLLBACK, vec![]) }
        else if upper_cmd == "DEBUG" { (OP_DEBUG, vec![]) }
        else if upper_cmd == "INFO" { (OP_INFO, vec![]) }
        else {
            // Try to detect valid SQL verb before sending to server
            let first_word = trimmed.split_whitespace().next().unwrap_or("").to_uppercase();
            if SQL_COMMANDS.contains(&first_word.as_str()) {
                (OP_QUERY, trimmed.as_bytes().to_vec())
            } else {
                println!("Unknown command or invalid SQL start: '{}'", first_word);
                continue; // Skip sending to server
            }
        };
        

        if let Err(e) = send_packet(&mut stream, opcode, &payload) {
            eprintln!("Connection error: {}. Please restart client.", e); break;
        }

        match read_packet(&mut stream) {
            Ok((resp_code, resp_payload)) => {
                match resp_code {
                    RESP_OK => println!("OK: {}", String::from_utf8_lossy(&resp_payload)),
                    RESP_ERR => eprintln!("ERR: {}", String::from_utf8_lossy(&resp_payload)),
                    RESP_RESULT_SET => print_result_set(&resp_payload)?,
                    _ => eprintln!("Unknown response from server: {:#02x}", resp_code),
                }
            },
            Err(e) => { eprintln!("Connection error: {}. Please restart client.", e); break; }
        }
    }

    rl.save_history(".tuco_history")?;
    Ok(())
}

fn send_packet(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> io::Result<()> {
    stream.write_all(&[opcode])?;
    stream.write_all(&(payload.len() as u32).to_be_bytes())?;
    stream.write_all(payload)?;
    Ok(())
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

fn print_result_set(payload: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let mut cursor = 0;
    
    let read_u32 = |c: &mut usize, p: &[u8]| -> u32 {
        let val = u32::from_be_bytes(p.get(*c..*c+4).unwrap_or(&[0;4]).try_into().unwrap());
        *c += 4;
        val
    };

    if payload.len() < 4 { return Ok(()); }
    let num_cols = read_u32(&mut cursor, payload) as usize;
    
    let mut headers = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        let name_len = read_u32(&mut cursor, payload) as usize;
        let name = String::from_utf8_lossy(&payload[cursor..cursor+name_len]).to_string();
        cursor += name_len;
        headers.push(name);
        cursor += 1; // Skip type byte
    }
    
    let num_rows = read_u32(&mut cursor, payload) as usize;
    let mut all_rows_str = Vec::with_capacity(num_rows);

    for _ in 0..num_rows {
        let row_len = read_u32(&mut cursor, payload) as usize;
        let row_payload = &payload[cursor..cursor+row_len];
        cursor += row_len;
        
        let (row, _) = client_decode_row(row_payload)?;
        all_rows_str.push(row.iter().map(|v| v.to_string()).collect::<Vec<_>>());
    }
    
    if headers.is_empty() && all_rows_str.is_empty() {
        println!("OK (empty result set)");
        return Ok(());
    }

    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    if num_cols > 0 {
        for row in &all_rows_str {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }
    
    let header_line = headers.iter().enumerate().map(|(i,h)| format!(" {:<w$} ", h, w=widths[i])).collect::<Vec<_>>().join("|");
    println!("\n{}", header_line);
    let sep_line = widths.iter().map(|w| "-".repeat(w+2)).collect::<Vec<_>>().join("+");
    println!("{}", sep_line);
    
    for row in &all_rows_str {
        let data_line = row.iter().enumerate().map(|(i,c)| format!(" {:<w$} ", c, w=widths[i])).collect::<Vec<_>>().join("|");
        println!("{}", data_line);
    }
    println!("\n({} rows)\n", num_rows);

    Ok(())
}

fn client_decode_row(input: &[u8]) -> Result<(Vec<SqlValue>, usize), Box<dyn std::error::Error>> {
    if input.len() < 4 { return Err("Invalid row payload".into()); }
    let count = u32::from_le_bytes(input[0..4].try_into()?) as usize;
    let mut values = Vec::with_capacity(count);
    let mut cursor = 4;
    for _ in 0..count {
        let (val, used) = client_decode_value(&input[cursor..])?;
        values.push(val);
        cursor += used;
    }
    Ok((values, cursor))
}

fn client_decode_value(input: &[u8]) -> Result<(SqlValue, usize), Box<dyn std::error::Error>> {
    let tag = *input.get(0).ok_or("Missing tag")?;
    let mut cursor = 1;

    let (value, len) = match tag {
        0 => (SqlValue::TINYINT(i8::from_le_bytes(input[cursor..cursor+1].try_into()?)), 1),
        1 => (SqlValue::SMALLINT(i16::from_le_bytes(input[cursor..cursor+2].try_into()?)), 2),
        2 => (SqlValue::INT(i32::from_le_bytes(input[cursor..cursor+4].try_into()?)), 4),
        3 => (SqlValue::UTINYINT(u8::from_le_bytes(input[cursor..cursor+1].try_into()?)), 1),
        4 => (SqlValue::USMALLINT(u16::from_le_bytes(input[cursor..cursor+2].try_into()?)), 2),
        5 => (SqlValue::UINT(u32::from_le_bytes(input[cursor..cursor+4].try_into()?)), 4),
        6 => (SqlValue::FLOAT(f32::from_le_bytes(input[cursor..cursor+4].try_into()?)), 4),
        7 => (SqlValue::DOUBLE(f64::from_le_bytes(input[cursor..cursor+8].try_into()?)), 8),
        8 | 9 => {
            let len_bytes: [u8; 4] = input[cursor..cursor+4].try_into()?;
            cursor += 4;
            let len = u32::from_le_bytes(len_bytes) as usize;
            let data = input[cursor..cursor+len].to_vec();
            let val = if tag == 8 { SqlValue::CHAR(data) } else { SqlValue::VARCHAR(data) };
            (val, 4 + len)
        }
        10 => (SqlValue::BIGINT(i64::from_le_bytes(input[cursor..cursor+8].try_into()?)), 8),
        11 => (SqlValue::UBIGINT(u64::from_le_bytes(input[cursor..cursor+8].try_into()?)), 8),
        _ => return Err(format!("Unknown SqlValue tag: {}", tag).into())
    };
    
    Ok((value, 1 + len))
}

// tests/integration_test.rs
pub mod test_utils;
use std::path::{Path, PathBuf};
use std::fs;
use memtuco::dbengine::DatabaseEngine;
use memtuco::dberror::DbError;
use memtuco::tokenizer::Statement;
use rusqlite::{Connection, Result as SqliteResult, types::{Value as SqliteValue}, ffi};
use csv::ReaderBuilder;
use std::sync::OnceLock as OnceCell;

// --- Global Test Environment (Fixed Location) ---

// Define the directory where test CSVs will be generated.
// This is relative to project's root.
// I dedice to use target/ because with cargo clean we clean all target/ folder
const TEST_DATA_DIR: &str = "target/test_data";

// This struct will hold the paths to our generated CSVs.
struct TestEnvironment {
    weather_csv_path: PathBuf,
    taxi_csv_path: PathBuf,
}

static TEST_ENV: OnceCell<TestEnvironment> = OnceCell::new();

/// Initializes the global test environment, generating CSVs if not already done.
/// This function ensures the CSV files are created only once at a predictable location.
fn get_test_environment() -> &'static TestEnvironment {
    TEST_ENV.get_or_init(|| {
        println!("\n--- Setting up global test environment: Generating CSVs ---");
        let base_dir = PathBuf::from(TEST_DATA_DIR);

        // Ensure the directory exists. Remove it first for a clean state each test run.
        if base_dir.exists() {
            fs::remove_dir_all(&base_dir).expect("Failed to clear existing test_data directory.");
        }
        fs::create_dir_all(&base_dir).expect("Failed to create test_data directory.");

        // Generate the CSV files in the specified directory
        let weather_path = test_utils::create_weather_csv(&base_dir, 2000)
            .expect("Failed to create weather.csv");
        let taxi_path = test_utils::create_taxi_csv(&base_dir, 2000)
            .expect("Failed to create taxi.csv");

        println!("CSVs generated in: {:?}", base_dir);
        println!("-------------------------------------------------------\n");

        TestEnvironment {
            weather_csv_path: weather_path,
            taxi_csv_path: taxi_path,
        }
    })
}

fn run_my_db_query(db: &mut DatabaseEngine, sql: &str) -> Result<Vec<Vec<String>>, DbError> {
    let lexer = memtuco::tokenizer::Lexer::new(sql);
    let mut parser = memtuco::tokenizer::Parser::new(lexer)?;
    let stmt = match parser.parse_query()? {
        Statement::Select(s) => Statement::Select(s),
        other => return Err(DbError::Other(format!("Expected SELECT statement, got {:?}", other))),
    };
    let planner = memtuco::planner::QueryPlanner::new(&*db);
    let plan = planner.plan_statement(stmt)?;

    let rows_iter = db.execute_plan(&plan)?;
    let results = rows_iter
        .map(|row| row.values.iter().map(|v| v.to_string()).collect())
        .collect();
    Ok(results)
}

// Helper to load CSV data into a rusqlite connection with robust schema inference
fn load_csv_into_sqlite(conn: &Connection, table_name: &str, csv_path: &Path) -> SqliteResult<()> {
    // Read headers first
    let mut reader_for_headers = ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .map_err(|e| rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_ERROR),
            Some(format!("CSV read error for headers from {}: {}", csv_path.display(), e))
        ))?;
    let headers: Vec<String> = reader_for_headers.headers()
        .map_err(|e| rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_ERROR),
            Some(format!("CSV header error for {}: {}", csv_path.display(), e))
        ))?
        .iter()
        .map(|s| s.to_string())
        .collect();

    // --- Dynamically determine column types by analyzing a significant sample of rows ---
    let mut column_inferred_types: Vec<String> = vec!["TEXT".to_string(); headers.len()]; // Default to TEXT

    let mut reader_for_inference = ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .map_err(|e| rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_ERROR),
            Some(format!("CSV read error for inference from {}: {}", csv_path.display(), e))
        ))?;

    let num_rows_to_sample = 50; // Increased sample size for better inference
    let mut sampled_records: Vec<csv::StringRecord> = Vec::new();
    for result in reader_for_inference.records() {
        if sampled_records.len() >= num_rows_to_sample { break; }
        sampled_records.push(result.map_err(|e| rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_ERROR),
            Some(format!("CSV record error during sample collection for {}: {}", csv_path.display(), e))
        ))?);
    }

    // Now, iterate through each column and try to determine its type based on the samples
    for col_idx in 0..headers.len() {
        let mut could_be_integer = true;
        let mut could_be_real = true;

        for record in &sampled_records {
            if let Some(value_str) = record.get(col_idx) {
                if value_str.trim().is_empty() {
                    // Empty string: can be NULL, doesn't break numeric inference
                    continue;
                }

                // Try to parse as integer
                if value_str.trim().parse::<i64>().is_err() {
                    could_be_integer = false; // Not all values are integers
                }
                
                // Try to parse as float (Real)
                if value_str.trim().parse::<f64>().is_err() {
                    could_be_real = false; // Not all values are real numbers
                }

                if !could_be_integer && !could_be_real {
                    // Cannot be integer or real, must be TEXT
                    break; // No need to check further for this column
                }
            }
        }

        // Determine final type based on what was possible
        if could_be_integer {
            column_inferred_types[col_idx] = "INTEGER".to_string();
        } else if could_be_real {
            column_inferred_types[col_idx] = "REAL".to_string();
        } else {
            column_inferred_types[col_idx] = "TEXT".to_string();
        }
    }


    // Create table with inferred types
    let columns_sql = headers.iter()
        .zip(column_inferred_types.iter()) // Pair headers with their inferred types
        .map(|(h, col_type)| {
            // Normalize column names for SQL (replace spaces, handle special chars)
            let column_name_normalized = h.replace(' ', "_").replace('.', "_");
            format!("`{}` {}", column_name_normalized, col_type)
        })
        .collect::<Vec<String>>()
        .join(", ");
    let create_table_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, columns_sql);
    conn.execute(&create_table_sql, [])?;

    // Prepare insert statement
    let placeholders = (0..headers.len()).map(|_| "?".to_string()).collect::<Vec<String>>().join(", ");
    let quoted_headers = headers.iter().map(|h| format!("`{}`", h.replace(' ', "_").replace('.', "_"))).collect::<Vec<String>>().join(", ");
    let insert_sql = format!("INSERT INTO {} ({}) VALUES ({})", table_name, quoted_headers, placeholders);
    let mut stmt = conn.prepare(&insert_sql)?;

    // Insert data (need a fresh reader as previous ones consumed records)
    let mut reader_for_insert = ReaderBuilder::new()
        .has_headers(true)
        .from_path(csv_path)
        .map_err(|e| rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_ERROR),
            Some(format!("CSV read error for insertion from {}: {}", csv_path.display(), e))
        ))?;

    for result in reader_for_insert.records() {
        let record = result.map_err(|e| rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_ERROR),
            Some(format!("CSV record error during insertion for {}: {}", csv_path.display(), e))
        ))?;

        let params_owned: Vec<SqliteValue> = record.iter()
            .zip(column_inferred_types.iter()) // Use the now determined inferred types
            .map(|(s, col_type)| {
                if col_type == "INTEGER" {
                    s.trim().parse::<i64>().map_or_else(
                        |_| SqliteValue::Null,
                        SqliteValue::Integer
                    )
                } else if col_type == "REAL" {
                    s.trim().parse::<f64>().map_or_else(
                        |_| SqliteValue::Null,
                        SqliteValue::Real
                    )
                } else { // It's TEXT
                    SqliteValue::Text(s.to_string())
                }
            })
            .collect();

        let params_to_sql: Vec<&dyn rusqlite::ToSql> = params_owned.iter()
            .map(|v| v as &dyn rusqlite::ToSql)
            .collect();

        stmt.execute(params_to_sql.as_slice())?;
    }
    Ok(())
}

fn run_sqlite_query(csvs: &[(&str, &Path)], sql: &str) -> SqliteResult<Vec<Vec<String>>> {
    let conn = Connection::open_in_memory()?; // Use in-memory for tests

    // Load CSV data into the SQLite connection
    for (table_name, path) in csvs {
        if !path.exists() {
            return Err(rusqlite::Error::SqliteFailure(
                ffi::Error::new(ffi::SQLITE_NOTFOUND), // More specific for file not found
                Some(format!("CSV file '{}' for table '{}' not found", path.display(), table_name))
            ));
        }
        load_csv_into_sqlite(&conn, table_name, path)?;
    }

    let mut stmt = conn.prepare(sql)?;
    let column_count = stmt.column_count();

    let rows = stmt.query_map([], |row| {
        let mut row_vals = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let val: SqliteValue = row.get_ref(i)?.to_owned().into();
            let s = match val {
                SqliteValue::Null => "NULL".to_string(),
                SqliteValue::Integer(i) => i.to_string(),
                SqliteValue::Real(f) => {
                    if f.fract() == 0.0 { // If it's effectively an integer
                        format!("{:.0}", f)
                    } else {
                        format!("{:.6}", f) // Adjust precision as needed
                    }
                },
                SqliteValue::Text(t) => t,
                SqliteValue::Blob(b) => format!("{:?}", b),
            };
            row_vals.push(s);
        }
        Ok(row_vals)
    })?;

    rows.collect::<SqliteResult<Vec<Vec<String>>>>()
}


fn normalize_string(s: &str) -> String {
    s.trim().to_string()
}

// Compare floats with a tolerance
fn eq_with_tol(a: &str, b: &str, tol: f64) -> bool {
    match (a.trim().parse::<f64>(), b.trim().parse::<f64>()) {
        (Ok(fa), Ok(fb)) => (fa - fb).abs() < tol,
        _ => normalize_string(a) == normalize_string(b),
    }
}

fn compare_results(expected: &[Vec<String>], actual: &[Vec<String>]) -> bool {
    let mut expected_sorted = expected.to_vec();
    let mut actual_sorted = actual.to_vec();
    expected_sorted.sort();
    actual_sorted.sort();

    if expected_sorted.len() != actual_sorted.len() {
        println!("Result length mismatch: Expected {} rows, Actual {} rows", expected_sorted.len(), actual_sorted.len());
        return false;
    }

    for (row_a, row_b) in expected_sorted.iter().zip(actual_sorted.iter()) {
        if row_a.len() != row_b.len() {
            println!("Column count mismatch in rows: Expected {} columns, Actual {} columns", row_a.len(), row_b.len());
            return false;
        }
        for (a, b) in row_a.iter().zip(row_b.iter()) {
            if !eq_with_tol(a, b, 1e-3) {
                println!("Mismatch: '{}' (Expected) vs '{}' (Actual)", a, b);
                return false;
            }
        }
    }
    true
}

fn clear_db(db_path: &str) {
    if Path::new(db_path).exists() {
        if let Err(e) = fs::remove_dir_all(db_path) {
            panic!("Failed to remove database directory '{}': {}", db_path, e);
        }
        println!("Successfully cleared database directory: {}", db_path);
    } else {
        println!("Database directory '{}' does not exist, no need to clear.", db_path);
    }
}



// --- Test Functions ---

#[test]
fn test_avg_base_fare_groupby_having_print() {
    let db_name = "target/mydb_avg_base_fare_groupby_having_print";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).expect("Failed to open DB");

    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).expect("Failed to load taxi CSV");
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).expect("Failed to load weather CSV");
    db.reload_catalog();

    let sql = r#"
        SELECT payment_mode, AVG(base_fare), COUNT(*)
        FROM taxi
        GROUP BY payment_mode
        HAVING AVG(base_fare) > 6
        ORDER BY AVG(base_fare) DESC
    "#;

    let my_db_res = run_my_db_query(&mut db, sql).expect("run_my_db_query failed");
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).expect("SQLite query failed");

    println!("\n--- Test: test_avg_fare_groupby_having ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");


    assert!(compare_results(&sqlite_res, &my_db_res));
}


#[test]
fn test_avg_fare_amount_groupby_having() {
    let db_name = "target/mydb_avg_fare_amount_groupby_having";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).expect("Failed to open DB");

    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).expect("Failed to load taxi csv");
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).expect("Failed to load weather csv");
    db.reload_catalog();

    let sql = r#"
        SELECT payment_mode, AVG(base_fare), COUNT(*)
        FROM taxi
        GROUP BY payment_mode
        HAVING AVG(base_fare) > 6
        ORDER BY AVG(base_fare) DESC
    "#;

    let my_db_res = run_my_db_query(&mut db, sql).expect("run_my_db_query failed");
    let sqlite_res = match run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql,
    ) {
        Ok(rows) => rows,
        Err(e) => panic!("SQLite query failed: {:?}", e),
    };

    println!("\n--- Test: test_avg_fare_amount_groupby_having ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_basic_select() {
    let db_name = "target/mydb_basic_select";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).expect("Failed to open DB");

    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).expect("Failed to load taxi csv");
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).expect("Failed to load weather csv");
    db.reload_catalog();

    let sql = "SELECT min_temp FROM weather";

    let my_db_res = run_my_db_query(&mut db, sql).expect("run_my_db_query failed");
    let sqlite_res = match run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql,
    ) {
        Ok(rows) => rows,
        Err(e) => panic!("SQLite query failed: {:?}", e),
    };

    println!("\n--- Test: test_basic_select ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_basic_order() {
    let db_name = "target/mydb_basic_order";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).expect("Failed to open DB");

    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).expect("Failed to load taxi csv");
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).expect("Failed to load weather csv");
    db.reload_catalog();

    let sql = "SELECT min_temp FROM weather ORDER BY min_temp DESC";

    let my_db_res = run_my_db_query(&mut db, sql).expect("run_my_db_query failed");
    let sqlite_res = match run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql,
    ) {
        Ok(rows) => rows,
        Err(e) => panic!("SQLite query failed: {:?}", e),
    };

    println!("\n--- Test: test_basic_order ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_basic_join() {
    let db_name = "target/mydb_basic_join";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).expect("Failed to open DB");

    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).expect("Failed to load taxi csv");
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).expect("Failed to load weather csv");
    db.reload_catalog();

    let sql = "SELECT max_temp, min_temp, max_temp*min_temp FROM weather JOIN taxi ON weather.max_temp = taxi.end_zone ORDER BY max_temp DESC, min_temp DESC";

    let my_db_res = run_my_db_query(&mut db, sql).expect("run_my_db_query failed");
    let sqlite_res = match run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql,
    ) {
        Ok(rows) => rows,
        Err(e) => panic!("SQLite query failed: {:?}", e),
    };

    println!("\n--- Test: test_basic_join ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_avg_count_aggregate() {
    let db_name = "target/mydb_avg_count_aggregate";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).expect("Failed to open DB");

    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).expect("Failed to load taxi csv");
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).expect("Failed to load weather csv");
    db.reload_catalog();

    let sql = "SELECT AVG(min_temp) * COUNT(*) FROM weather";

    let my_db_res = run_my_db_query(&mut db, sql).expect("run_my_db_query failed");
    let sqlite_res = match run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql,
    ) {
        Ok(rows) => rows,
        Err(e) => panic!("SQLite query failed: {:?}", e),
    };

    println!("\n--- Test: test_avg_count_aggregate ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_avg_count_groupby() {
    let db_name = "target/mydb_avg_count_groupby";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).expect("Failed to open DB");

    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).expect("Failed to load taxi csv");
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).expect("Failed to load weather csv");
    db.reload_catalog();

    let sql = "SELECT region, AVG(min_temp), COUNT(*) FROM weather GROUP BY region";

    let my_db_res = run_my_db_query(&mut db, sql).expect("run_my_db_query failed");
    let sqlite_res = match run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql,
    ) {
        Ok(rows) => rows,
        Err(e) => panic!("SQLite query failed: {:?}", e),
    };

    println!("\n--- Test: test_avg_count_groupby ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_weather_prcp_filter() {
    let db_name = "target/mydb_prcp_filter";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).expect("Failed to open DB");
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT rainfall FROM weather WHERE rainfall > 4 ORDER BY rainfall DESC";
    let my_db_res = run_my_db_query(&mut db, sql).expect("run_my_db_query failed");
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql)
        .expect("SQLite query failed");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_min_tmin() {
    let db_name = "target/mydb_min_tmin";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT MIN(min_temp) FROM weather";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_max_tmax() {
    let db_name = "target/mydb_max_tmax";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT MAX(max_temp) FROM weather";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_sum_passenger_count() {
    let db_name = "target/mydb_sum_passenger_count";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT SUM(riders) FROM taxi";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_count_ny_us() {
    let db_name = "target/mydb_count_ny_us";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT COUNT(*) FROM weather WHERE region = 'East'";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();

    println!("\n--- Test: test_count_ny_us ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_distinct_city() {
    let db_name = "target/mydb_distinct_city";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT DISTINCT region FROM weather ORDER BY region";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_tmax_tmin_diff() {
    let db_name = "target/mydb_tmax_tmin_diff";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT max_temp, min_temp, (max_temp - min_temp) FROM weather ORDER BY (max_temp - min_temp) DESC";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();

    println!("\n--- Test: test_tmax_tmin_diff ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_join_zero_count() {
    let db_name = "target/mydb_join_zero_count";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT COUNT(*) FROM taxi JOIN weather ON taxi.end_zone = weather.min_temp";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();
    
    println!("\n--- Test: join_zero_count ---");
    println!("My DB Result:\n{:?}", my_db_res);
    println!("SQLite Result:\n{:?}", sqlite_res);
    println!("-----------------------------\n");
    
    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_taxi_filter_complex() {
    let db_name = "target/mydb_taxi_filter_complex";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT base_fare, riders FROM taxi WHERE base_fare > 20 AND riders <= 2 ORDER BY base_fare DESC";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_payment_type_groupby() {
    let db_name = "target/mydb_payment_type_groupby";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT payment_mode, COUNT(*) FROM taxi GROUP BY payment_mode ORDER BY payment_mode";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(&[("taxi", taxi_csv_path), ("weather", weather_csv_path)], sql).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_weather_snow_filter() {
    let db_name = "target/mydb_weather_snow_filter";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT snowfall, snow_depth FROM weather WHERE snowfall > 2 ORDER BY snowfall DESC, snow_depth DESC";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_avg_trip_distance() {
    let db_name = "target/mydb_avg_trip_distance";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT AVG(distance_miles) FROM taxi";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_congestion_surcharge_count() {
    let db_name = "target/mydb_congestion_count";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT COUNT(*) FROM taxi WHERE traffic_charge > 0";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_prcp_snow_ratio() {
    let db_name = "target/mydb_prcp_snow_ratio";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT (rainfall / snowfall) FROM weather WHERE snowfall > 0 ORDER BY (rainfall / snowfall) DESC";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_ratecode_groupby() {
    let db_name = "target/mydb_ratecode_groupby";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT zone_type, COUNT(*) FROM taxi GROUP BY zone_type ORDER BY zone_type";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_trip_distance_filter() {
    let db_name = "target/mydb_trip_distance_filter";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT distance_miles FROM taxi WHERE distance_miles > 10 ORDER BY distance_miles";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_snow_sum() {
    let db_name = "target/mydb_snow_sum";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT SUM(snowfall) FROM weather";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_count_tmax_gt_tmin() {
    let db_name = "target/mydb_count_tmax_gt_tmin";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT COUNT(*) FROM weather WHERE max_temp > min_temp";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_temp_diff_filter() {
    let db_name = "target/mydb_temp_diff_filter";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT max_temp - min_temp FROM weather WHERE (max_temp - min_temp) > 20 ORDER BY (max_temp - min_temp) DESC";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_taxi_pu_do_equal() {
    let db_name = "target/mydb_pu_do_equal";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT COUNT(*) FROM taxi WHERE start_zone = end_zone";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_select_weather_tmin_tmax() {
    let db_name = "target/mydb_select_weather_tmin_tmax";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT min_temp, max_temp FROM weather";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_select_taxi_amounts() {
    let db_name = "target/mydb_select_taxi_amounts";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT base_fare, gratuity, total_cost FROM taxi";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_count_by_city() {
    let db_name = "target/mydb_count_by_city";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT region, COUNT(*) FROM weather GROUP BY region";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_avg_tmin_by_city() {
    let db_name = "target/mydb_avg_tmin_by_city";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT region, AVG(min_temp) FROM weather GROUP BY region";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_sum_prcp_by_station() {
    let db_name = "target/mydb_sum_prcp_by_station";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT sensor_id, SUM(rainfall) FROM weather GROUP BY sensor_id";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_weather_awnd_filter_ordered() {
    let db_name = "target/mydb_weather_awnd_filter";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT sensor_id, avg_wind FROM weather WHERE avg_wind > 10 ORDER BY avg_wind";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_taxi_distance_plus_extra() {
    let db_name = "target/mydb_taxi_distance_plus_extra";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT distance_miles + surcharge FROM taxi";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_taxi_pu_eq_do() {
    let db_name = "target/mydb_taxi_pu_eq_do";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT start_zone, end_zone FROM taxi WHERE start_zone = end_zone";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_distinct_payment_type() {
    let db_name = "target/mydb_distinct_payment_type";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT DISTINCT payment_mode FROM taxi ORDER BY payment_mode";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

#[test]
fn test_weather_temp_and_ratio() {
    let db_name = "target/mydb_weather_temp_and_ratio";
    clear_db(db_name);

    let env = get_test_environment();
    let weather_csv_path = &env.weather_csv_path;
    let taxi_csv_path = &env.taxi_csv_path;

    let mut db = DatabaseEngine::open(db_name).unwrap();
    db.load_csv_pk("taxi", taxi_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.load_csv_pk("weather", weather_csv_path.to_str().unwrap(), None, 0).unwrap();
    db.reload_catalog();

    let sql = "SELECT max_temp - min_temp, rainfall / avg_wind FROM weather ORDER BY (max_temp - min_temp) DESC";
    let my_db_res = run_my_db_query(&mut db, sql).unwrap();
    let sqlite_res = run_sqlite_query(
        &[("taxi", taxi_csv_path), ("weather", weather_csv_path)],
        sql
    ).unwrap();

    assert!(compare_results(&sqlite_res, &my_db_res));
}

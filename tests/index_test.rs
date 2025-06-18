// tests/index_test.rs
pub mod test_utils;
use std::path::{Path, PathBuf};
use std::fs;
use memtuco::dbengine::DatabaseEngine;
use memtuco::dberror::DbError;
use rusqlite::{Connection, Result as SqliteResult, types::{Value as SqliteValue}};
use std::sync::OnceLock as OnceCell;

const TEST_DATA_DIR: &str = "target/test_data";

struct TestEnv {
    numbers_csv: PathBuf,
}

static TEST_ENV: OnceCell<TestEnv> = OnceCell::new();

fn get_test_env() -> &'static TestEnv {
    TEST_ENV.get_or_init(|| {
        let base_dir = PathBuf::from(TEST_DATA_DIR);

        if base_dir.exists() {
            fs::remove_dir_all(&base_dir).expect("Failed to clear test_data directory.");
        }
        fs::create_dir_all(&base_dir).expect("Failed to create test_data directory.");

        // Create a simple CSV: header "number", then numbers 0..n
        let numbers_path = test_utils::create_simple_number_csv(&base_dir, 100)
            .expect("Failed to create numbers.csv");

        TestEnv { numbers_csv: numbers_path }
    })
}

fn run_my_db_query(db: &mut DatabaseEngine, sql: &str) -> Result<Vec<Vec<String>>, DbError> {
    use memtuco::tokenizer::Statement;

    let lexer = memtuco::tokenizer::Lexer::new(sql);
    let mut parser = memtuco::tokenizer::Parser::new(lexer)?;
    let stmt = parser.parse_query()?;
    let planner = memtuco::planner::QueryPlanner::new(&*db);
    let plan = planner.plan_statement(stmt.clone())?;

    match stmt {
        Statement::Select(_) => {
            // SELECTs use execute_plan
            let rows_iter = db.execute_plan(&plan)?;
            let results = rows_iter
                .map(|row| row.values.iter().map(|v| v.to_string()).collect())
                .collect();
            Ok(results)
        }
        Statement::Insert { .. } => {
            db.execute_write_plan(&plan)?;
            Ok(vec![vec!["INSERT OK".to_string()]])
        }
        Statement::Update { .. } => {
            db.execute_write_plan(&plan)?;
            Ok(vec![vec!["UPDATE OK".to_string()]])
        }
        Statement::Delete { .. } => {
            db.execute_write_plan(&plan)?;
            Ok(vec![vec!["DELETE OK".to_string()]])
        }
        Statement::Create { .. } => {
            db.execute_write_plan(&plan)?;
            Ok(vec![vec!["CREATE TABLE OK".to_string()]])
        }
        Statement::CreateIndex { .. } => {
            db.execute_write_plan(&plan)?;
            Ok(vec![vec!["CREATE INDEX OK".to_string()]])
        }
        _ => Err(DbError::Other("Unexpected statement type in run_my_db_query".into()))
    }
}



fn load_csv_into_sqlite(conn: &Connection, table: &str, csv_path: &Path) -> SqliteResult<()> {
    // Simple: numbers is always INTEGER PRIMARY KEY
    let create = format!("CREATE TABLE IF NOT EXISTS {} (number INTEGER PRIMARY KEY)", table);
    conn.execute(&create, [])?;

    let mut rdr = csv::Reader::from_path(csv_path)
        .map_err(|_e| rusqlite::Error::ExecuteReturnedResults)?;
    let mut stmt = conn.prepare(&format!("INSERT INTO {} (number) VALUES (?1)", table))?;
    for result in rdr.records() {
        let rec = result.map_err(|_| rusqlite::Error::ExecuteReturnedResults)?;
        let val = rec.get(0).unwrap().parse::<i64>().unwrap();
        stmt.execute([val])?;
    }
    Ok(())
}

fn run_sqlite_query(csv: &Path, sql: &str) -> SqliteResult<Vec<Vec<String>>> {
    let conn = Connection::open_in_memory()?;
    load_csv_into_sqlite(&conn, "numbers", csv)?;

    let mut stmt = conn.prepare(sql)?;
    let col_count = stmt.column_count();
    let rows = stmt.query_map([], |row| {
        let mut vals = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val: SqliteValue = row.get(i)?;
            let s = match val {
                SqliteValue::Null => "NULL".to_string(),
                SqliteValue::Integer(i) => i.to_string(),
                SqliteValue::Real(f) => format!("{}", f),
                SqliteValue::Text(t) => t,
                SqliteValue::Blob(_) => "[BLOB]".to_string(),
            };
            vals.push(s);
        }
        Ok(vals)
    })?;
    rows.collect()
}

fn compare_results(expected: &[Vec<String>], actual: &[Vec<String>]) -> bool {
    let mut e = expected.to_vec();
    let mut a = actual.to_vec();
    e.sort();
    a.sort();
    e == a
}

fn clear_db(db_path: &str) {
    if Path::new(db_path).exists() {
        let _ = fs::remove_dir_all(db_path);
    }
}

// ----------- ACTUAL TESTS -------------

#[test]
fn test_btree_select_simple() {
    let db_name = "target/btree/mydb_btree_select";
    clear_db(db_name);

    let env = get_test_env();
    let csv_path = &env.numbers_csv;

    let mut db = DatabaseEngine::open(db_name).expect("open DB");

    // Load numbers, set 'number' as primary key
    db.load_csv_pk("numbers", csv_path.to_str().unwrap(), Some("number"), 0).expect("LOAD CSV");
    db.reload_catalog();

    let sql = "SELECT number FROM numbers WHERE number = 90";
    let mydb_res = run_my_db_query(&mut db, sql).expect("mydb query");
    let sqlite_res = run_sqlite_query(csv_path, sql).expect("sqlite query");

    assert!(compare_results(&sqlite_res, &mydb_res));
}

#[test]
fn test_btree_delete() {
    let db_name = "target/btree/mydb_btree_delete";
    clear_db(db_name);

    let env = get_test_env();
    let csv_path = &env.numbers_csv;

    let mut db = DatabaseEngine::open(db_name).expect("open DB");
    db.load_csv_pk("numbers", csv_path.to_str().unwrap(), Some("number"), 0).expect("LOAD CSV");
    db.reload_catalog();

    let delete_sql = "DELETE FROM numbers WHERE number = 42";
    run_my_db_query(&mut db, delete_sql).unwrap();

    let select_sql = "SELECT number FROM numbers ORDER BY number";
    let mydb_res = run_my_db_query(&mut db, select_sql).unwrap();
    
    // For SQLite, re-load and run DELETE
    let conn = Connection::open_in_memory().unwrap();
    load_csv_into_sqlite(&conn, "numbers", csv_path).unwrap();
    conn.execute(delete_sql, []).unwrap();
    let mut stmt = conn.prepare(select_sql).unwrap();
    let sqlite_res: Vec<Vec<String>> = stmt.query_map([], |row| {
        Ok(vec![row.get::<_, i64>(0).unwrap().to_string()])
    }).unwrap().map(|r| r.unwrap()).collect();

    assert!(compare_results(&sqlite_res, &mydb_res));
}

#[test]
fn test_btree_update() {
    let db_name = "target/btree/mydb_btree_update";
    clear_db(db_name);

    let env = get_test_env();
    let csv_path = &env.numbers_csv;

    let mut db = DatabaseEngine::open(db_name).expect("open DB");
    db.load_csv_pk("numbers", csv_path.to_str().unwrap(), Some("number"), 0).expect("LOAD CSV");
    db.reload_catalog();

    let update_sql = "UPDATE numbers SET number = 1234 WHERE number = 10";
    run_my_db_query(&mut db, update_sql).unwrap();

    let select_sql = "SELECT number FROM numbers ORDER BY number";
    let mydb_res = run_my_db_query(&mut db, select_sql).unwrap();

    // SQLite update
    let conn = Connection::open_in_memory().unwrap();
    load_csv_into_sqlite(&conn, "numbers", csv_path).unwrap();
    conn.execute(update_sql, []).unwrap();
    let mut stmt = conn.prepare(select_sql).unwrap();
    let sqlite_res: Vec<Vec<String>> = stmt.query_map([], |row| {
        Ok(vec![row.get::<_, i64>(0).unwrap().to_string()])
    }).unwrap().map(|r| r.unwrap()).collect();

    assert!(compare_results(&sqlite_res, &mydb_res));
}


#[test]
fn test_btree_select_update_delete_routine() {
    let db_name = "target/btree/mydb_multi_query";
    clear_db(db_name);

    let env = get_test_env();
    let csv_path = &env.numbers_csv;

    let mut db = DatabaseEngine::open(db_name).expect("open DB");
    db.load_csv_pk("numbers", csv_path.to_str().unwrap(), None, 0).expect("LOAD CSV");
    db.reload_catalog();

    // 1. Select a value
    let sql1 = "SELECT number FROM numbers WHERE number = 42";
    let res1 = run_my_db_query(&mut db, sql1).expect("mydb select");
    assert_eq!(res1, vec![vec!["42".to_string()]]);

    // 2. Update that value
    let sql2 = "UPDATE numbers SET number = 999 WHERE number = 42";
    let res2 = run_my_db_query(&mut db, sql2).expect("mydb update");
    assert_eq!(res2, vec![vec!["UPDATE OK".to_string()]]);

    // 3. Check updated value
    let sql3 = "SELECT number FROM numbers WHERE number = 999";
    let res3 = run_my_db_query(&mut db, sql3).expect("mydb select after update");
    assert_eq!(res3, vec![vec!["999".to_string()]]);

    // 4. Delete the updated value
    let sql4 = "DELETE FROM numbers WHERE number = 999";
    let res4 = run_my_db_query(&mut db, sql4).expect("mydb delete");
    assert_eq!(res4, vec![vec!["DELETE OK".to_string()]]);

    // 5. Check that the value is gone
    let sql5 = "SELECT number FROM numbers WHERE number = 999";
    let res5 = run_my_db_query(&mut db, sql5).expect("mydb select after delete");
    assert_eq!(res5, vec![] as Vec<Vec<String>>);

    // 6. Insert a new value
    let sql6 = "INSERT INTO numbers (number) VALUES (777)";
    let res6 = run_my_db_query(&mut db, sql6).expect("mydb insert");
    assert_eq!(res6, vec![vec!["INSERT OK".to_string()]]);

    // 7. Final check: is 777 present?
    let sql7 = "SELECT number FROM numbers WHERE number = 777";
    let res7 = run_my_db_query(&mut db, sql7).expect("mydb select after insert");
    assert_eq!(res7, vec![vec!["777".to_string()]]);
}

#[test]
fn test_btree_multi_insert_delete_order() {
    let db_name = "target/btree/mydb_multi_insert_delete";
    clear_db(db_name);

    let env = get_test_env();
    let csv_path = &env.numbers_csv;

    let mut db = DatabaseEngine::open(db_name).expect("open DB");
    db.load_csv_pk("numbers", csv_path.to_str().unwrap(), None, 0).expect("LOAD CSV");
    db.reload_catalog();

    // 1. Insert several new values
    let sql_insert = [
        "INSERT INTO numbers (number) VALUES (2000)",
        "INSERT INTO numbers (number) VALUES (1500)",
        "INSERT INTO numbers (number) VALUES (2500)",
    ];

    for sql in &sql_insert {
        let res = run_my_db_query(&mut db, sql).expect("mydb insert");
        println!("After: `{}` => {:?}", sql, res);
        assert_eq!(res, vec![vec!["INSERT OK".to_string()]]);
    }

    // 2. Select all numbers > 1499, should include 1500, 2000, 2500
    let sql_select = "SELECT number FROM numbers WHERE number > 1499 ORDER BY number";
    let res = run_my_db_query(&mut db, sql_select).expect("mydb select after insert");
    println!("Numbers > 1499: {:?}", res);
    assert_eq!(
        res,
        vec![
            vec!["1500".to_string()],
            vec!["2000".to_string()],
            vec!["2500".to_string()],
        ]
    );

    // 3. Delete 2000 and 1500
    let sql_del_2000 = "DELETE FROM numbers WHERE number = 2000";
    let res_del_2000 = run_my_db_query(&mut db, sql_del_2000).expect("mydb delete 2000");
    println!("Delete 2000: {:?}", res_del_2000);
    assert_eq!(res_del_2000, vec![vec!["DELETE OK".to_string()]]);

    let sql_del_1500 = "DELETE FROM numbers WHERE number = 1500";
    let res_del_1500 = run_my_db_query(&mut db, sql_del_1500).expect("mydb delete 1500");
    println!("Delete 1500: {:?}", res_del_1500);
    assert_eq!(res_del_1500, vec![vec!["DELETE OK".to_string()]]);

    // 4. Check that only 2500 remains among high values
    let sql_select2 = "SELECT number FROM numbers WHERE number > 1499 ORDER BY number";
    let res2 = run_my_db_query(&mut db, sql_select2).expect("mydb select after deletes");
    println!("Numbers > 1499 after deletes: {:?}", res2);
    assert_eq!(res2, vec![vec!["2500".to_string()]]);
}

#[test]
fn test_btree_update_and_revert() {
    let db_name = "target/btree/mydb_update_and_revert";
    clear_db(db_name);

    let env = get_test_env();
    let csv_path = &env.numbers_csv;

    let mut db = DatabaseEngine::open(db_name).expect("open DB");
    db.load_csv_pk("numbers", csv_path.to_str().unwrap(), None, 0).expect("LOAD CSV");
    db.reload_catalog();

    // 1. Change 10 to 555, then back to 10
    let sql_upd_1 = "UPDATE numbers SET number = 555 WHERE number = 10";
    let res_upd_1 = run_my_db_query(&mut db, sql_upd_1).expect("mydb update to 555");
    println!("Update 10 -> 555: {:?}", res_upd_1);
    assert_eq!(res_upd_1, vec![vec!["UPDATE OK".to_string()]]);

    let sql_check_555 = "SELECT number FROM numbers WHERE number = 555";
    let res_555 = run_my_db_query(&mut db, sql_check_555).expect("mydb select 555");
    println!("Check 555 present: {:?}", res_555);
    assert_eq!(res_555, vec![vec!["555".to_string()]]);

    // 2. Change it back
    let sql_upd_2 = "UPDATE numbers SET number = 10 WHERE number = 555";
    let res_upd_2 = run_my_db_query(&mut db, sql_upd_2).expect("mydb update back to 10");
    println!("Update 555 -> 10: {:?}", res_upd_2);
    assert_eq!(res_upd_2, vec![vec!["UPDATE OK".to_string()]]);

    let sql_check_10 = "SELECT number FROM numbers WHERE number = 10";
    let res_10 = run_my_db_query(&mut db, sql_check_10).expect("mydb select 10");
    println!("Check 10 present again: {:?}", res_10);
    assert_eq!(res_10, vec![vec!["10".to_string()]]);

    // 3. Delete 10 for good measure
    let sql_del_10 = "DELETE FROM numbers WHERE number = 10";
    let res_del_10 = run_my_db_query(&mut db, sql_del_10).expect("mydb delete 10");
    println!("Delete 10: {:?}", res_del_10);
    assert_eq!(res_del_10, vec![vec!["DELETE OK".to_string()]]);

    let sql_check_none = "SELECT number FROM numbers WHERE number = 10";
    let res_none = run_my_db_query(&mut db, sql_check_none).expect("mydb select after final delete");
    println!("Check 10 gone: {:?}", res_none);
    assert_eq!(res_none, vec![] as Vec<Vec<String>>);
}


#[test]
fn test_btree_insert_delete_reinsert() {
    let db_name = "target/btree/mydb_insert_delete_reinsert";
    clear_db(db_name);

    let env = get_test_env();
    let csv_path = &env.numbers_csv;

    let mut db = DatabaseEngine::open(db_name).expect("open DB");
    db.load_csv_pk("numbers", csv_path.to_str().unwrap(), None, 0).expect("LOAD CSV");
    db.reload_catalog();

    // 1. Insert a value not present
    let sql_insert = "INSERT INTO numbers (number) VALUES (2024)";
    let res_insert = run_my_db_query(&mut db, sql_insert).expect("mydb insert 2024");
    println!("Insert 2024: {:?}", res_insert);
    assert_eq!(res_insert, vec![vec!["INSERT OK".to_string()]]);

    // 2. Confirm it is present
    let sql_sel = "SELECT number FROM numbers WHERE number = 2024";
    let res_sel = run_my_db_query(&mut db, sql_sel).expect("mydb select 2024");
    println!("Select 2024: {:?}", res_sel);
    assert_eq!(res_sel, vec![vec!["2024".to_string()]]);

    // 3. Delete the value
    let sql_del = "DELETE FROM numbers WHERE number = 2024";
    let res_del = run_my_db_query(&mut db, sql_del).expect("mydb delete 2024");
    println!("Delete 2024: {:?}", res_del);
    assert_eq!(res_del, vec![vec!["DELETE OK".to_string()]]);

    // 4. Confirm it's gone
    let res_sel2 = run_my_db_query(&mut db, sql_sel).expect("mydb select 2024 after delete");
    println!("Select 2024 after delete: {:?}", res_sel2);
    assert_eq!(res_sel2, vec![] as Vec<Vec<String>>);

    // 5. Insert again
    let res_insert2 = run_my_db_query(&mut db, sql_insert).expect("mydb re-insert 2024");
    println!("Re-insert 2024: {:?}", res_insert2);
    assert_eq!(res_insert2, vec![vec!["INSERT OK".to_string()]]);

    // 6. Confirm it's present
    let res_sel3 = run_my_db_query(&mut db, sql_sel).expect("mydb select 2024 after re-insert");
    println!("Select 2024 after re-insert: {:?}", res_sel3);
    assert_eq!(res_sel3, vec![vec!["2024".to_string()]]);
}


#[test]
fn test_btree_update_chain_and_delete() {
    let db_name = "target/btree/mydb_update_chain";
    clear_db(db_name);

    let env = get_test_env();
    let csv_path = &env.numbers_csv;

    let mut db = DatabaseEngine::open(db_name).expect("open DB");
    db.load_csv_pk("numbers", csv_path.to_str().unwrap(), None, 0).expect("LOAD CSV");
    db.reload_catalog();

   
    let res_upd1 = run_my_db_query(&mut db, "UPDATE numbers SET number = 201 WHERE number = 20").expect("upd 20->201");
    println!("Update 20->201: {:?}", res_upd1);
    assert_eq!(res_upd1, vec![vec!["UPDATE OK".to_string()]]);

    let res_upd2 = run_my_db_query(&mut db, "UPDATE numbers SET number = 401 WHERE number = 201").expect("upd 401->201");
    println!("Update 201->401: {:?}", res_upd2);
    assert_eq!(res_upd2, vec![vec!["UPDATE OK".to_string()]]);

  
    let res_sel = run_my_db_query(&mut db, "SELECT number FROM numbers WHERE number = 401").expect("select 401");
    println!("Select 401: {:?}", res_sel);
    assert_eq!(res_sel, vec![vec!["401".to_string()]]);

 
    let res_del = run_my_db_query(&mut db, "DELETE FROM numbers WHERE number = 401").expect("delete 401");
    println!("Delete 401: {:?}", res_del);
    assert_eq!(res_del, vec![vec!["DELETE OK".to_string()]]);

    // 5. Check
    let res_sel2 = run_my_db_query(&mut db, "SELECT number FROM numbers WHERE number = 20")
        .expect("final select");
    println!("Check 20 is gone: {:?}", res_sel2);
    assert_eq!(res_sel2, vec![] as Vec<Vec<String>>);
}

# memtuco

**memtuco** is a toy RDBMS written in Rust — not a competitor to PostgreSQL or SQLite, but a learning-focused project packed with meaningful features.

It’s built using only Rust’s standard library, avoiding the complexity of external dependencies. The result is a minimalistic yet surprisingly capable system, intended more for education and exploration than production use.

![Demo_memtuco](https://github.com/user-attachments/assets/60bbb1c1-1b5c-4671-a4f6-856136564c44)

## Features

-  Persistent on-disk storage for tables and indexes (B-tree)
-  Variable-length data support
-  SQL query parser and executor
-  Query planner visualization via `EXPLAIN`
-  CRUD operations: `CREATE`, `READ`, `UPDATE`, `DELETE`
-  Index creation via `CREATE INDEX` (B-tree)
-  `ORDER BY` implemented with a naive external disk sort
-  `INNER JOIN` (sort-merge), `DISTINCT`, and `GROUP BY` 
-  Expression support, like:

  ```sql
  SELECT (hours * (days + 2))/7
  ```
-  Transactions with isolation and support for `COMMIT` / `ROLLBACK` via journaling
-  Concurrency: multiple readers, single writer
-  Aggregate functions & expressions, like:

  ```sql
  SELECT (MAX(age) - MIN(age)) / AVG(age)
  ```
- LOAD & EXPORT: import/export of .csv files with auto parsing of column type
- TCP Client/Server mode ( very basic)

## LOAD and EXPORT for .csv File
For load and export .csv files

### LOAD command

```sql
LOAD <tbl> <file.csv> <force> [pk]
-- force=0: infer column type
-- force=1: treat all columns as VARCHAR
-- pk: optional, set primary key column
```

### EXPORT command
```sql
EXPORT <file.csv> <table> <query>
-- Exports results of <query> on <table> to <file.csv>
```

### Load example
 ```sql
  LOAD tablename myfile.csv 0 <pk>
```

### Export example
 ```sql
 EXPORT myexport.csv SELECT name, days*hours FROM mytable
 ```

## Architecture Notes

The project includes a module named `bintuco.rs` for (de)serializing structs. While not optimized for performance, it serves an educational purpose and works as expected.

The TCP server component is currently very basic and still under development. For now, the project can interface with clients in a basic way. (See Below)

## Testing

Tests have been written to compare query results against `runsqlite`, and can be found in the `tests/` directory.

## Getting Started

Clone the repository, then run:

```bash
cargo run <dbname> <port>
```

Once running, you can connect using a TCP client, 127.0.0.1 is the default address. In the client folder there is a very basic Client that is being used during development. For running it

```bash
cargo run -p client <port>
```
---

## Dockerfile

I write a simple Dockerfile for using memtuco inside a container. So:

### Build
```bash
docker build -t memtuco .
```
### Run
```bash
docker run --rm -p <host_port>:<container_port> memtuco <my_db_name> <container_port> <address>
```

### Example Run with mounted folder (SERVER)
```bash
docker run --rm -p 8000:7878 -v /home/usr/data:/data memtuco /data/mydb 7878 0.0.0.0
```

### (CLIENT)
```bash
cargo run -p client 8000
```

This project is an educational RDBMS, currently a work in progress. Feel free to clone it, explore the code, and use it for learning or experimentation.

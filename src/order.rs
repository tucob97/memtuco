//! Order by Iterator implementation
/// How we can sort without collecting all rows in memory?
/// So the idea is to create "Runs" -> group of rows internally
/// sorted, and stored to a file. So we create a in memory
/// Heap, that store the rows in memory. If we pop
/// a row from the heap, we fetch next row from the same Run

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::page::{Row, TableSchema, SqlValue};
use crate::planner::Expression;
use crate::dbengine::{to_f64_or_nan, eval_expr};
use crate::bintuco;
use crate::constant::CHUNK_SIZE_K_ORDER;
use crate::dbengine::RowIterator;
use crate::dbengine::SchemaRowIter;
use crate::dberror::DbError;
use crate::tokenizer::ColumnRef;
use std::collections::VecDeque;



/// /// The `order.rs` module implements one of the most critical components for handling
/// /// large datasets: an external merge sort. This allows the database to sort result
/// /// sets that are larger than the available RAM by using temporary disk space.
/// /// This same sorting mechanism is the foundation for the `MERGE JOIN` operator.
/// 
///
/// /// The `RowSorter` implements a multi-phase sort algorithm.
///
/// /// 1.  Create Runs: The input iterator is consumed in chunks (e.g., a few
/// ///     thousand rows at a time). Each chunk is sorted in memory and then written
/// ///     to a temporary file on disk. Each of these sorted temporary files is
/// ///     called a "run".
///
/// /// 2.  Merge Runs: After all runs have been created, a K-way merge is performed.
/// ///     A `BinaryHeap` (acting as a min-heap) is used to manage the merge. The
/// ///     heap stores the *next* row from each of the K runs. To get the next row
/// ///     in the final sorted sequence, we simply pop the smallest item from the heap.
/// ///     When a row is popped, we fetch the next row from the same run it came from
/// ///     and insert it into the heap. This process continues until all runs are
/// ///     exhausted.
///
/// 
/// 
/// /// /// Visualizing the Merge Phase:
/// ///
/// /// /// +-------------------------------------------------------------------+
/// /// /// |                        Final Sorted Output                        |
/// /// /// +---------------------------------^---------------------------------+
/// /// ///                                   | Pop smallest row
/// /// /// +---------------------------------+---------------------------------+
/// /// /// |                 Min-Heap (in memory)                              |
/// /// /// | Stores the next available row from each run.                      |
/// /// /// |                                                                   |
/// /// /// |  HeapItem { row: [Eve, 25], run: 2 }                              |
/// /// /// |  HeapItem { row: [Frank, 30], run: 0 }                            |
/// /// /// |  HeapItem { row: [Grace, 35], run: 1 }                            |
/// /// /// +------------------^----------------^----------------^--------------+
/// /// ///                  | Fetch next     | Fetch next     | Fetch next
/// /// /// +----------------|----------------|----------------|----------------+
/// /// /// | Run 0 (on disk)| Run 1 (on disk)| Run 2 (on disk)| ... (K Runs)   |
/// /// /// | [Alice, 20]    | [Bob, 22]      | [Charlie, 21]  |                |
/// /// /// | [Frank, 30]    | [Grace, 35]    | [Eve, 25]      |                |
/// /// /// | ...            | ...            | ...            |                |
/// /// /// +----------------+----------------+----------------+----------------+
/// 
/// Otherwise order is reversed for DESC

pub struct RowSorter {
    schema: TableSchema,
    ordering: Vec<(Expression, bool)>,
    chunk_size: usize,
    id_temp: u128,
    temp_dir_path: PathBuf,
}

impl RowSorter {
    pub fn new(schema: TableSchema, ordering: Vec<(Expression, bool)>) -> Self {
        let id_temp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| UNIX_EPOCH.elapsed().unwrap_or_default())
            .as_nanos();
        let temp_dir_path = PathBuf::from("tucotemp").join(format!("query_{}", id_temp));
        Self { schema, ordering, chunk_size: CHUNK_SIZE_K_ORDER, id_temp, temp_dir_path }
    }

    pub fn sort<'a, I>(self, input: I) -> Result<Box<dyn RowIterator + 'a>, DbError>
    where
        I: Iterator<Item = Row> + 'a,
    {
        std::fs::create_dir_all(&self.temp_dir_path).map_err(DbError::Io)?;
        let files = self.sort_to_disk(input)?;
        let iter = self.merge_sorted_chunks(files)?;
        Ok(Box::new(SchemaRowIter { schema: self.schema.clone(), inner: iter }))
    }

    fn sort_to_disk<I>(&self, mut input: I) -> Result<Vec<PathBuf>, DbError>
    where
        I: Iterator<Item = Row>,
    {
        let mut out = Vec::new();
        loop {
            let mut rows: Vec<Row> = input.by_ref().take(self.chunk_size).collect();
            if rows.is_empty() { break; }
            out.push(self.write_sorted_chunk(&mut rows)?);
        }
        Ok(out)
    }

    fn unique_temp_path(&self) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| UNIX_EPOCH.elapsed().unwrap_or_default())
            .as_nanos();
        self.temp_dir_path.join(format!("row_sort_{}.tmp", nanos))
    }

    fn write_sorted_chunk(&self, rows: &mut Vec<Row>) -> Result<PathBuf, DbError> {
        if rows.is_empty() {
            return Err(DbError::Order("Empty chunk".into()));
        }
        let schema = self.schema.clone();
        let ordering = self.ordering.clone();
        rows.sort_by(|a, b| compare_rows(a, b, &schema, &ordering));

        let path = self.unique_temp_path();
        println!("Writing chunk ({} rows) to {:?}", rows.len(), path);

        let file = OpenOptions::new().create(true).write(true).truncate(true).open(&path)?;
        let mut w = BufWriter::new(file);
        for row in rows.drain(..) {
            let enc = bintuco::encode_to_vec(&row).map_err(|e| DbError::Bintuco(e.to_string()))?;
            w.write_all(&(enc.len() as u32).to_be_bytes())?;
            w.write_all(&enc)?;
        }
        w.flush()?;
        Ok(path)
    }

    fn merge_sorted_chunks<'a>(
        &self,
        files: Vec<PathBuf>
    ) -> Result<Box<dyn Iterator<Item = Row> + 'a>, DbError> {
        let schema = self.schema.clone();
        let ordering = self.ordering.clone();

        // Open readers
        let mut readers: Vec<BufReader<File>> = files
            .into_iter()
            .map(|p| BufReader::new(File::open(p).unwrap()))
            .collect();

            let mut heap = BinaryHeap::new();
            for (i, rdr) in readers.iter_mut().enumerate() {
                if let Some(row) = read_next_row(rdr)? {
                    // 1) extract the key while `row` is still available
                    let sort_key = extract_sort_key(&row, &schema, &ordering);
            
                    // 2) now push the HeapItem, moving `row` exactly once
                    heap.push(HeapItem {
                        row,
                        source: i,
                        sort_key,
                    });
                }
            }

        // The actual merge iterator
        let mut readers = readers; // move into closure
        let iter = std::iter::from_fn(move || {
            if let Some(item) = heap.pop() {
                let idx = item.source;
                let out_row = item.row;
                if let Ok(Some(next)) = read_next_row(&mut readers[idx]) {
                    heap.push(HeapItem {
                        row: next.clone(),
                        source: idx,
                        sort_key: extract_sort_key(&next, &schema, &ordering),
                    });
                }
                Some(out_row)
            } else {
                None
            }
        });

        Ok(Box::new(iter))
    }
}

impl Drop for RowSorter {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.temp_dir_path);
    }
}

// Wrapper to encode ASC vs DESC per column
#[derive(Clone, Debug)]
enum KeyDirection {
    Asc(Option<SqlValue>),
    Desc(Option<SqlValue>),
}

// Manual PartialEq/Eq
impl PartialEq for KeyDirection {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (KeyDirection::Asc(a),  KeyDirection::Asc(b))  => a == b,
            (KeyDirection::Desc(a), KeyDirection::Desc(b)) => a == b,
            _ => false,
        }
    }
}
impl Eq for KeyDirection {}

// Total ordering using underlying PartialOrd
impl PartialOrd for KeyDirection {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (KeyDirection::Asc(a),  KeyDirection::Asc(b))  => a.partial_cmp(b),
            (KeyDirection::Desc(a), KeyDirection::Desc(b)) => b.partial_cmp(a),
            (KeyDirection::Asc(_),  KeyDirection::Desc(_)) => Some(Ordering::Less),
            (KeyDirection::Desc(_), KeyDirection::Asc(_))  => Some(Ordering::Greater),
        }
    }
}
impl Ord for KeyDirection {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

fn extract_sort_key(
    row: &Row,
    schema: &TableSchema,
    ordering: &[(Expression, bool)],
) -> Vec<KeyDirection> {
    ordering
        .iter()
        .map(|(expr, asc)| {
            let v = eval_expr(row, schema, expr).ok();
            if *asc { KeyDirection::Asc(v) } else { KeyDirection::Desc(v) }
        })
        .collect()
}

fn compare_rows(
    a: &Row,
    b: &Row,
    schema: &TableSchema,
    ordering: &[(Expression, bool)],
) -> Ordering {
    for (expr, asc) in ordering {
        let va = eval_expr(a, schema, expr).ok();
        let vb = eval_expr(b, schema, expr).ok();
        if let (Some(va), Some(vb)) = (va, vb) {
            let ord = va.partial_cmp(&vb).unwrap_or(Ordering::Equal);
            if ord != Ordering::Equal {
                return if *asc { ord } else { ord.reverse() };
            }
        }
    }
    Ordering::Equal
}

struct HeapItem {
    row: Row,
    source: usize,
    sort_key: Vec<KeyDirection>,
}

// Manual PartialEq/Eq for HeapItem
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_key == other.sort_key
    }
}
impl Eq for HeapItem {}

// Reverse compare for min-heap behavior on a max-heap
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // BinaryHeap is max-heap, so reverse to get min-heap
        Some(other.sort_key.cmp(&self.sort_key))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.sort_key.cmp(&self.sort_key)
    }
}

fn read_next_row(reader: &mut BufReader<File>) -> Result<Option<Row>, DbError> {
    let mut len = [0u8; 4];
    if reader.read_exact(&mut len).is_err() {
        return Ok(None);
    }
    let size = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; size];
    if reader.read_exact(&mut buf).is_err() {
        return Ok(None);
    }
    bintuco::decode_from_slice::<Row>(&buf)
        .map(|(r, _)| Some(r))
        .map_err(|e| DbError::Bintuco(e.to_string()))
}



/// SORT-MERGE JOIN ITERATOR
/// Implementation of INNER JOIN 
/// 
/// ### Sort-Merge Join (`merge_join`)
///
/// /// The `merge_join` function provides a highly efficient way to perform an `INNER JOIN`
/// /// on two large datasets. It leverages the `RowSorter` to ensure both its left and
/// /// right inputs are sorted on their respective join keys.
///
/// /// Algorithm:
/// ///
/// /// 1.  Independently sort the left and right input iterators using `RowSorter`.
/// /// 2.  Iterate through the sorted left input. For each left row:
/// ///     a. Advance the right iterator past any rows with a smaller join key.
/// ///     b. Collect all subsequent rows from the right iterator that have an *equal*
/// ///        join key. This is the "group" of matching right rows.
/// ///     c. Emit a cross-product between the current left row and every row in the
/// ///        collected group of matching right rows.
/// /// 3.  Because both inputs are sorted, this process only requires a single pass
/// ///     through each dataset after the initial sort, making it very efficient.



pub fn merge_join<'a>(
    left_input: Box<dyn RowIterator + 'a>,
    right_input: Box<dyn RowIterator + 'a>,
    left_schema: TableSchema,
    right_schema: TableSchema,
    on_left: ColumnRef,
    on_right: ColumnRef,
) -> Result<Box<dyn RowIterator + 'a>, DbError> { // Changed return type to Result
    let left_sorter = RowSorter::new(
        left_schema.clone(),
        vec![(crate::planner::Expression::Column(on_left.clone()), true)],
    );
    let right_sorter = RowSorter::new(
        right_schema.clone(),
        vec![(crate::planner::Expression::Column(on_right.clone()), true)],
    );

    let mut left_iter = left_sorter.sort(left_input)?.peekable(); // Propagate error
    let mut right_iter = right_sorter.sort(right_input)?.peekable(); // Propagate error

    let left_idx = left_schema
        .name_col
        .iter()
        .position(|c| c == &on_left.column)
        .ok_or_else(|| DbError::Order(format!("Left join column '{}' not found", on_left.column)))?; // Return error
    let right_idx = right_schema
        .name_col
        .iter()
        .position(|c| c == &on_right.column)
        .ok_or_else(|| DbError::Order(format!("Right join column '{}' not found", on_right.column)))?; // Return error

    let mut result_buffer: std::collections::VecDeque<Row> = std::collections::VecDeque::new();
    let mut matched_rights: Vec<Row> = Vec::new();
    let mut current_right_key: Option<SqlValue> = None;

    let mut current_left: Option<Row> = None;

    let join_iter = std::iter::from_fn(move || {
        loop {
            if let Some(row) = result_buffer.pop_front() {
                eprintln!("Emitting joined row: {:?}", row);
                return Some(row);
            }

            // Fetch next left row
            if current_left.is_none() {
                current_left = left_iter.next();
                if let Some(ref row) = current_left {
                    eprintln!("Next left row: {:?}", row);
                } else {
                    eprintln!("No more left rows");
                }
            }
            let left_row = match current_left.take() {
                Some(row) => row,
                None => return None, // done
            };
            let left_key = &left_row.values[left_idx];
            eprintln!("Left key: {:?}", left_key);

            // If key changed, load new matching rights
            if current_right_key.as_ref() != Some(left_key) {
                eprintln!("Left key changed or first time, resetting matched rights");
                matched_rights.clear();
                current_right_key = None;

                while let Some(peek_row) = right_iter.peek() {
                    let right_key = &peek_row.values[right_idx];
                    eprintln!("Comparing left key {:?} with right key {:?}", left_key, right_key);
                    match left_key.partial_cmp(right_key) {
                        Some(std::cmp::Ordering::Less) => {
                            eprintln!("Left key < right key, break matching loop");
                            break;
                        }
                        Some(std::cmp::Ordering::Greater) => {
                            eprintln!("Left key > right key, skipping right row");
                            right_iter.next();
                        }
                        Some(std::cmp::Ordering::Equal) => {
                            let matched = right_iter.next().unwrap(); // Unwrap should be safe here since we peeked
                            eprintln!("Match found: right row {:?}", matched);
                            matched_rights.push(matched);
                            current_right_key = Some(left_key.clone());
                        }
                        None => {
                            // Incomparable keys during join are usually treated as no match.
                            // This scenario might indicate bad data or a type mismatch.
                            eprintln!("Join key comparison failed (incomparable values): {:?} vs {:?}", left_key, right_key);
                            // To avoid panic, just break and don't match.
                            break; 
                        }
                    }
                }
            }

            if matched_rights.is_empty() {
                eprintln!("No matches found for left key {:?}", left_key);
            } else {
                eprintln!("Matches found for left key: {} right rows", matched_rights.len());
            }

            // Emit cross-product for this left_row
            for right_row in &matched_rights {
                let mut combined = left_row.values.clone();
                combined.extend_from_slice(&right_row.values);
                eprintln!("Buffering joined row: {:?}", combined);
                result_buffer.push_back(Row { values: combined });
            }
        }
    });

    let output_schema = combine_schemas(&left_schema, &right_schema, Some(&left_schema.name), Some(&right_schema.name));

    Ok(Box::new(SchemaRowIter {
        schema: output_schema,
        inner: join_iter,
    }))
}

/// Joined table has rows combined from the two table
pub fn combine_schemas(
    left: &TableSchema,
    right: &TableSchema,
    left_table: Option<&str>,
    right_table: Option<&str>,
) -> TableSchema {
    let mut combined_cols = Vec::new();
    let mut combined_types = Vec::new();

    for (col, ty) in left.name_col.iter().zip(left.columns.iter()) {
        let name = if let Some(tbl) = left_table {
            format!("{}.{}", tbl, col)
        } else {
            col.clone()
        };
        combined_cols.push(name);
        combined_types.push(ty.clone());  // clone here
    }
    
    for (col, ty) in right.name_col.iter().zip(right.columns.iter()) {
        let name = if let Some(tbl) = right_table {
            format!("{}.{}", tbl, col)
        } else {
            col.clone()
        };
        combined_cols.push(name);
        combined_types.push(ty.clone());  // clone here
    }
    

    TableSchema {
        name: "__virtual__".to_string(),
        name_col: combined_cols,
        columns: combined_types,
        primary_key: None,
    }
}



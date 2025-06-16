//! DatabaseEngine is the in-Memory orchestrator of the database
//! So what it does is to execute query, so is the real low-level
//! manager for all the execution

// File: dbengine.rs

use crate::btree::*;
use crate::page::*;
use crate::planner::{PlanNode, Expression, Value};
use crate::tokenizer::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions, remove_file};
use std::io::{Read, Seek, SeekFrom, Write, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use crate::transaction::*;
use std::error::Error;
use std::thread;
use crate::bintuco;
use std::fmt;
use std::io;
use crate::constant::{PAGE_CAPACITY,PAGE_SIZE, SAMPLE_SIZE, CHUNK_CSV_SIZE};
use std::io::BufWriter;
use crate::dberror::DbError;
use std::str::FromStr;
use crate::planner::*;


// ══════════════════════════════════════ CATALOG ══════════════════════════════════════

/// Catalog is a "catalog" so contains information in memory
/// of the current database (e.g. which indexes, table name ecc...)
#[derive(Clone)]
pub struct Catalog {
    /// maps table name → set of indexed columns
    pub table_indexes: HashMap<String, HashSet<String>>,
}

impl Catalog {
    pub fn from_page(page: &DatabasePage) -> Self {
        let mut table_indexes = HashMap::new();
        for table_root in &page.all_tableroot {
            let idx_cols = table_root
                .index_vec
                .iter()
                .map(|btree| btree.key_name.clone())
                .collect::<HashSet<_>>();
            table_indexes.insert(table_root.tb_schema.name.clone(), idx_cols);
        }
        Catalog { table_indexes }
    }

    pub fn index_exists(&self, table: &str, col: &str) -> bool {
        self.table_indexes
            .get(table)
            .map_or(false, |cols| cols.contains(col))
    }
    
    pub fn contains_table(&self, table: &str) -> bool {
        self.table_indexes.contains_key(table)
    }

}


impl fmt::Display for Catalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Catalog:")?;
        for (table, idx_cols) in &self.table_indexes {
            write!(f, "  {} → ", table)?;
            if idx_cols.is_empty() {
                writeln!(f, "<no indexes>")?;
            } else {
                let mut first = true;
                for col in idx_cols {
                    if !first { write!(f, ", ")?; }
                    write!(f, "{}", col)?;
                    first = false;
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}


// ══════════════════════════════════════ File Handle ══════════════════════════════════════

/// Connected to StorageManager, for managing access to table and index files
#[derive(Clone, Debug)]
pub struct FileHandle {
    inner: Arc<RwLock<File>>,
}

impl FileHandle {
    pub fn open(path: PathBuf) -> Result<Self, DbError> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        Ok(FileHandle {
            inner: Arc::new(RwLock::new(f)),
        })
    }

    pub fn read_at(&self, pos: u64, buf: &mut [u8]) -> Result<(), DbError> {
        let mut guard = self.inner.write().map_err(|e| DbError::Other(format!("Failed to acquire write lock on FileHandle: {}", e)))?;
        guard.seek(SeekFrom::Start(pos))?;
        guard.read_exact(buf)?;
        Ok(())
    }

    pub fn write_at(&self, pos: u64, buf: &[u8]) -> Result<(), DbError> {
        let mut guard = self.inner.write().map_err(|e| DbError::Other(format!("Failed to acquire write lock on FileHandle: {}", e)))?;
        guard.seek(SeekFrom::Start(pos))?;
        guard.write_all(buf)?;
        guard.flush()?;
        Ok(())
    }

    pub fn metadata(&self) -> Result<std::fs::Metadata, DbError> {
        let guard = self.inner.read().map_err(|e| DbError::Other(format!("Failed to acquire read lock on FileHandle: {}", e)))?;
        Ok(guard.metadata()?)
    }

    pub fn set_len(&self, new_len: u64) -> Result<(), DbError> {
        let mut guard = self.inner.write().map_err(|e| DbError::Other(format!("Failed to acquire write lock on FileHandle: {}", e)))?;
        guard.set_len(new_len)?;
        Ok(())
    }

    pub fn file_len(&self) -> Result<u64, DbError> {
        let guard = self.inner.read().map_err(|e| DbError::Other(format!("Failed to acquire read lock on FileHandle: {}", e)))?;
        Ok(guard.metadata()?.len())
    }

    pub fn sync_all(&self) -> Result<(), DbError> {
        let guard = self.inner.read().map_err(|e| DbError::Other(format!("Failed to acquire read lock on FileHandle: {}", e)))?;
        guard.sync_all()?;
        Ok(())
    }

    pub fn sync_data(&self) -> Result<(), DbError> {
        let guard = self.inner.read().map_err(|e| DbError::Other(format!("Failed to acquire read lock on FileHandle: {}", e)))?;
        guard.sync_data()?;
        Ok(())
    }
}

// ══════════════════════════════════════ STORAGE MANAGER ══════════════════════════════════════

/// The struct responsible for read & write files. So thanks to that we can 
/// build the journal, because it store each page that it is supposed
/// to write
#[derive(Debug)]
pub struct StorageManager {
    handles: HashMap<String, FileHandle>,
    base_dir: PathBuf,
    root_journal: Option<FileHandle>,
    journal: Option<FileHandle>,
    journaled_pages: Mutex<HashSet<(String, u64)>>,
}

impl fmt::Display for StorageManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "StorageManager at: {}", self.base_dir.display())?;
        writeln!(f, "Registered files:")?;
        for key in self.handles.keys() {
            writeln!(f, "  - {}", key)?;
        }
        Ok(())
    }
}

impl StorageManager {
    pub fn new(base_dir: PathBuf) -> Self {
        StorageManager {
            handles: HashMap::new(),
            base_dir,
            root_journal: None,
            journal: None,
            journaled_pages: Mutex::new(HashSet::new()),
        }
    }

    /// Register a new file (e.g., a table file or a B‐Tree index file).
    pub fn register_file(&mut self, logical_name: &str) -> Result<FileHandle, DbError> {
        if let Some(h) = self.handles.get(logical_name) {
            return Ok(h.clone());
        }
        let mut path = self.base_dir.clone();
        path.push(logical_name);
        let handle = FileHandle::open(path)?;
        self.handles.insert(logical_name.to_string(), handle.clone());
        Ok(handle)
    }

    pub fn get_handle(&self, logical_name: &str) -> Option<FileHandle> {
        self.handles.get(logical_name).cloned()
    }

    /// Internally: append a raw‐byte entry to the journal.
    fn append_to_journal(&self, data: &[u8]) -> Result<(), DbError> {
        let jh = self.journal.as_ref().ok_or_else(|| DbError::DbEngine("Journal handle not found".to_string()))?;
        let mut guard = jh.inner.write().map_err(|e| DbError::Other(format!("Failed to acquire write lock on journal: {}", e)))?;
        guard.seek(SeekFrom::End(0))?;
        guard.write_all(data)?;
        guard.flush()?;
        Ok(())
    }

    /// When no transaction is active, this is a no‐op. If we are in a transaction, append the old page first.
    pub fn write_page(&self, logical_name: &str, pos: u64, buf: &[u8]) -> Result<(), DbError> {

        if self.journal.is_some() {
            let page_number = pos / PAGE_SIZE as u64;
            let key = (logical_name.to_string(), page_number);
            let mut seen_pages = self.journaled_pages.lock().map_err(|e| DbError::Other(format!("Failed to acquire lock on journaled_pages: {}", e)))?;

            if !seen_pages.contains(&key) {
                let handle = self.get_handle(logical_name).ok_or_else(|| DbError::DbEngine(format!("File '{}' must be registered", logical_name)))?;
                let mut orig = vec![0u8; PAGE_SIZE];
                let file_len = handle.file_len()? as i64;
                if file_len >= (pos as i64 + PAGE_SIZE as i64) {
                    handle.read_at(pos, &mut orig)?;
                } else {
                    if file_len > (pos as i64) {
                        let valid = (file_len - (pos as i64)) as usize;
                        let mut tmp = vec![0u8; valid];
                        handle.read_at(pos, &mut tmp)?;
                        orig[..valid].copy_from_slice(&tmp);
                    }
                }

                let name_bytes = logical_name.as_bytes();
                let name_len = (name_bytes.len() as u32).to_be_bytes();
                let pos_be = pos.to_be_bytes();

                let mut entry = Vec::new();
                entry.extend_from_slice(&name_len);
                entry.extend_from_slice(name_bytes);
                entry.extend_from_slice(&pos_be);
                entry.extend_from_slice(&orig);

                self.append_to_journal(&entry)?;
                seen_pages.insert(key);
            }
        }

        let handle = self.get_handle(logical_name).ok_or_else(|| DbError::DbEngine(format!("File '{}' must be registered", logical_name)))?;
        handle.write_at(pos, buf)
    }

    pub fn commit_journal(&mut self) -> Result<(), DbError> {
        if self.journal.take().is_some() {
            self.journaled_pages.lock().map_err(|e| DbError::Other(format!("Failed to acquire lock on journaled_pages: {}", e)))?.clear();
            let journal_path = self.base_dir.join("journal.dat");
            if journal_path.exists() {
                std::fs::remove_file(&journal_path)?;
            }
        }
        Ok(())
    }

    pub fn rollback_journal(&mut self) -> Result<(), DbError> {

        if self.journal.is_none() {
            return Ok(());
        }
        self.journal = None;

        let journal_path = self.base_dir.join("journal.dat");
        if !journal_path.exists() {
            return Ok(());
        }

        let file = File::open(&journal_path)?;
        let mut reader = BufReader::new(file);

        loop {
            let mut name_len_buf = [0u8; 4];
            match reader.read_exact(&mut name_len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let name_len = u32::from_be_bytes(name_len_buf) as usize;

            let mut name_bytes = vec![0u8; name_len];
            reader.read_exact(&mut name_bytes)?;
            let name_str = String::from_utf8(name_bytes)
                .map_err(|e| DbError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())))?;

            let mut pos_buf = [0u8; 8];
            reader.read_exact(&mut pos_buf)?;
            let pos = u64::from_be_bytes(pos_buf);

            let mut page_buf = vec![0u8; PAGE_SIZE];
            reader.read_exact(&mut page_buf)?;

            let handle = self
                .get_handle(&name_str)
                .ok_or_else(|| {
                    DbError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("File '{}' not registered for rollback", name_str),
                    ))
                })?;
            handle.write_at(pos, &page_buf)?;
        }

        fs::remove_file(&journal_path)?;
        self.journaled_pages.lock().map_err(|e| DbError::Other(format!("Failed to acquire lock on journaled_pages: {}", e)))?.clear();
        Ok(())
    }

    /// Begin journaling for a new write transaction.
    pub fn start_journal(&mut self) -> Result<(), DbError> {
        let journal_path = self.base_dir.join("journal.dat");
        if journal_path.exists() {
            fs::remove_file(&journal_path)?;
        }
        let journal_handle = FileHandle::open(journal_path)?;
        self.journal = Some(journal_handle);
        self.journaled_pages.lock().map_err(|e| DbError::Other(format!("Failed to acquire lock on journaled_pages: {}", e)))?.clear();
        Ok(())
    }

    /// Start a root‐level journal (for write transactions).
    pub fn start_transaction(&mut self) -> Result<(), DbError> {

        let folder_name = self
            .base_dir
            .file_name()
            .ok_or_else(|| DbError::DbEngine("base_dir must end with a folder name".to_string()))?
            .to_string_lossy()
            .to_string();
        let root_logical = format!("{}.tucodb", folder_name);

        let root_path = self.base_dir.join(&root_logical);
        let mut on_disk_root = std::fs::File::open(&root_path)?;
        let mut buf = Vec::new();
        on_disk_root.read_to_end(&mut buf)?;

        let journal_path = self.base_dir.join("root_journal.dat");
        let journal_handle = self.register_file("root_journal.dat")?;
        journal_handle.set_len(0)?;
        journal_handle.write_at(0, &buf)?;
        journal_handle.sync_all()?;

        self.root_journal = Some(journal_handle);
        self.start_journal()?;
        Ok(())
    }

    /// Commits the root file by overwriting `<folder>.tucodb` and deleting `root_journal.dat`.
    pub fn commit_root(&mut self, folder_name: &str, data: &[u8]) -> Result<(), DbError> {
        let logical = format!("{}", folder_name);
        let root_path = self.base_dir.join(&logical);

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&root_path)?;
        file.write_all(data)?;
        file.sync_all()?;

        if self.root_journal.take().is_some() {}
        let journal_path = self.base_dir.join("root_journal.dat");
        if journal_path.exists() {
            fs::remove_file(journal_path)?;
        }
        Ok(())
    }

    /// Roll back the root by reloading from “root_journal.dat”.
    pub fn get_root_file_path(&self) -> std::path::PathBuf {
        // This function as provided doesn't actually get the path to the root file
        // dynamically, it returns a hardcoded placeholder.
        // If it was meant to provide the actual path, it would need access to self.db_path or similar.
        Path::new("path/to/root/file").to_path_buf() 
    }

    /// Roll back the root by reloading from “root_journal.dat” and overwriting `<folder_name>.tucodb`.
    pub fn root_recovery(&mut self, folder_name: &str) -> Result<(), DbError> {
        let logical = format!("{}", folder_name); // This logical name might be incorrect based on usage in `open`
        eprintln!("Root recovery name {:}" , logical);
        let root_path = self.base_dir.join(&logical);

        // 1. Ensure “root_journal.dat” exists on disk.
        let root_journal_path = self.base_dir.join("root_journal.dat");
        if !root_journal_path.exists() {
            return Ok(());
        }

        // 2. Register “root_journal.dat” if not already registered.
        let logical_journal = "root_journal.dat";
        if self.get_handle(logical_journal).is_none() {
            self.register_file(logical_journal)?;
        }
        let journal_handle = self
            .get_handle(logical_journal)
            .ok_or_else(|| DbError::DbEngine("root_recovery: just registered root_journal.dat; handle must exist".to_string()))?;

        // 3. Register/open the logical file (“<folder_name>.tucodb”) if not already registered.
        if self.get_handle(&logical).is_none() {
            self.register_file(&logical)?;
        }
        let logical_handle = self
            .get_handle(&logical)
            .ok_or_else(|| DbError::DbEngine("root_recovery: just registered logical file; handle must exist".to_string()))?;

        // 4. Check lengths: if either the journal is empty or the logical file is empty, do nothing.
        let journal_len = journal_handle.file_len()? as usize;
        let logical_len = logical_handle.file_len()? as usize;
        if journal_len == 0 || logical_len == 0 {
            // Nothing to replay if either file is empty
            return Ok(());
        } 

        // 5. Read the entire journal into memory via FileHandle::read_at.
        let mut journal_bytes = vec![0u8; journal_len];
        journal_handle.read_at(0, &mut journal_bytes)?;

        // 6. Overwrite (truncate) the logical file:
        //    a) Set length to 0 so we can rewrite from scratch.
        logical_handle.set_len(0)?;
        //    b) Write the journal contents back at offset 0.
        logical_handle.write_at(0, &journal_bytes)?;
        //    c) Sync data to disk (optional, but ensures durability).
        logical_handle.sync_all()?;

        // 7. Truncate the journal (set its length to 0) so it won’t be replayed again.
        journal_handle.set_len(0)?;
        journal_handle.sync_all()?;

        Ok(())
    }
}

// ══════════════════════════════════════ DATABASE ENGINE ══════════════════════════════════════

/// Is the in-Memory orchestrator for the database
/// run and execute query, store metadata, and is responsible
/// for commit & rollback operation
#[derive()]
pub struct DatabaseEngine {
    pub db_folder: PathBuf,
    pub db_path: String, // Represents the logical name of the database root file (e.g., "mydb.tucodb")
    pub db_page: DatabasePage,
    pub is_writing: bool,
    pub catalog: Catalog,
    pub storage_manager: StorageManager,
}

impl DatabaseEngine {
    pub fn print_storage(&self) {
        println!("{:?}", self.storage_manager);
    }
    pub fn print_debug_btree(&self){
        for table_root in &self.db_page.all_tableroot {
            // grab the table name once
            let table_name = &table_root.tb_schema.name;
            for btree in &table_root.index_vec {
                // header so you can tell them apart
                println!("--- Debug BTree for index '{}' on table '{}' ---",
                         btree.key_name,
                         table_name);
                // ...and then print every node in it
                btree.debug_print_all_nodes(&self.storage_manager);
            }
        }
    }

    pub fn root_recovery(&mut self) -> Result<(), DbError> {
        let folder = &self.db_folder;

        let root_journal_path = folder.join("root_journal.dat");
        if root_journal_path.exists() {
            let logical = "root_journal.dat";
            if self.storage_manager.get_handle(logical).is_none() {
                self.storage_manager.register_file(logical)?;
            }
            let handle = self.storage_manager.get_handle(&logical)
                .ok_or_else(|| DbError::DbEngine(format!("Failed to get handle for {}", logical)))?;
            let file_len = handle.file_len()? as usize;
            if file_len > 0 {
                self.rollback_root()?;
            }
        }
        Ok(())
    }

    pub fn recovery(&mut self) -> Result<(), DbError> {
        let folder = &self.db_folder;

        let journal_path = folder.join("journal.dat");
        if journal_path.exists() {
            eprintln!("Recovery Journal exitst");
            let logical = "journal.dat";
            if self.storage_manager.get_handle(logical).is_none() {
                self.storage_manager.register_file(logical)?;
            }
            let handle = self.storage_manager.get_handle(&logical)
                .ok_or_else(|| DbError::DbEngine(format!("Failed to get handle for {}", logical)))?;
            let file_len = handle.file_len()? as usize;
            if file_len > 0 {
                self.storage_manager.rollback_journal()?;
            }
        }

        let root_journal_path = folder.join("root_journal.dat");
        if root_journal_path.exists() {
            eprintln!("Recovery Root-Journal exitst");
            let logical = "root_journal.dat";
            if self.storage_manager.get_handle(logical).is_none() {
                self.storage_manager.register_file(logical)?;
            }
            let handle = self.storage_manager.get_handle(&logical)
                .ok_or_else(|| DbError::DbEngine(format!("Failed to get handle for {}", logical)))?;
            let file_len = handle.file_len()? as usize;
            if file_len > 0 {
                self.rollback_root()?;
            }
        }

        // --- START OF THE MEMORY REFRESH ---
        // Here we explicitly refresh the StorageManager's in-memory state
        // to match the now-restored DatabasePage state.

        // Create a new, empty map for the file handles.
        let mut new_handles = HashMap::new();

        // Add back only the essential handles for the database itself.
        if let Some(h) = self.storage_manager.handles.get(&self.db_path) {
            new_handles.insert(self.db_path.clone(), h.clone());
        }
        if let Some(h) = self.storage_manager.handles.get("root_journal.dat") {
            new_handles.insert("root_journal.dat".to_string(), h.clone());
        }
        if let Some(h) = self.storage_manager.handles.get("journal.dat") {
            new_handles.insert("journal.dat".to_string(), h.clone());
        }

        // Iterate through the tables that are supposed to exist *after* the rollback
        // and add only their handles to the new map.
        for tr in &self.db_page.all_tableroot {
            let table_name = &tr.tb_schema.name;
            let tbl_file = format!("{}.tbl", table_name);
            if let Some(h) = self.storage_manager.handles.get(&tbl_file) {
                new_handles.insert(tbl_file, h.clone());
            }

            let meta_file = format!("{}.meta.idx", table_name);
            if let Some(h) = self.storage_manager.handles.get(&meta_file) {
                new_handles.insert(meta_file, h.clone());
            }

            for btree in &tr.index_vec {
                let idx_file = format!("{}.idx", btree.key_name);
                if let Some(h) = self.storage_manager.handles.get(&idx_file) {
                    new_handles.insert(idx_file, h.clone());
                }
            }
        }

        // Replace the old, "dirty" handle map with the new, clean one.
        // This purges the ghost handles from memory.
        self.storage_manager.handles = new_handles;
        eprintln!("In-memory StorageManager has been refreshed.");

        // --- END OF THE MEMORY REFRESH ---

        self.reload_catalog();
        eprintln!("Recovery Catalog Ok");
        self.cleanup_orphan_files()?;
        eprintln!("Recovery Cleaned files Ok");
        Ok(())
    }

    /// Delete any file in `db_folder` ending in .tbl, .idx, or .meta.idx
    /// whose prefix (before the first '.') is not one of the currently‐loaded tables.
    fn cleanup_orphan_files(&self) -> Result<(), DbError> {
        // Build set of all valid table names:
        let mut valid_tables = std::collections::HashSet::new();
        for tr in &self.db_page.all_tableroot {
            valid_tables.insert(tr.tb_schema.name.clone());
        }

        // Build set of all valid “index names” (i.e. column names that have a B‐Tree index):
        // Catalog.table_indexes: HashMap<table_name → HashSet<indexed_column_name>>
        let mut valid_index_names = std::collections::HashSet::new();
        for (_table, idx_cols) in &self.catalog.table_indexes {
            for col in idx_cols {
                valid_index_names.insert(col.clone());
            }
        }

        for entry_res in fs::read_dir(&self.db_folder)? {
            let entry = entry_res?;
            let fname = entry.file_name().into_string()
                .map_err(|_| DbError::Io(io::Error::new(io::ErrorKind::InvalidData, "Invalid filename".to_string())))?;

            if let Some(stripped) = fname.strip_suffix(".tbl") {
                // “<X>.tbl”: keep only if X is a valid table
                if !valid_tables.contains(stripped) {
                    let to_delete = self.db_folder.join(&fname);
                    let _ = remove_file(to_delete)?;
                }
            }
            else if let Some(stripped) = fname.strip_suffix(".meta.idx") {
                // “<X>.meta.idx”: keep only if X is a valid table
                if !valid_tables.contains(stripped) {
                    let to_delete = self.db_folder.join(&fname);
                    let _ = remove_file(to_delete)?;
                }
            }
            else if fname.ends_with(".idx") {
                // It’s some other “<X>.idx” (but not “.meta.idx” or “.tbl”)
                //   check base = fname without “.idx”
                let base = &fname[..fname.len() - 4];
                // If base is neither a table nor a known index‐column, delete it:
                if !valid_tables.contains(base) && !valid_index_names.contains(base) {
                    let to_delete = self.db_folder.join(&fname);
                    let _ = remove_file(to_delete)?;
                }
            }
            // else: any other extension we ignore
        }

        Ok(())
    }

    pub fn open(path: &str) -> Result<Self, DbError> {
        let db_folder = Path::new(path).to_path_buf();
        if !db_folder.exists() {
            fs::create_dir_all(&db_folder)?;
        }
    
        let folder_name = db_folder
            .file_name()
            .ok_or_else(|| DbError::DbEngine("Path must end with a folder name".to_string()))?
            .to_string_lossy()
            .to_string();
        let root_logical = if folder_name.ends_with(".tucodb") {
            folder_name
        } else {
            format!("{}.tucodb", folder_name)
        };
        //let root_logical = format!("{}.tucodb", folder_name);// no .tucodb end 
        let root_path = db_folder.join(&root_logical);
    
        let mut storage = StorageManager::new(db_folder.clone());
        // Register the root file and load its contents (or create a new empty page)
        let db_page = if root_path.exists() {
            storage.register_file(&root_logical)?;
            storage.root_recovery(&root_logical)?;
            let handle = storage.get_handle(&root_logical)
                .ok_or_else(|| DbError::DbEngine(format!("Failed to get handle for {}", root_logical)))?;
            let file_len = handle.file_len()? as usize;
            let mut buf = vec![0u8; file_len];
            handle.read_at(0, &mut buf)?;
            let (page, _) = bintuco::decode_from_slice::<DatabasePage>(&buf)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;
            page
        } else {
            storage.register_file(&root_logical)?;
            let page = DatabasePage::new_empty(db_folder.clone());
            let encoded = bintuco::encode_to_vec(&page)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;
            let handle = storage.get_handle(&root_logical)
                .ok_or_else(|| DbError::DbEngine(format!("Failed to get handle for {}", root_logical)))?;
            handle.write_at(0, &encoded)?;
            page
        };
    
        // Build a provisional engine in order to run recovery() on the root
        let mut engine = DatabaseEngine {
            db_folder: db_folder.clone(),
            db_path: root_logical.clone(), // Correctly store the logical name
            db_page,
            is_writing: false,
            catalog: Catalog { table_indexes: HashMap::new() },
            storage_manager: storage,
        };

        // Now we know which tables actually exist in engine.db_page.all_tableroot
        // Register only those .tbl/.idx/.meta.idx files
        for tr in &engine.db_page.all_tableroot {
            let tbl_file  = format!("{}.tbl",      tr.tb_schema.name);
            let meta_file = format!("{}.meta.idx", tr.tb_schema.name);
        
            // always register the data file and the free‐space meta‐index
            engine.storage_manager.register_file(&tbl_file)?;
            engine.storage_manager.register_file(&meta_file)?;
        
            // only register the primary‐key/index file if the table actually has one
            if tr.tb_schema.primary_key.is_some() {
                let pk_name = tr.tb_schema.primary_key.as_ref().ok_or_else(|| DbError::DbEngine("Primary key name missing".to_string()))?;
                let idx_file = format!("{}.idx", pk_name);
                engine.storage_manager.register_file(&idx_file)?;
            }
            for btree in &tr.index_vec {
                let idx_file = format!("{}.idx", btree.key_name);
                engine.storage_manager.register_file(&idx_file)?;
            }
        }
    
        // Rebuild catalog from the recovered page
        // and print some info for debug
        engine.reload_catalog();
        engine.print_storage();
        println!("After recovery");
        engine.recovery()?; // Use ? operator for error propagation
        engine.print_storage();
        engine.db_page.print_all_schema();
        engine.print_catalog();
        
        Ok(engine)
    }
    

    pub fn reload_catalog(&mut self) {
        let catalog = Catalog::from_page(&self.db_page);
        self.catalog = catalog;
    }

    pub fn print_catalog(&mut self) {
        println!("CATALOG: {}",self.catalog)
    }

    pub fn save(&self) -> Result<(), DbError> {
        let folder_name = self
            .db_folder
            .file_name()
            .ok_or_else(|| DbError::DbEngine("Path must end with a folder name".to_string()))?
            .to_string_lossy()
            .to_string();
        let root_logical = format!("{}.tucodb", folder_name);

        let encoded = bintuco::encode_to_vec(&self.db_page)
            .map_err(|e| DbError::Bintuco(e.to_string()))?;

        let handle = self
            .storage_manager
            .get_handle(&root_logical)
            .ok_or_else(|| {
                DbError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Root file not registered: {}", root_logical),
                ))
            })?;
        handle.write_at(0, &encoded)?;
        Ok(())
    }
    //NEW More General
    pub fn infer_column_type(samples: &[String]) -> SqlColumnType {
        
        /* // Sometimes give problem for large variable dataset
        // if *every* non-empty sample parses as an i32, call it INT
        let all_int = samples.iter().filter_map(|s| {
            let t = s.trim();
            if t.is_empty() { None } else { Some(t.parse::<i32>().is_ok()) }
        }).all(|ok| ok);

        if all_int {
            return SqlColumnType::INT;
        }
        */
        // otherwise, if *every* non-empty sample parses as an f32, call it FLOAT
        let all_float = samples.iter().filter_map(|s| {
            let t = s.trim();
            if t.is_empty() { None } else { Some(t.parse::<f32>().is_ok()) }
        }).all(|ok| ok);

        if all_float {
            return SqlColumnType::FLOAT;
        }

        // otherwise, if *every* non-empty sample parses as an f64, call it DOUBLE
        let all_float = samples.iter().filter_map(|s| {
            let t = s.trim();
            if t.is_empty() { None } else { Some(t.parse::<f64>().is_ok()) }
        }).all(|ok| ok);

        if all_float {
            return SqlColumnType::DOUBLE;
        }

        // fallback to VARCHAR --> Vec<u8>
        SqlColumnType::VARCHAR
    }

    pub fn load_csv_pk(
        &mut self,
        table_name: &str,
        csv_path: &str,
        pk: Option<&str>,
        force_varchar: u8,
    ) -> Result<usize, DbError> {

        // No journal -> take()
        let old_page_journal = self.storage_manager.journal.take();
        
        println!(
            "DEBUG: → enter load_csv_pk(table=\"{}\", csv_path=\"{}\", pk={:?}, force={})",
            table_name, csv_path, pk, force_varchar
        );

        let file = File::open(csv_path)?;
        let mut reader = BufReader::new(file).lines();

        // Read header
        let header = reader
            .next()
            .ok_or_else(|| DbError::Io(io::Error::new(io::ErrorKind::InvalidData, "CSV is empty".to_string())))??;
        let cols: Vec<&str> = header.split(',').map(|s| s.trim()).collect();
        let ncols = cols.len();
        println!("DEBUG: Parsed {} columns: {:?}", ncols, cols);

        // Read SAMPLE_SIZE rows for inference (and seed our first chunk)
        let mut sample_rows: Vec<Vec<String>> = Vec::with_capacity(SAMPLE_SIZE);
        let mut chunk_rows: Vec<Vec<String>> = Vec::with_capacity(CHUNK_CSV_SIZE);

        for _ in 0..SAMPLE_SIZE {
            if let Some(line_res) = reader.next() {
                let line = line_res?;
                let fields = line
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect::<Vec<_>>();
                if fields.len() != ncols {
                    return Err(DbError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Expected {} columns but found {}", ncols, fields.len()),
                    )));
                }
                sample_rows.push(fields.clone());
                chunk_rows.push(fields);
            } else {
                break;
            }
        }

        // Infer column types
        let mut types = Vec::with_capacity(ncols);
        for i in 0..ncols {
            let col_samples = sample_rows
                .iter()
                .map(|row| row[i].clone())
                .collect::<Vec<_>>();
            let ty = if force_varchar == 1 {
                SqlColumnType::VARCHAR
            } else {
                Self::infer_column_type(&col_samples)
            };
            types.push(ty);
        }

        // Create the schema & table
        let schema = TableSchema::new(
            table_name,
            types.clone(),
            cols.clone(),
            pk.map(|s| s.to_string()).as_deref(),
        )?;
        self.db_page
            .add_table(schema.clone(), &mut self.storage_manager)?;
        println!("DEBUG: db_page.add_table(\"{}\") succeeded", table_name);

        // Helper to convert & insert one chunk
        let mut skipped = 0usize;

        // In insert_chunk, increment skipped as previously shown
        let mut insert_chunk = |rows: &mut Vec<Vec<String>>| -> Result<(), DbError> {
            for raw in rows.drain(..) {
                let inputs: Vec<&str> = raw.iter().map(String::as_str).collect();
                match schema.set_row(inputs) {
                    Ok(row) => {
                        if let Err(_e) = self.db_page.insert_into_table(table_name, row, &self.storage_manager) {
                            skipped += 1;
                        }
                    }
                    Err(_e) => {
                        skipped += 1;
                    }
                }
            }
            Ok(())
        };

        // Insert the first (sample) chunk
        insert_chunk(&mut chunk_rows)?;

        // Stream the rest of the file, chunk by chunk
        for line_res in reader {
            let line = line_res?;
            let fields = line
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>();
            if fields.len() != ncols {
                return Err(DbError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Expected {} columns but found {}", ncols, fields.len()),
                )));
            }
            chunk_rows.push(fields);
            if chunk_rows.len() >= CHUNK_CSV_SIZE {
                insert_chunk(&mut chunk_rows)?;
            }
        }

        // 7) Final flush of any leftover rows
        if !chunk_rows.is_empty() {
            insert_chunk(&mut chunk_rows)?;
        }

        // Realease Journal
        self.storage_manager.journal = old_page_journal;
        println!("DEBUG: load_csv_pk completed");
        Ok(skipped)
    }

}


impl DatabaseEngine {
    fn expr_display_name(expr: &Expression) -> String {
        match expr {
            Expression::Column(colref) => colref.column.clone(),
            Expression::Literal(val) => val.to_string(),
            Expression::ArithmeticOp { left, op, right } => {
                let op_str = match op {
                    ArithmeticOp::Add => "+",
                    ArithmeticOp::Subtract => "-",
                    ArithmeticOp::Multiply => "*",
                    ArithmeticOp::Divide => "/",
                };
                format!(
                    "({} {} {})",
                    Self::expr_display_name(left),
                    op_str,
                    Self::expr_display_name(right)
                )
            }
            Expression::UnaryOp { op, expr } => {
                let sign = match op {
                    UnaryOp::Plus => "+",
                    UnaryOp::Minus => "-",
                };
                format!("({}{})", sign, Self::expr_display_name(expr))
            }
            Expression::Aggregate { func, arg } => {
                let inside = arg.as_ref()
                    .map(|boxed| Self::expr_display_name(&*boxed))
                    .unwrap_or_else(|| "*".to_string());
                format!("{:?}({})", func, inside)
            }
            _ => format!("{:?}", expr),
        }
    }

    pub fn execute_plan<'a>(&'a self, plan: &'a PlanNode) -> Result<Box<dyn RowIterator + 'a>, DbError> {
        match plan {
            PlanNode::TableScan { table } => {
                let root = self.db_page.all_tableroot.iter()
                    .find(|t| t.tb_schema.name == *table)
                    .ok_or_else(|| DbError::DbEngine(format!("Table '{}' not found", table)))?;

                let schema = root.tb_schema.clone();

                let iter = root.row_iterator(&self.storage_manager, schema.clone());

                Ok(Box::new(SchemaRowIter {
                    schema,
                    inner: iter,
                }))
            }

            PlanNode::IndexScan { table, index_column, expected_value } => {
                let key_str = match expected_value {
                    Value::BigInt(i) => i.to_string(),
                    Value::String(s) => s.clone(),
                };
                let mut found = Vec::new();
                if let Some(row) = self
                    .db_page
                    .search_by_key(table, &index_column.column, &key_str, &self.storage_manager)?
                {
                    found.push(row);
                }

                let schema = self.db_page.all_tableroot.iter()
                    .find(|t| t.tb_schema.name == *table)
                    .ok_or_else(|| DbError::DbEngine(format!("Table '{}' not found", table)))?
                    .tb_schema.clone();

                Ok(Box::new(SchemaRowIter {
                    schema,
                    inner: Box::new(found.into_iter()),
                }))
            }

            PlanNode::Projection { input, expressions } => {
                let child_iter = self.execute_plan(input)?;
                let child_schema = child_iter.schema().clone();

                let final_projection_exprs: Vec<Expression> = expressions.iter().flat_map(|item| {
                    if let Expression::Column(colref) = item {
                        if colref.column == "*" {
                            return child_schema.name_col.iter().map(|field_name| {
                                Expression::Column(ColumnRef { table: None, column: field_name.clone() })
                            }).collect::<Vec<Expression>>();
                        }
                    }
                    vec![item.clone()]
                }).collect();

                let schema_names = final_projection_exprs.iter().map(|e| Self::expr_display_name(e)).collect::<Vec<_>>();
                let schema = TableSchema::from_column_names(schema_names);

                // This iterator now uses `filter_map` to skip any rows that produce an error
                // during expression evaluation, in a way to handle
                // the existing `RowIterator` trait that expects `Item = Row`.
                let mapped_iter = child_iter.filter_map(move |row| {
                    let projected_values_result: Result<Vec<SqlValue>, DbError> = final_projection_exprs
                        .iter()
                        .map(|expr| eval_expr(&row, &child_schema, expr))
                        .collect();
                    
                    match projected_values_result {
                        Ok(values) => Some(Row { values }), // If evaluation succeeds, return Some(row).
                        Err(e) => {
                            // If evaluation fails, log the error and return None to drop the row.
                            eprintln!("Skipping row due to projection error: {}", e);
                            None
                        }
                    }
                });

                Ok(Box::new(SchemaRowIter {
                    schema,
                    inner: Box::new(mapped_iter),
                }))
            }

            PlanNode::Filter { input, predicate } => {
                let child_iter = self.execute_plan(input)?;
                let schema = child_iter.schema().clone();
                println!("Filter schema columns: {:?}", schema.name_col);

                let pred = predicate.clone();

                let schema_for_filter = schema.clone();
                println!("Filter schema columns: {:?}", schema_for_filter.name_col);
                let filtered_iter = child_iter.filter(move |row| evaluate_predicate(row, &schema_for_filter, &pred).unwrap_or(false)); //FIXME: unwrap/panic

                Ok(Box::new(SchemaRowIter {
                    schema,
                    inner: Box::new(filtered_iter),
                }))
            }


            PlanNode::Limit { input, count } => {
                let child_iter = self.execute_plan(input)?;
                let schema = child_iter.schema().clone();

                Ok(Box::new(SchemaRowIter {
                    schema,
                    inner: Box::new(child_iter.take(*count)),
                }))
            }

            PlanNode::Distinct { input } => {
                // peel off any ORDER BY under Projection
                let (_stripped_plan, orderings) = strip_order_by(input.as_ref());
                // execute the remaining plan (now without the ORDER BY node)
                let child_iter = self.execute_plan(input)?;
                let schema = child_iter.schema().clone();
                
                crate::groupiter::distinct_sorted(child_iter, schema, orderings) // This function should also return Result
                    .map_err(|e| DbError::GroupIter(e.to_string())) // Map error to DbError
            }
            

            PlanNode::GroupBy { input, columns, aggregates } => {
                let child_iter = self.execute_plan(input)?;
                let input_schema = child_iter.schema().clone();

                let group_idxs: Vec<usize> = columns.iter()
                    .map(|colref| {
                        input_schema
                            .name_col
                            .iter()
                            .position(|n| n == &colref.column)
                            .ok_or_else(|| DbError::Planner(format!("GROUP BY column '{}' not found", colref.column))) // Return an error
                    })
                    .collect::<Result<Vec<usize>, DbError>>()?; // Collect into a Result

                let mut out_cols: Vec<String> =
                    columns.iter().map(|c| c.column.clone()).collect();

                for expr in aggregates.iter() {
                    if let Expression::Aggregate { func, arg } = expr {
                        let inside = arg
                            .as_ref()
                            .map(|e| match &**e {
                                Expression::Column(colref) => colref.column.clone(),
                                _ => format!("{:?}", e),
                            })
                            .unwrap_or_else(|| "*".into());
                        out_cols.push(format!("{:?}({})", func, inside));
                    } else {
                        return Err(DbError::Planner(format!("Expected only aggregates in GroupBy node, found {:?}", expr))); // Return an error
                    }
                }

                let output_schema = TableSchema::from_column_names(out_cols);

                let aggr_exprs = aggregates.clone();
                let input_iter = self.execute_plan(input)?;

                let grouped_iter = crate::groupiter::group_by(
                    input_iter,
                    group_idxs,
                    aggr_exprs,
                    input_schema.clone(),
                )?;


                Ok(Box::new(SchemaRowIter {
                    schema: output_schema,
                    inner: grouped_iter,
                }))
            }

            PlanNode::HavingFilter { input, predicate } => {
                let child_iter = self.execute_plan(input)?;
                let schema = child_iter.schema().clone();
            
                let expr: Expression = condition_expr_to_expression(&predicate);
            
                let filtered_iter = crate::groupiter::having(child_iter, expr, schema.clone());
            
                Ok(Box::new(SchemaRowIter {
                    schema,
                    inner: filtered_iter,
                }))
            }
            

            PlanNode::OrderBy { input, orderings } => {
                // build child iterator
                let child_iter = self.execute_plan(input)?;
                let schema     = child_iter.schema().clone();
    
                // inline DFS over each expr in ORDER BY
                for (expr, _) in orderings {
                    let mut stack = vec![expr];
                    while let Some(e) = stack.pop() {
                        match e {
                            Expression::Column(colref) => {
                                if resolve_column(&schema.name_col, &colref.column).is_none() {
                                    return Err(DbError::DbEngine(
                                        format!("Unknown column in ORDER BY: {}", colref.column)
                                    ));
                                }
                            }
                            Expression::UnaryOp { expr: inner, .. } => {
                                // e.g. -TMIN
                                stack.push(inner);
                            }
                            Expression::ArithmeticOp { left, right, .. } => {
                                
                                stack.push(left);
                                stack.push(right);
                            }
                            _ => {
                                // literals, functions, etc...
                            }
                        }
                    }
                }
    
                // 3) do the sort now that every Column was validated
                let sorter      = crate::order::RowSorter::new(schema.clone(), orderings.clone());
                let sorted_iter = sorter.sort(child_iter)?;
    
                Ok(Box::new(SchemaRowIter {
                    schema,
                    inner: sorted_iter,
                }))
            }

            PlanNode::Join { left, right, on_left, on_right } => {
                let left_iter = self.execute_plan(left)?;
                let right_iter = self.execute_plan(right)?;
            
                let left_schema = left_iter.schema().clone();
                let right_schema = right_iter.schema().clone();
            
                let output_schema = crate::order::combine_schemas(&left_schema, &right_schema, Some(&left_schema.name), Some(&right_schema.name));
                println!();
                println!("JOIN output schema {:?}", output_schema);
                let join_iter = crate::order::merge_join(
                    left_iter,
                    right_iter,
                    left_schema,
                    right_schema,
                    on_left.clone(),
                    on_right.clone(),
                )?;
            
                Ok(Box::new(SchemaRowIter {
                    schema: output_schema,
                    inner: join_iter,
                }))
            }

            PlanNode::Explain { inner } => {
                let mut lines = Vec::new();
                Self::collect_plan_lines_postorder(&*inner, 0, &mut lines);
            
                let row_iter = lines.into_iter().map(|line| {
                    let varchar = SqlValue::VARCHAR(line.into_bytes());
                    Row { values: vec![varchar] }
                });
            
                Ok(Box::new(SchemaRowIter {
                    schema: TableSchema::from_column_names(vec!["EXPLAIN".to_string()]),
                    inner: Box::new(row_iter),
                }))
            }
            
            _ => Err(DbError::DbEngine(format!("execute_plan variant not implemented: {:?}", plan))),
        }
    }
}



impl DatabaseEngine {
    /// Recursively traverse `node` in POSTORDER to build textual plan output.
    fn collect_plan_lines_postorder(node: &PlanNode, indent: usize, lines: &mut Vec<String>) {
        let prefix = "-->".repeat(1);
        match node {
            PlanNode::TableScan { table } => {
                lines.push(format!("{}TableScan(table = \"{}\")", prefix, table));
            }
            PlanNode::IndexScan {
                table,
                index_column,
                expected_value,
            } => {
                lines.push(format!(
                    "{}IndexScan(table = \"{}\", index_column = {:?}, expected_value = {:?})",
                    prefix, table, index_column, expected_value
                ));
            }
            PlanNode::Join {
                left,
                right,
                on_left,
                on_right,
            } => {
                Self::collect_plan_lines_postorder(left, indent + 1, lines);
                Self::collect_plan_lines_postorder(right, indent + 1, lines);
                lines.push(format!(
                    "{}Join(on_left = {:?}, on_right = {:?})",
                    prefix, on_left, on_right
                ));
            }
            PlanNode::Filter { input, predicate } => {
                Self::collect_plan_lines_postorder(input, indent + 1, lines);
                lines.push(format!("{}Filter(predicate = {:?})", prefix, predicate));
            }
            PlanNode::GroupBy { input, columns , aggregates} => {
                Self::collect_plan_lines_postorder(input, indent + 1, lines);
                lines.push(format!("{}GroupBy(columns = {:?}, aggregates = {:?})", prefix, columns, aggregates));
            }
            PlanNode::HavingFilter { input, predicate } => {
                Self::collect_plan_lines_postorder(input, indent + 1, lines);
                lines.push(format!("{}HavingFilter(predicate = {:?})", prefix, predicate));
            }
            PlanNode::Projection { input, expressions } => {
                Self::collect_plan_lines_postorder(input, indent + 1, lines);
                lines.push(format!("{}Projection(expressions = {:?})", prefix, expressions));
            }
            PlanNode::Distinct { input } => {
                Self::collect_plan_lines_postorder(input, indent + 1, lines);
                lines.push(format!("{}Distinct", prefix));
            }
            PlanNode::OrderBy { input, orderings } => {
                Self::collect_plan_lines_postorder(input, indent + 1, lines);
                lines.push(format!("{}OrderBy(orderings = {:?})", prefix, orderings));
            }
            PlanNode::Limit { input, count } => {
                Self::collect_plan_lines_postorder(input, indent + 1, lines);
                lines.push(format!("{}Limit(count = {})", prefix, count));
            }
            PlanNode::CreateTable {
                table,
                columns,
                primary_key,
                indexes,
            } => {
                lines.push(format!(
                    "{}CreateTable(table = \"{}\", columns = {:?}, primary_key = {:?}, indexes = {:?})",
                    prefix, table, columns, primary_key, indexes
                ));
            }
            PlanNode::CreateIndex {
                table,
                column,
                index_name,
            } => {
                lines.push(format!(
                    "{}CreateIndex(table = \"{}\", column = {:?}, index_name = {:?})",
                    prefix, table, column, index_name
                ));
            }
            PlanNode::Insert {
                table,
                columns,
                values,
            } => {
                lines.push(format!(
                    "{}Insert(table = \"{}\", columns = {:?}, values = {:?})",
                    prefix, table, columns, values
                ));
            }
            PlanNode::Update {
                table,
                assignments,
                predicate,
            } => {
                lines.push(format!(
                    "{}Update(table = \"{}\", assignments = {:?}, predicate = {:?})",
                    prefix, table, assignments, predicate
                ));
            }
            PlanNode::Delete { table, predicate } => {
                lines.push(format!(
                    "{}Delete(table = \"{}\", predicate = {:?})",
                    prefix, table, predicate
                ));
            }
            PlanNode::Explain { inner: _ } => { /* Handled by the outer execute_plan */ }
        }
    }
}

impl DatabaseEngine {
    pub fn end_transaction(&mut self) {
        self.is_writing = false;
    }

    pub fn execute_write_plan(&mut self, plan: &PlanNode) -> Result<(), DbError> {
        match plan {
            
            PlanNode::Update { table, assignments, predicate } => {
                // Convert assignments (Vec<(ColumnRef, Expression)>) → Vec<(String, Expression)>
                let assigns: Vec<(String, Expression)> = assignments
                    .iter()
                    // Corrected: Convert Value into an Expression::Literal
                    .map(|(colref, value)| (colref.column.clone(), Expression::Literal(value.clone())))
                    .collect();

                let updated = self.db_page.update(
                    table,
                    &assigns,
                    predicate.as_ref(),
                    &mut self.storage_manager,
                )?;
                println!("Updated {} rows in '{}'", updated, table);
                Ok(())
            }
            
            PlanNode::Delete { table, predicate } => {
                let deleted = self.db_page.delete(
                    table,
                    predicate.as_ref(),
                    &mut self.storage_manager,
                )?;
                println!("Deleted {} rows from '{}'", deleted, table);
                Ok(())
            }
            
            PlanNode::CreateTable { table, columns, primary_key, indexes } => {
                // Convert the Vec<(String,String)> into SqlColumnType + &str name lists
                let col_types = columns.iter().map(|(_, ty)| SqlColumnType::from_str(ty))
                    .collect::<Result<Vec<_>, DbError>>()?; // Collect errors

                let col_names_owned = columns.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();
                let col_names = col_names_owned.iter().map(String::as_str).collect::<Vec<&str>>();
            
                // Build & register the TableSchema
                let schema = TableSchema::new(
                    table,
                    col_types,
                    col_names,
                    primary_key.as_deref(),
                )?;
                self.db_page
                    .add_table(schema, &mut self.storage_manager)?;
            
                // Immediately create any secondary indexes declared in the CREATE
                {
                    // the new TableRoot is always the last one in all_tableroot
                    let tr = self.db_page.all_tableroot.last_mut().ok_or_else(|| DbError::DbEngine("No table root found after create table".to_string()))?;
                    for idx_col in indexes {
                        let idx_logical = format!("{}.idx", idx_col);
                        // register the file on disk
                        self.storage_manager
                            .register_file(&idx_logical)?;
                        // build the B-tree
                        let btree = Btree::new(
                            idx_col.clone(),
                            &self.db_folder,
                            &self.storage_manager,
                        )?;
                        // hook it into the TableRoot
                        tr.index_vec.push(btree);
                    }
                }
            
                Ok(())
            }
    
            // — CREATE INDEX —
            PlanNode::CreateIndex { table, column, index_name: _ } => {
                // No journal -> take()
                let old_page_journal = self.storage_manager.journal.take();
        
                self.db_page
                    .create_secondary_index(&table, &column, &mut self.storage_manager)?;
                
                // Realease Journal
                self.storage_manager.journal = old_page_journal;

                Ok(())
            }

            PlanNode::Insert { table, columns, values } => {
                let table_root = self.db_page
                    .all_tableroot
                    .iter()
                    .find(|t| t.tb_schema.name == *table)
                    .ok_or_else(|| DbError::DbEngine(format!("Table '{}' not found", table)))?;

                let schema = &table_root.tb_schema;
                let all_cols = &schema.name_col;
                let mut row_vals: Vec<SqlValue> = Vec::new();

                for col_name in all_cols {
                    if let Some(pos) = columns.iter().position(|c| c == col_name) {
                        let val = &values[pos];
                        let col_type = schema
                            .columns
                            .iter()
                            .zip(&schema.name_col)
                            .find_map(|(t, n)| if n == col_name { Some(t) } else { None })
                            .ok_or_else(|| DbError::DbEngine(format!("Column type not found for '{}'", col_name)))?;
                        row_vals.push(conv_val_to_sql_val(val, col_type)?);
                    } else {
                        return Err(DbError::DbEngine(format!("Missing value for column '{}'", col_name)));
                    }
                }

                let row = Row { values: row_vals };
                self.db_page
                    .insert_into_table(table, row, &self.storage_manager)?;
                Ok(())
            }
            _ => Err(DbError::DbEngine(format!("Unsupported write operation: {:?}", plan))),
        }
    }
}

impl DatabaseEngine {
    pub fn print_all_table_head_cells(&self) {
        for table_root in &self.db_page.all_tableroot {
            let table_name = &table_root.tb_schema.name;
            println!("--- Table: {} (up to 15 head cells) ---", table_name);
            self.db_page
                .print_head_cells(&table_root.name_root, 15, &self.storage_manager);
        }
    }
}


impl DatabaseEngine {
    pub fn begin_transaction(&mut self) -> Result<(), DbError> {
        if !self.is_writing {
            self.is_writing = true;
            self.storage_manager
                .start_transaction()?;
            
        } else {
            // Already in a transaction, perhaps return an error or do nothing
            return Err(DbError::Transaction("Already in a transaction".to_string()));
        }
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> Result<(), DbError> {
        println!("Commit from dbengine");
        self.storage_manager.commit_journal()?;
        let encoded_root: Vec<u8> =
            bintuco::encode_to_vec(&self.db_page).map_err(|e| {
                DbError::Bintuco(e.to_string())
            })?;
        self.storage_manager.commit_root(&self.db_path, &encoded_root)?;
        //self.is_writing = false;
    
        Ok(())
    }

    pub fn rollback_transaction(&mut self) -> Result<(), DbError> {
        self.storage_manager.rollback_journal()?;
        self.rollback_root()?;
    
        Ok(())
    }

    pub fn rollback_root(&mut self) -> Result<(), DbError> {
        let root_logical = format!("root_journal.dat");
        let handle = self
            .storage_manager
            .get_handle(&root_logical)
            .ok_or_else(|| {
                DbError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Root journal file not registered: {}", root_logical),
                ))
            })?;
        let file_len = handle.file_len()? as usize;
        let mut buf = vec![0u8; file_len];
        handle.read_at(0, &mut buf)?;
        let (restored_page, _) = bintuco::decode_from_slice(&buf)
            .map_err(|e| DbError::Bintuco(e.to_string()))?;

        self.db_page = restored_page;

        let mut new_roots = Vec::with_capacity(self.db_page.all_tableroot.len());
        for existing in self.db_page.all_tableroot.clone().into_iter() {
            let schema = existing.tb_schema.clone();
            let table_root = TableRoot::from_schema(schema, &self.db_folder, &mut self.storage_manager)?;
            new_roots.push(table_root);
        }
        self.db_page.all_tableroot = new_roots;
        self.reload_catalog();
        Ok(())
    }
}



impl DatabaseEngine {
    /// Export `table` to CSV at `out_path`, quoting & escaping as needed.
    pub fn export_query_csv(&self, query: &str, out_path: &str) -> Result<(), DbError> {
        // 1. Parse and plan the arbitrary SELECT query.
        let lexer = Lexer::new(query);
        let mut parser = Parser::new(lexer)?;
        let stmt = parser.parse_query()?;
        let plan = {
            let planner = QueryPlanner::new(self);
            planner.plan_statement(stmt)?
        };
    
        // 2. Open the file for writing.
        let f = File::create(out_path)?;
        let mut w = BufWriter::new(f);
    
        // 3. Execute the plan to get the iterator and the correct output schema.
        let plan_iterator = self.execute_plan(&plan)?;
        
        // 4. Get the header row directly from the executed plan's schema.
        let cols = plan_iterator.schema().name_col.clone();
    
        // 5. Write the header row, escaping fields if necessary.
        for (i, col) in cols.iter().enumerate() {
            let mut field = col.clone();
            // Quote fields containing commas, quotes, or newlines.
            if field.contains(',') || field.contains('"') || field.contains('\n') {
                field = format!("\"{}\"", field.replace('"', "\"\""));
            }
            if i > 0 {
                w.write_all(b",")?;
            }
            w.write_all(field.as_bytes())?;
        }
        w.write_all(b"\n")?;
    
        // 6. Stream the result rows to the file.
        for row in plan_iterator {
            for (i, val) in row.values.iter().enumerate() {
                let mut field = val.to_string();
                if field.contains(',') || field.contains('"') || field.contains('\n') {
                    field = format!("\"{}\"", field.replace('"', "\"\""));
                }
                if i > 0 {
                    w.write_all(b",")?;
                }
                w.write_all(field.as_bytes())?;
            }
            w.write_all(b"\n")?;
        }
    
        w.flush()?;
        Ok(())
    }
}


// ══════════════════════════════════════ ITERATOR WITH SCHEMA ══════════════════════════════════════

///I faced some problem with expression columns and Projection node. So instead of refactor planner.rs
/// or other solution that could broke all my code, i decide that the iterator carry with it the schema
/// of the current node. So in projection node we can have information about schema of the 
/// precedent node

/// Iterator that carry the schema with it
pub trait RowIterator: Iterator<Item = Row> {
    fn schema(&self) -> &TableSchema;
}
pub struct SchemaRowIter<I> {
    pub schema: TableSchema,
    pub inner: I,
}

impl<I> Iterator for SchemaRowIter<I>
where
    I: Iterator<Item = Row>,
{
    type Item = Row;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<I> RowIterator for SchemaRowIter<I>
where
    I: Iterator<Item = Row>,
{
    fn schema(&self) -> &TableSchema {
        &self.schema
    }
}

// ══════════════════════════════════════ GENERAL UTILITY FUNCTION ══════════════════════════════════════


fn conv_val_to_sql_val(val: &Value, col_type: &SqlColumnType) -> Result<SqlValue, DbError> {
    match col_type {
        SqlColumnType::TINYINT => val.to_string().parse::<i8>().map(SqlValue::TINYINT).map_err(|_| DbError::Parser(format!("Invalid TINYINT value: {:?}", val))),
        SqlColumnType::SMALLINT => val.to_string().parse::<i16>().map(SqlValue::SMALLINT).map_err(|_| DbError::Parser(format!("Invalid SMALLINT value: {:?}", val))),
        SqlColumnType::INT => val.to_string().parse::<i32>().map(SqlValue::INT).map_err(|_| DbError::Parser(format!("Invalid INT value: {:?}", val))),
        SqlColumnType::BIGINT => val.to_string().parse::<i64>().map(SqlValue::BIGINT).map_err(|_| DbError::Parser(format!("Invalid BIGINT value: {:?}", val))),
        SqlColumnType::UBIGINT => val.to_string().parse::<u64>().map(SqlValue::UBIGINT).map_err(|_| DbError::Parser(format!("Invalid UBIGINT value: {:?}", val))),
        SqlColumnType::UTINYINT => val.to_string().parse::<u8>().map(SqlValue::UTINYINT).map_err(|_| DbError::Parser(format!("Invalid UTINYINT value: {:?}", val))),
        SqlColumnType::USMALLINT => val.to_string().parse::<u16>().map(SqlValue::USMALLINT).map_err(|_| DbError::Parser(format!("Invalid USMALLINT value: {:?}", val))),
        SqlColumnType::UINT => val.to_string().parse::<u32>().map(SqlValue::UINT).map_err(|_| DbError::Parser(format!("Invalid UINT value: {:?}", val))),
        SqlColumnType::FLOAT => val.to_string().parse::<f32>().map(SqlValue::FLOAT).map_err(|_| DbError::Parser(format!("Invalid FLOAT value: {:?}", val))),
        SqlColumnType::DOUBLE => val.to_string().parse::<f64>().map(SqlValue::DOUBLE).map_err(|_| DbError::Parser(format!("Invalid DOUBLE value: {:?}", val))),
        SqlColumnType::CHAR => Ok(SqlValue::CHAR(val.to_string().into_bytes())),
        SqlColumnType::VARCHAR => Ok(SqlValue::VARCHAR(val.to_string().into_bytes())),
    }
}

fn find_table_name(node: &PlanNode) -> Result<&str, DbError> {
    match node {
        PlanNode::TableScan { table } => Ok(table.as_str()),
        PlanNode::IndexScan { table, .. } => Ok(table.as_str()),
        PlanNode::Filter { input, .. } => find_table_name(input),
        PlanNode::Projection { input, .. } => find_table_name(input),
        PlanNode::Distinct { input } => find_table_name(input),
        PlanNode::OrderBy { input, .. } => find_table_name(input),
        PlanNode::Limit { input, .. } => find_table_name(input),
        PlanNode::GroupBy { input, .. } => find_table_name(input),
        PlanNode::HavingFilter { input, .. } => find_table_name(input),
        PlanNode::Join { left, .. } => find_table_name(left),
        _ => Err(DbError::DbEngine(format!("Cannot determine table name from plan node: {:?}", node))),
    }
}

// helper: turn any Expression into a SqlValue
pub fn eval_expr(row: &Row, schema: &TableSchema, expr: &Expression) -> Result<SqlValue, DbError> {
    // reuse your existing to_f64_or_nan, parse_literal_as_column_type, etc.
    //eprint!("row= {:?} , Schema= {:?}, Expr= {:?}", row, schema, expr);
    match expr {
        Expression::Column(colref) => {
            let idx = resolve_column(&schema.name_col, &colref.column)
                .ok_or_else(|| DbError::Planner(format!("Projection: column '{}' not found in schema {:?}", colref.column, schema.name_col)))?;
            Ok(row.values[idx].clone())
        }
        
        Expression::Literal(val) => {
            // choose a default target type; adjust as needed
            // This needs to be more robust, picking a target type for the literal
            // If the literal is used in a context where its type can be inferred (e.g. assignment)
            // then `parse_literal_as_column_type` is appropriate. Here, a default
            // conversion might be better or require more context.
            match val {
                Value::BigInt(i) => Ok(SqlValue::BIGINT(*i)),
                Value::String(s) => Ok(SqlValue::VARCHAR(s.clone().into_bytes())), 
            }
        }
        Expression::ArithmeticOp { left, op, right } => {
            let l = eval_expr(row, schema, left)?;
            let r = eval_expr(row, schema, right)?;
            let xf = to_f64_or_nan(&l);
            let yf = to_f64_or_nan(&r);
            let rf = match op {
                ArithmeticOp::Add      => xf + yf,
                ArithmeticOp::Subtract => xf - yf,
                ArithmeticOp::Multiply => xf * yf,
                ArithmeticOp::Divide   => {
                    if yf == 0.0 {
                        return Err(DbError::Planner("Division by zero".to_string()));
                    }
                    xf / yf
                },
            };
            Ok(SqlValue::DOUBLE(rf))
        }
        Expression::UnaryOp { op, expr } => {
            let v = eval_expr(row, schema, expr)?;
            let vf = to_f64_or_nan(&v);
            let rf = if *op == UnaryOp::Minus { -vf } else { vf };
            Ok(SqlValue::DOUBLE(rf))
        }
        // ← New arm for pulling out aggregated values
        Expression::Aggregate { func, arg } => {
            let inside = arg.as_ref().map(|boxed| {
                match &**boxed {
                    Expression::Column(colref) => colref.column.clone(),
                    other => eval_expr(row, schema, other).unwrap().to_string(), //FIXME: unwrap
                }
            }).unwrap_or_else(|| "*".to_string());
            let col_name = format!("{:?}({})", func, inside);
            let idx = resolve_column(&schema.name_col, &col_name)
                .ok_or_else(|| DbError::Planner(format!(
                    "Projection/Aggregate: '{}' not found in schema {:?}",
                    col_name, schema.name_col
                )))?;
            Ok(row.values[idx].clone())
        }
        _ => Err(DbError::Planner(format!("Unsupported projection expression: {:?}", expr))),
    }
}


/// Recursively evaluate any Expression into a boolean predicate.
pub fn evaluate_predicate(
    row: &Row,
    schema: &TableSchema,
    expr: &Expression,
) -> Result<bool, DbError> {
    eprintln!("→ evaluate_predicate: {:?}", expr);

    let result = match expr {
        Expression::LogicalOp { left, op, right } => {
            let l = evaluate_predicate(row, schema, left)?;
            let r = evaluate_predicate(row, schema, right)?;
            let res = match op {
                LogicalOperator::And => l && r,
                LogicalOperator::Or => l || r,
            };
            eprintln!("  LogicalOp {:?}: left={} right={} → {}", op, l, r, res);
            Ok(res)
        }

        Expression::Comparison { left, op, right } => {
            // Evaluate left expr
            let lval = eval_expr(row, schema, left)?;
            let ltype = expr_output_type(left, schema);
            println!("x999 ltype{:?}", ltype);
            // Evaluate right expr, coercing literals to ltype if possible
            let rval = match (&**right, &ltype) {
                (Expression::Literal(raw), Some(ty)) => {
                    let v = parse_literal_as_column_type(raw, ty)?;
                    eprintln!("    Coerced right literal {:?} to type {:?} → {:?}", raw, ty, v);
                    v
                }
                _ => {
                    let v = eval_expr(row, schema, right)?;
                    eprintln!("    Evaluated right expr = {:?}", v);
                    v
                }
            };

            let cmp = compare_sqlvalue(&lval, &rval, op);
            eprintln!("  → compare_sqlvalue: {:?} {:?} {:?} = {}", lval, op, rval, cmp);
            Ok(cmp)
        }

        other => Err(DbError::Planner(format!("Unsupported predicate node: {:?}", other))),
    };

    eprintln!("← predicate result: {:?}\n", result);
    result
}
fn expr_output_type(expr: &Expression, schema: &TableSchema) -> Option<SqlColumnType> {
    println!("x777{:?}", schema);
    match expr {
        Expression::Column(colref) => {
            println!("Matched Column in exproutype");
            println!("Colref:{:?}", colref);

            // Try exact match of unqualified name first
            if let Some(idx) = schema.name_col.iter().position(|c| c == &colref.column) {
                return Some(schema.columns[idx].clone());
            }

            // If table name present, try fully qualified match
            if let Some(table) = &colref.table {
                let qualified = format!("{}.{}", table, colref.column);
                if let Some(idx) = schema.name_col.iter().position(|c| c == &qualified) {
                    return Some(schema.columns[idx].clone());
                }
            }

            // If no table name, try suffix match for qualified schema names
            if colref.table.is_none() {
                if let Some(idx) = schema.name_col.iter()
                    .position(|c| c.ends_with(&format!(".{}", colref.column)))
                {
                    return Some(schema.columns[idx].clone());
                }
            }

            None
        }
        Expression::ArithmeticOp { left, .. } => {
            // For arithmetic, infer as DOUBLE
            Some(SqlColumnType::DOUBLE)
        }
        Expression::UnaryOp { expr, .. } => expr_output_type(expr, schema),
        Expression::Aggregate { func, .. } => {
            // Return the SQL type depending on aggregation function
            match func {
                crate::tokenizer::AggFunc::Count => Some(SqlColumnType::UBIGINT),
                crate::tokenizer::AggFunc::Sum => Some(SqlColumnType::BIGINT),
                // Add more aggregates
                _ => None,
            }
        }
        Expression::Literal(_) => None,
        
        _ => None,
    }
}



pub fn compare_sqlvalue(a: &SqlValue, b: &SqlValue, op: &BinaryOperator) -> bool {
    use SqlValue::*;
    match (a, b) {
        (TINYINT(x), TINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (SMALLINT(x), SMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (INT(x), INT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (BIGINT(x), BIGINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UBIGINT(x), UBIGINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UTINYINT(x), UTINYINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (USMALLINT(x), USMALLINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UINT(x), UINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (FLOAT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), DOUBLE(y)) => cmp_primitive(*x, *y, op),
       
        // Signed Integer vs. Signed Integer
        (TINYINT(x), SMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (TINYINT(x), INT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (TINYINT(x), BIGINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (SMALLINT(x), INT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (SMALLINT(x), BIGINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (INT(x), BIGINT(y)) => cmp_primitive(*x as i64, *y as i64, op),

        // Unsigned Integer vs. Unsigned Integer
        (UTINYINT(x), USMALLINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UTINYINT(x), UINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UTINYINT(x), UBIGINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (USMALLINT(x), UINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (USMALLINT(x), UBIGINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UINT(x), UBIGINT(y)) => cmp_primitive(*x as u64, *y as u64, op),

        // Signed Integer vs. Unsigned Integer (for correctness i cast u64 as i128 )
        (TINYINT(x), UTINYINT(y)) => cmp_primitive(*x as i16, *y as i16, op), 
        (TINYINT(x), USMALLINT(y)) => cmp_primitive(*x as i32, *y as i32, op),
        (TINYINT(x), UINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (TINYINT(x), UBIGINT(y)) => cmp_primitive(*x as i128, *y as i128, op),

        (SMALLINT(x), UTINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (SMALLINT(x), USMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (SMALLINT(x), UINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (SMALLINT(x), UBIGINT(y)) => cmp_primitive(*x as i128, *y as i128, op),

        (INT(x), UTINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (INT(x), USMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (INT(x), UINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (INT(x), UBIGINT(y)) => cmp_primitive(*x as i128, *y as i128, op),

        (BIGINT(x), UTINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (BIGINT(x), USMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (BIGINT(x), UINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (BIGINT(x), UBIGINT(y)) => cmp_primitive(*x as i128, *y as i128, op),


        // Floating-point vs. Floating-point
        (FLOAT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),

        // Integer vs. Floating-point (all cast to f64)
        (TINYINT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (TINYINT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (SMALLINT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (SMALLINT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (INT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (INT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (BIGINT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (BIGINT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),

        (UTINYINT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (UTINYINT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (USMALLINT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (USMALLINT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (UINT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (UINT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (UBIGINT(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (UBIGINT(x), DOUBLE(y)) => cmp_primitive(*x as f64, *y as f64, op),

        // And vice-versa for all the above to cover both (TypeA, TypeB) and (TypeB, TypeA) if your `match` is not commutative.
    
        // Signed Integer vs. Signed Integer (Reverse Order)
        (SMALLINT(x), TINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (INT(x), TINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (BIGINT(x), TINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (INT(x), SMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (BIGINT(x), SMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (BIGINT(x), INT(y)) => cmp_primitive(*x as i64, *y as i64, op),

        // Unsigned Integer vs. Unsigned Integer (Reverse Order)
        (USMALLINT(x), UTINYINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UINT(x), UTINYINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UBIGINT(x), UTINYINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UINT(x), USMALLINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UBIGINT(x), USMALLINT(y)) => cmp_primitive(*x as u64, *y as u64, op),
        (UBIGINT(x), UINT(y)) => cmp_primitive(*x as u64, *y as u64, op),

        // Signed Integer vs. Unsigned Integer (Reverse Order)
        (UTINYINT(x), TINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (USMALLINT(x), TINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UINT(x), TINYINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UBIGINT(x), TINYINT(y)) => cmp_primitive(*x as i128, *y as i128, op),

        (UTINYINT(x), SMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (USMALLINT(x), SMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UINT(x), SMALLINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UBIGINT(x), SMALLINT(y)) => cmp_primitive(*x as i128, *y as i128, op),

        (UTINYINT(x), INT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (USMALLINT(x), INT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UINT(x), INT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UBIGINT(x), INT(y)) => cmp_primitive(*x as i128, *y as i128, op),

        (UTINYINT(x), BIGINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (USMALLINT(x), BIGINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UINT(x), BIGINT(y)) => cmp_primitive(*x as i64, *y as i64, op),
        (UBIGINT(x), BIGINT(y)) => cmp_primitive(*x as i128, *y as i128, op),

        // Floating-point vs. Floating-point (Reverse Order)
        (DOUBLE(x), FLOAT(y)) => cmp_primitive(*x as f64, *y as f64, op),

        // Integer vs. Floating-point (Reverse Order)
        (FLOAT(x), TINYINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), TINYINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (FLOAT(x), SMALLINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), SMALLINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (FLOAT(x), INT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), INT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (FLOAT(x), BIGINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), BIGINT(y)) => cmp_primitive(*x as f64, *y as f64, op),

        (FLOAT(x), UTINYINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), UTINYINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (FLOAT(x), USMALLINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), USMALLINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (FLOAT(x), UINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), UINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (FLOAT(x), UBIGINT(y)) => cmp_primitive(*x as f64, *y as f64, op),
        (DOUBLE(x), UBIGINT(y)) => cmp_primitive(*x as f64, *y as f64, op),

        // CHAR VALUES
        (CHAR(x), CHAR(y)) => cmp_primitive(&x, &y, op),
        (VARCHAR(x), VARCHAR(y)) => cmp_primitive(&x, &y, op),
        (VARCHAR(x), CHAR(y)) => cmp_primitive(&x, &y, op),
        (CHAR(x), VARCHAR(y)) => cmp_primitive(&x, &y, op),
        _ => false, // Incomparable types
    }
}

fn cmp_primitive<T: PartialOrd + PartialEq>(x: T, y: T, op: &BinaryOperator) -> bool {
    match op {
        BinaryOperator::Equal => x == y,
        BinaryOperator::NotEqual => x != y,
        BinaryOperator::GreaterThan => x > y,
        BinaryOperator::LessThan => x < y,
        BinaryOperator::GreaterThanEqual => x >= y,
        BinaryOperator::LessThanEqual => x <= y,
    }
}

pub fn parse_literal_as_column_type(val: &Value, col_type: &SqlColumnType) -> Result<SqlValue, DbError> {
    match col_type {
        SqlColumnType::TINYINT => val.to_string().parse::<i8>().map(SqlValue::TINYINT).map_err(|_| DbError::Parser(format!("Invalid TINYINT literal: {:?}", val))),
        SqlColumnType::SMALLINT => val.to_string().parse::<i16>().map(SqlValue::SMALLINT).map_err(|_| DbError::Parser(format!("Invalid SMALLINT literal: {:?}", val))),
        SqlColumnType::INT => val.to_string().parse::<i32>().map(SqlValue::INT).map_err(|_| DbError::Parser(format!("Invalid INT literal: {:?}", val))),
        SqlColumnType::BIGINT => val.to_string().parse::<i64>().map(SqlValue::BIGINT).map_err(|_| DbError::Parser(format!("Invalid BIGINT literal: {:?}", val))),
        SqlColumnType::UBIGINT => val.to_string().parse::<u64>().map(SqlValue::UBIGINT).map_err(|_| DbError::Parser(format!("Invalid UBIGINT literal: {:?}", val))),
        SqlColumnType::UTINYINT => val.to_string().parse::<u8>().map(SqlValue::UTINYINT).map_err(|_| DbError::Parser(format!("Invalid UTINYINT literal: {:?}", val))),
        SqlColumnType::USMALLINT => val.to_string().parse::<u16>().map(SqlValue::USMALLINT).map_err(|_| DbError::Parser(format!("Invalid USMALLINT literal: {:?}", val))),
        SqlColumnType::UINT => val.to_string().parse::<u32>().map(SqlValue::UINT).map_err(|_| DbError::Parser(format!("Invalid UINT literal: {:?}", val))),
        SqlColumnType::FLOAT => val.to_string().parse::<f32>().map(SqlValue::FLOAT).map_err(|_| DbError::Parser(format!("Invalid FLOAT literal: {:?}", val))),
        SqlColumnType::DOUBLE => val.to_string().parse::<f64>().map(SqlValue::DOUBLE).map_err(|_| DbError::Parser(format!("Invalid DOUBLE literal: {:?}", val))),
        SqlColumnType::CHAR => Ok(SqlValue::CHAR(val.to_string().into_bytes())),
        SqlColumnType::VARCHAR => Ok(SqlValue::VARCHAR(val.to_string().into_bytes())),
    }
}

// 3) helper to turn any SqlValue into f64 (falling back to lexicographic)
pub fn to_f64_or_nan(v: &SqlValue) -> f64 {
    match v {
        SqlValue::TINYINT(x)   => *x as f64,
        SqlValue::SMALLINT(x)  => *x as f64,
        SqlValue::INT(x)       => *x as f64,
        SqlValue::BIGINT(x)       => *x as f64,
        SqlValue::UBIGINT(x)       => *x as f64,
        SqlValue::UINT(x)      => *x as f64,
        SqlValue::USMALLINT(x)     => *x as f64,
        SqlValue::UTINYINT(x)     => *x as f64,
        SqlValue::FLOAT(x)     => *x as f64,
        SqlValue::DOUBLE(x)    => *x,
        SqlValue::CHAR(bs) 
        | SqlValue::VARCHAR(bs) => {
            let s = String::from_utf8_lossy(bs);
            // try integer first
            if let Ok(i) = s.parse::<i64>() {
                i as f64
            } else if let Ok(f) = s.parse::<f64>() {
                f
            } else {
                std::f64::NAN
            }
        }
    }
}


fn find_column_idx(schema: &TableSchema, name: &str) -> Option<usize> {
    // Try exact match
    if let Some(idx) = schema.name_col.iter().position(|c| c == name) {
        return Some(idx);
    }
    // Try suffix match for "A.x" (table.col)
    if let Some(dot_pos) = name.find('.') {
        let col = &name[dot_pos + 1..];
        schema.name_col.iter().position(|c| c == col)
    } else {
        None
    }
}

fn collect_output_columns( // SAME as NEW PROjection
                    plan: &PlanNode,
                    db_page: &DatabasePage,
                ) -> Result<Vec<String>, DbError> {
                    match plan {
                        PlanNode::Join { left, right, .. } => {
                            let left_schema = collect_output_columns(left, db_page)?;
                            let right_schema = collect_output_columns(right, db_page)?;
                            Ok(left_schema.into_iter().chain(right_schema).collect())
                        }
                        PlanNode::TableScan { table }
                        | PlanNode::IndexScan { table, .. } => {
                            db_page
                                .all_tableroot
                                .iter()
                                .find(|t| t.tb_schema.name == *table)
                                .ok_or_else(|| DbError::DbEngine(format!("execute_plan: table '{}' not found", table)))?
                                .tb_schema
                                .name_col
                                .iter()
                                .map(|c| format!("{}.{}", table, c))
                                .collect::<Vec<_>>()
                                .pipe(Ok) // Wrap in Ok for Result return type
                        }
                        PlanNode::Filter { input, .. }
                        | PlanNode::Projection { input, .. }
                        | PlanNode::OrderBy { input, .. }
                        | PlanNode::HavingFilter { input, .. }
                        | PlanNode::GroupBy { input, .. } => {
                            collect_output_columns(input, db_page)
                        }
                        _ => Err(DbError::DbEngine(format!("Unsupported input node for projection schema: {:?}", plan))),
                    }
                }

fn resolve_column<'a>(columns: &'a [String], target: &str) -> Option<usize> {
    // First, try for an exact match
    if let Some(idx) = columns.iter().position(|c| c == target) {
        return Some(idx);
    }
    // Next, try for suffix match (after '.')
    let mut found: Option<usize> = None;
    for (i, c) in columns.iter().enumerate() {
        if let Some(pos) = c.rfind('.') {
            if &c[pos + 1..] == target {
                if found.is_some() {
                    // ambiguous match!
                    return None;
                }
                found = Some(i);
            }
        }
    }
    found
}

fn strip_order_by(
    plan: &PlanNode,
) -> (Box<PlanNode>, Vec<(Expression, bool)>) {
    match plan {
        PlanNode::Projection { expressions, input } => {
            let (inner, orderings) = strip_order_by(input.as_ref());
            let new_proj = PlanNode::Projection {
                expressions: expressions.clone(),
                input: inner.clone(),
            };
            (Box::new(new_proj), orderings)
        }
        PlanNode::OrderBy { orderings, input } => {
            // drop the OrderBy, keep its child & capture the orderings
            (input.clone(), orderings.clone())
        }
        // anything else: no ORDER BY here
        other => (Box::new(other.clone()), Vec::new()),
    }
}
// Helper for `collect_output_columns`
trait Pipe<T> {
    fn pipe<U, F>(self, f: F) -> U
    where
        F: FnOnce(T) -> U;
}

impl<T> Pipe<T> for T {
    fn pipe<U, F>(self, f: F) -> U
    where
        F: FnOnce(T) -> U,
    {
        f(self)
    }
}
//! BTree Module
//! First of all, i have to say that this implemention is not
//! perfect at all for scanning BTree file in disk
//! Because we need a real rebalance algorithm that put root node
//! on the offset 0 ever, operation that this implemention not doing.
//! This implementation simply move root node when is full and rewrite
//! root node on the first offset free. ( [1024] bytes )

use std::fmt::Error;
use std::fs::{File};
use std::io::{Seek, SeekFrom, Write, Read};
use std::cmp::Ordering;
use crate::bintuco;
use crate::page::{self, SqlValue};
use std::path::{Path, PathBuf};
use crate::constant::{BTREE_INIT_SPACE, PAGE_CAPACITY, NODE_CAPACITY, PAGE_SIZE};
use crate::dbengine::*;
use crate::dberror::DbError;


///+----------------------------------------------------------------------------------------+
///|                               BTreeNode  - 1024 bytes                                  |
///+----------------------------------------------------------------------------------------+
///|                                    Serialized Data                                     |
///|                                                                                        |
///| +-------------------------+                                                            |
///| | is_root: bool           |                                                            |
///| | is_leaf: false          | // This flag is false for internal nodes                   |
///| | keys: Vec<Triplet>      | // Keys to guide search                                    | 
///| | children: Vec<usize>    | // Page indices of child nodes                             |
///| +-------------------------+                                                            |
///|                                                                                        |
///|   ideal_Layout: [Pointer_0] [Key_1] [Pointer_1]... [Key_N] [Pointer_N]                 |
///|                                                                                        |
///|   - Each Triplet contains: { key: SqlValue, page: usize, rowid: usize }                |
///|   - Pointer_i is an index to a child page where keys are < Key_{i+1}.                  |
///|   - Key_i is a Triplet, but only its `key` value is used for comparison.               |
///|   - The (page, rowid) pair is the direct pointer to the data row.                      |
///|                                                                                        |
///+----------------------------------------------------------------------------------------+
///|                                   Free Space (Padding)                                 |
///+----------------------------------------------------------------------------------------+



/// /// Parent Node P
/// /// +-----------------------------+
/// /// | ... | Ptr_L | Key_X | ... |
/// /// +-----------------------------+
/// ///         |
/// ///         V
/// /// Leaf Node L (Full)
/// /// +-----------------------------------+
/// /// | K_1 | V_1 | K_2 | V_2 | K_3 | V_3 |
/// /// +-----------------------------------+
/// ///
/// ///            After
/// ///
/// /// Parent Node P (Updated)
/// /// +-------------------------------------------------+
/// /// | ... | Ptr_L | Promoted_Key | Ptr_L' | Key_X | ... |
/// /// +-------------------------------------------------+
/// ///         |                      |
/// ///         |                      V
/// ///         |                New Leaf Node L'
/// ///         |                +-----------------+
/// ///         |                | K_3 | V_3 | ... |
/// ///         |                +-----------------+
/// ///         V
/// /// Original Leaf Node L (Now half-full)
/// /// +-----------------------+
/// /// | K_1 | V_1 | K_2 | V_2 |
/// /// +-----------------------+
/// 
 

/// /// /// # Visualizing a Root Split (See first comment section):
/// /// /// Let's trace the state of the B-Tree file when inserting the key `25` into
/// /// /// a full root node.
/// ///
/// /// ///
/// /// /// 1. State Before Split
/// /// /// The B-Tree has one node at page 0, which is the root and is full.
/// /// /// The `Btree` struct points to it.
/// ///
/// /// ///   B-Tree File                BTree Struct
/// /// /// +---------------------+      +-----------------------------+
/// /// /// | Page 0 (Root, Leaf) | <--- | root_index: 0               |
/// /// /// | Keys: [10, 20, 30]  |      | node_counter: 1             |
/// /// /// +---------------------+      +-----------------------------+
/// /// /// | Page 1 (Free)       |
/// /// /// +---------------------+
/// /// /// | Page 2 (Free)       |
/// /// /// +---------------------+
/// ///
/// ///
/// /// 2. State After Split
/// /// Inserting `25` forces the node at page 0 to split.
/// ///   a. The keys are divided. The old root (page 0) keeps `[10, 20]`.
/// ///   b. A new sibling leaf is created at the next free page (page 1) and gets `[30]`.
/// ///   c. The middle key, `25`, is promoted upwards.
/// ///   d. Since there is no parent, a brand new root is created at the next
/// ///      free page (page 2). This new root contains the promoted key and pointers
/// ///      to the two children (page 0 and page 1).
/// ///   e. The `Btree` struct is updated to point to the new root at page 2.
/// ///
/// /// ///   B-Tree File                          BTree Struct
/// /// /// +---------------------------+        +-----------------------------+
/// /// /// | Page 0 (Now a Leaf)       |        | root_index: 2               |
/// /// /// | Keys: [10, 20]            |        | node_counter: 3             |
/// /// /// | Parent: 2                 |        +-----------------------------+
/// /// /// +---------------------------+                        ^
/// /// /// | Page 1 (New Sibling Leaf) |                        |
/// /// /// | Keys: [30]                |                        |
/// /// /// | Parent: 2                 |                        |
/// /// /// +---------------------------+                        |
/// /// /// | Page 2 (NEW ROOT)         | -----------------------+
/// /// /// | Keys: [25]                |
/// /// /// | Children: [0, 1]          |
/// /// /// +---------------------------+
///





// ══════════════════════════════════════ A TRIPLET DEFINITION ══════════════════════════════════════

/// Why Triplet?-> Store key -> [Page_num + row_id]
/// Triplet struct with key K
/// Page and rowid member are for search inside table 
#[derive(Debug, Clone)]
pub struct Triplet{
    pub key: SqlValue,
    pub rowid: usize,
    pub page: usize,
}

#[derive(Debug, Clone)]
pub enum FindResult {
    ExactMatch(Triplet),
    ChildIndex(usize),
}

// ══════════════════════════════════════ BTree NODE DEFINITION ══════════════════════════════════════

/// Node for a btree [1024] bytes
#[repr(C, align(1024))]
#[derive(Debug, Clone)]
pub struct BtreeNode {
    pub is_root: bool,
    pub parent_idx: usize,
    pub index: usize,
    pub is_leaf: bool,
    pub free_space: usize,
    pub keys: Vec<Triplet>,
    pub children: Vec<usize>,
}

impl BtreeNode {
    pub fn new(index: usize, parent: usize) -> Self {
        BtreeNode {
            is_root: false,
            parent_idx: parent,
            index,
            is_leaf: true,
            free_space: BTREE_INIT_SPACE,
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn add(&mut self, triplet: Triplet) -> Result<(), DbError> {
        self.keys.push(triplet);
        self.keys.sort_by(|a, b| {
            a.key.partial_cmp(&b.key)
                .unwrap_or(Ordering::Equal) // Treat incomparable as equal for sorting
        });
        Ok(())
    }
    pub fn find_child_index(&self, triplet: &Triplet) -> Result<usize, DbError> {
        // Assume self.keys is a sorted Vec of keys
        for (i, i_key) in self.keys.iter().enumerate() {
            if triplet.key < i_key.key {
                return self.children.get(i)
                    .cloned()
                    .ok_or_else(|| DbError::BTree(format!("Child index {} out of bounds for node {}", i, self.index)));
            }
        }
        // Handle the case where children is unexpectedly empty
        self.children.last()
            .cloned()
            .ok_or_else(|| DbError::BTree(format!(
                "BTreeNode {} has no children but was treated as internal node. Keys: {:?}",
                self.index, self.keys
            )))
    }

    pub fn find_child_read(&self, sql_value: &SqlValue) -> Result<FindResult, DbError> {
        for (i, triplet) in self.keys.iter().enumerate() {
            if *sql_value == triplet.key {
                return Ok(FindResult::ExactMatch(triplet.clone()));
            } else if *sql_value < triplet.key {
                return self.children.get(i)
                    .map(|&idx| FindResult::ChildIndex(idx))
                    .ok_or_else(|| DbError::BTree(format!("Child index {} out of bounds for node {}", i, self.index)));
            }
        }
        self.children.last()
            .map(|&last_child| FindResult::ChildIndex(last_child))
            .ok_or_else(|| DbError::BTree(format!("BTreeNode {} has no children but was treated as internal node. Keys: {:?}", self.index, self.keys)))
    }


}

// ══════════════════════════════════════ BTree DEFINITION ══════════════════════════════════════

///BTree def
#[derive(Debug, Clone)]
pub struct Btree {
    pub key_name: String,
    pub btree_name_file: String,
    pub node_counter: usize,
    pub root_index: usize,
}

impl Btree{
    /// Now takes a &StorageManager instead of opening its own File.
    pub fn new(
        key_name: String,
        _db_folder: &Path, // db_folder is not directly used here after refactoring storage
        storage: &StorageManager,
    ) -> Result<Self, DbError> {
        // 1) Build the logical filename, e.g. "users.idx"
        let btree_name_file = format!("{}.idx", key_name);
        let logical_name = btree_name_file.clone();

        // 2) We assume the caller has already done `storage.register_file(&logical_name)?`.
        //    Now grab the handle. If it isn’t registered, that’s an error:
        let handle = storage.get_handle(&logical_name).ok_or_else(|| {
            DbError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("B-tree file not registered: {}", logical_name),
            ))
        })?;

        // 3) Look at the on‐disk length. If it’s zero, we must write a fresh empty root.
        //    If it’s nonzero, we decode the existing page 0.
        let file_len = handle.file_len()?;

        if file_len == 0 {
            // ─── File is brand‐new. Write an empty root node at page 0 ───

            // 3a) Create a brand‐new empty node. In your code, you used `BtreeNode::new(0, 0)`
            //     to construct a root node with index 0. We do exactly the same here.
            let mut empty_root = BtreeNode::new(0, 0);
            empty_root.is_root = true;

            // 3b) Serialize it into a 1024‐byte buffer
            let mut buf = vec![0u8; PAGE_SIZE];
            let encoded = bintuco::encode_to_vec(&empty_root)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;
            buf[..encoded.len()].copy_from_slice(&encoded);

            // 3c) Use `storage.write_page(...)` so that journaling (if enabled) is respected.
            storage.write_page(&btree_name_file, 0, &buf)?;

            // 3d) Return a fresh Btree struct pointing at root_index = 0, node_counter = 1
            Ok(Self {
                key_name,
                btree_name_file,
                node_counter: 1,
                root_index: 0,
            })
        } else {
            // ─── File already has ≥1 page. Decode page 0 from disk ───

            // 4a) Read exactly PAGE_SIZE bytes from offset 0
            let mut buf = vec![0u8; PAGE_SIZE];
            handle.read_at(0, &mut buf)?;

            // 4b) Decode them into a BtreeNode
            let (root_node, _) = bintuco::decode_from_slice::<BtreeNode>(&buf)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;

            if !root_node.is_leaf && root_node.children.is_empty() {
                eprintln!(
                    "WARNING: B-Tree root for '{}' was corrupt. Correcting to a leaf node.",
                    key_name
                );
            }

            let existing_pages = (file_len as usize + PAGE_SIZE - 1) / PAGE_SIZE;
            let next_counter = existing_pages; 
            //   • If file_len = 1024, existing_pages = 1, so next_counter = 1
            //   • If file_len = 3072, existing_pages = 3, so next_counter = 3
            // That way, on your next insertion you allocate page 3, etc.

            Ok(Self {
                key_name,
                btree_name_file,
                // node_counter = “first unused page index”
                node_counter: next_counter,
                // We always assume the on‐disk root is page 0
                root_index: 0,
            })
        }
    }

    /// Refactored to use StorageManager instead of opening its own File.
    pub fn search_value_at_least(
        &mut self,
        sql_value: SqlValue,
        storage: &StorageManager,
    ) -> Result<Option<(BtreeNode, Option<Triplet>)>, DbError> {
        // Build the same logical filename you registered at creation time:
        let logical_name = self.btree_name_file.clone();

        // Grab the FileHandle from StorageManager:
        let handle = storage.get_handle(&logical_name).ok_or_else(|| {
            DbError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("B-tree file not registered: {}", logical_name),
            ))
        })?;

        let mut index = self.root_index;

        loop {
            // Read the entire 1024-byte page out of the handle:
            let mut buf = vec![0u8; PAGE_SIZE];
            handle.read_at((index * PAGE_SIZE) as u64, &mut buf)?;

            // Decode one BtreeNode from those 1024 bytes
            let (node, _): (BtreeNode, usize) = bintuco::decode_from_slice(&buf)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;

            if node.is_leaf {
                // Once we hit a leaf, scan for the smallest key >= sql_value
                let mut candidate: Option<Triplet> = None;
                for triplet in &node.keys {
                    if let Some(cmp) = triplet.key.partial_cmp(&sql_value) {
                        if cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal {
                            match &candidate {
                                None => candidate = Some(triplet.clone()),
                                Some(current) => {
                                    if let Some(cmp2) = triplet.key.partial_cmp(&current.key) {
                                        if cmp2 == std::cmp::Ordering::Less {
                                            candidate = Some(triplet.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                return Ok(Some((node, candidate)));
            } else {
                // Internal node: decide which child page to descend into
                let pos = node
                    .keys
                    .iter()
                    .position(|t| t.key >= sql_value);

                index = match pos {
                    Some(p) => node.children.get(p)
                        .cloned()
                        .ok_or_else(|| DbError::BTree(format!("Child index {} out of bounds for node {}", p, node.index)))?,
                    None => node.children.last()
                        .cloned()
                        .ok_or_else(|| DbError::BTree(format!("BTreeNode {} has no children but was treated as internal node. Keys: {:?}", node.index, node.keys)))?,
                };
            }
        }
    }

    /// Refactored to use StorageManager instead of opening its own File.
    pub fn search_and_remove_value_at_least(
        &mut self,
        sql_value: SqlValue,
        storage: &StorageManager,
    ) -> Result<Option<(BtreeNode, Option<Triplet>)>, DbError> {
        // Build the same logical filename you registered in `new`:
        let logical_name = self.btree_name_file.clone();

        // Grab the FileHandle from StorageManager:
        let handle = storage.get_handle(&logical_name).ok_or_else(|| {
            DbError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("B-tree file not registered: {}", logical_name),
            ))
        })?;

        let mut index = self.root_index;

        loop {
            // 1) Read the 1024-byte node from disk:
            
            let mut buf = vec![0u8; PAGE_SIZE];
            handle.read_at((index * PAGE_SIZE) as u64, &mut buf)?;

            // 2) Decode it into a BtreeNode
            let (mut node, _): (BtreeNode, usize) = bintuco::decode_from_slice(&buf)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;

            if node.is_leaf {
                // We’re at a leaf. Find the first key >= sql_value
                let candidate_pos = node.keys.iter()
                    .position(|triplet| triplet.key >= sql_value);

                if let Some(pos) = candidate_pos {
                    // 3) Remove and return that Triplet
                    let removed = node.keys.remove(pos);

                    // 4) Re-serialize this updated node back into a 1024-byte buffer
                    let mut write_buf = vec![0u8; PAGE_SIZE];
                    let encoded = bintuco::encode_to_vec(&node)
                        .map_err(|e| DbError::Bintuco(e.to_string()))?;
                    write_buf[..encoded.len()].copy_from_slice(&encoded);

                    // 5) Overwrite that node’s page via write_page (journaling + write)
                    let offset = (index * PAGE_SIZE) as u64;
                    storage.write_page(&self.btree_name_file, offset, &write_buf)?;


                    return Ok(Some((node, Some(removed))));
                } else {
                    // No key ≥ sql_value here
                    return Ok(Some((node, None)));
                }
            } else {
                // Internal node: decide which child to descend into
                let pos = node.keys.iter().position(|t| t.key >= sql_value);
                index = if let Some(p) = pos {
                    node.children.get(p)
                        .cloned()
                        .ok_or_else(|| DbError::BTree(format!("Child index {} out of bounds for node {}", p, node.index)))?
                } else {
                    node.children.last()
                        .cloned()
                        .ok_or_else(|| DbError::BTree(format!("BTreeNode {} has no children but was treated as internal node. Keys: {:?}", node.index, node.keys)))?
                };
            }
        }
    }



    // Refactored to use StorageManager instead of opening its own File.
    pub fn search_value(
        &self,
        sql_value: SqlValue,
        storage: &StorageManager,
    ) -> Result<Option<(BtreeNode, Option<Triplet>)>, DbError> {
        // Build the same logical filename registered earlier:
        let logical_name = self.btree_name_file.clone();
        let handle = storage.get_handle(&logical_name).ok_or_else(|| {
            DbError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("B-tree file not registered: {}", logical_name),
            ))
        })?;

        let mut index = self.root_index;

        loop {
            // Read the full 1024-byte page
            let mut buf = vec![0u8; PAGE_SIZE];
            handle.read_at((index * PAGE_SIZE) as u64, &mut buf)?;

            // Decode into a BtreeNode
            let (node, _): (BtreeNode, usize) = bintuco::decode_from_slice(&buf)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;

            if node.is_leaf {
                // At a leaf, look for an exact match
                let triplet_opt = node
                    .keys
                    .iter()
                    .find(|t| t.key == sql_value)
                    .cloned();

                return Ok(Some((node, triplet_opt)));
            } else {
                // Not a leaf: use find_child_read to decide where to go
                match node.find_child_read(&sql_value)? {
                    FindResult::ExactMatch(triplet) => {
                        return Ok(Some((node, Some(triplet))));
                    }
                    FindResult::ChildIndex(child_idx) => {
                        index = child_idx;
                    }
                }
            }
        }
    }
    

    /// Traverse from the root down to the leaf that should contain `triplet.key`.
    ///
    /// Returns the leaf node that we found (to which you can then insert).
    pub fn search_node(
        &self,
        triplet: &Triplet,
        storage: &StorageManager,
    ) -> Result<BtreeNode, DbError> {
        // Build the logical filename and get the FileHandle:
        let logical_name = self.btree_name_file.clone();
        let handle = storage.get_handle(&logical_name).ok_or_else(|| {
            DbError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("B-tree file not registered: {}", logical_name),
            ))
        })?;

        let mut index = self.root_index;

        loop {
            // 1) Read the 1024-byte page corresponding to `index`
            let mut buf = vec![0u8; PAGE_SIZE];
            handle.read_at((index * PAGE_SIZE) as u64, &mut buf)?;

            // 2) Decode into a BtreeNode
            let (node, _): (BtreeNode, usize) = bintuco::decode_from_slice(&buf)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;

            if node.is_leaf {
                // We're at the leaf that should receive the new triplet
                return Ok(node);
            } else {
                // Internal node: pick the child index that leads downward
                index = node.find_child_index(triplet)?;
            }
        }
    }
}

impl Btree {
    /// Inserts a Triplet, splitting nodes if necessary.
    /// Now uses `storage` instead of opening files directly.
    pub fn insert(
        &mut self,
        ins_triplet: Triplet,
        storage: &StorageManager,
    ) -> Result<(), DbError> {
        // Build the logical filename and get the FileHandle:
        let logical_name = self.btree_name_file.clone();
        let _handle = storage.get_handle(&logical_name).ok_or_else(|| {
            DbError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("B-tree file not registered: {}", logical_name),
            ))
        })?;

        // 1) Find the leaf node to insert into
        let mut target_node = self.search_node(&ins_triplet, storage)?;

        // 2) Add the new key and sort
        target_node.add(ins_triplet.clone())?;
        target_node
            .keys
            .sort_by(|a, b| a.key.partial_cmp(&b.key).unwrap_or(Ordering::Equal)); // Unwrap here is safe as SqlValue has PartialOrd

        // 3) Serialize to check if it exceeds NODE_CAPACITY
        let encoded = bintuco::encode_to_vec(&target_node)
            .map_err(|e| DbError::Bintuco(e.to_string()))?;

        // Helper closure to write a BtreeNode at a given index:
        let write_node = |node: &BtreeNode, storage: &StorageManager, file_name: &str| -> Result<(), DbError> {
            let mut write_buf = vec![0u8; PAGE_SIZE];
            let enc = bintuco::encode_to_vec(node)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;
            write_buf[..enc.len()].copy_from_slice(&enc);
        
            // The storage.write_page function already returns Result<(), DbError>.
            // No need for .map_err(DbError::Io) here, as the error is already DbError.
            storage.write_page(file_name, (node.index * PAGE_SIZE) as u64, &write_buf)
        };

        if encoded.len() > NODE_CAPACITY {
            // Overflow: need to split leaf into `target_node` and a new `sibling`
            let sibling_idx = self.node_counter;
            self.node_counter += 1;
            let mut sibling = BtreeNode::new(sibling_idx, target_node.parent_idx);
            sibling.is_leaf = target_node.is_leaf;

            // Split keys in half
            let mid = target_node.keys.len() / 2;
            let promote_key = target_node.keys[mid].clone();

            // For a leaf, sibling gets keys from mid onward; target_node keeps the first half
            sibling.keys = target_node.keys.split_off(mid);

            // 4) Write both updated nodes back to disk
            write_node(&target_node, storage, &self.btree_name_file)?;
            write_node(&sibling, storage, &self.btree_name_file)?;

            if target_node.is_root {
                // 5a) If it was the root, create a brand-new root
                let new_root_idx = self.node_counter;
                self.node_counter += 1;
                let mut new_root = BtreeNode::new(new_root_idx, 0);
                new_root.is_root = true;
                new_root.is_leaf = false;
                new_root.keys.push(promote_key);
                new_root.children.push(target_node.index);
                new_root.children.push(sibling.index);

                // Update the old root and sibling parents
                target_node.is_root = false;
                target_node.parent_idx = new_root_idx;
                sibling.parent_idx = new_root_idx;

                write_node(&target_node, storage, &self.btree_name_file)?;
                write_node(&sibling, storage, &self.btree_name_file)?;
                write_node(&new_root, storage, &self.btree_name_file)?;

                self.root_index = new_root_idx;
            } else {
                // 5b) Otherwise, insert `promote_key` into the parent
                let mut parent = self.load_node(target_node.parent_idx, storage)?;
                parent.keys.push(promote_key);
                parent
                    .keys
                    .sort_by(|a, b| a.key.partial_cmp(&b.key).unwrap_or(Ordering::Equal)); // Unwrap here is safe

                if let Some(pos) = parent.children.iter().position(|&c| c == target_node.index)
                {
                    parent.children.insert(pos + 1, sibling.index);
                } else {
                    return Err(DbError::BTree(
                        format!(
                            "Child index {} not found in parent children {:?}",
                            target_node.index, parent.children
                        ),
                    ));
                }

                write_node(&parent, storage, &self.btree_name_file)?;
                self.insert_into_parent(storage, parent)?; // may recurse
            }
        } else {
            // Fits without split: just write the updated leaf back
            write_node(&target_node, storage, &self.btree_name_file)?;
        }

        Ok(())
    }

    // Recursive helper to split ancestor nodes when needed
    fn insert_into_parent(
        &mut self,
        storage: &StorageManager,
        mut node: BtreeNode,
    ) -> Result<(), DbError> {
        // Helper to write any node
        let logical_name = self.btree_name_file.clone();
        let _handle = storage.get_handle(&logical_name)
            .ok_or_else(|| DbError::BTree("B-tree file not registered".to_string()))?; // Using DbError::BTree for consistency
        
        let write_node = |n: &BtreeNode, storage: &StorageManager, file_name: &str| -> Result<(), DbError> {
            let mut write_buf = vec![0u8; PAGE_SIZE];
            let enc = bintuco::encode_to_vec(n)
                .map_err(|e| DbError::Bintuco(e.to_string()))?;
            write_buf[..enc.len()].copy_from_slice(&enc);
            // The storage.write_page function already returns Result<(), DbError>.
            // No need for .map_err(DbError::Io) here, as the error is already DbError.
            storage.write_page(file_name, (n.index * PAGE_SIZE) as u64, &write_buf)
        };

        let encoded = bintuco::encode_to_vec(&node)
            .map_err(|e| DbError::Bintuco(e.to_string()))?;

        if encoded.len() > NODE_CAPACITY {
            // Need to split this internal node
            let sibling_idx = self.node_counter;
            self.node_counter += 1;
            let mut sibling = BtreeNode::new(sibling_idx, node.parent_idx);
            sibling.is_leaf = node.is_leaf;

            // Split keys: promote the middle one
            let mid = node.keys.len() / 2;
            let promote_key = node.keys[mid].clone();

            if node.is_leaf {
                // For leaf, sibling takes keys from mid onward
                sibling.keys = node.keys.split_off(mid);
            } else {
                // For internal: sibling takes keys after mid, node keeps keys before mid
                sibling.keys = node.keys.split_off(mid + 1);
                node.keys.truncate(mid);

                // Split children: sibling gets the tail
                sibling.children = node.children.split_off(mid + 1);

                // Update children's parent pointers
                for &child_idx in &sibling.children {
                    let mut child = self.load_node(child_idx, storage)?;
                    child.parent_idx = sibling_idx;
                    write_node(&child, storage, &self.btree_name_file)?;
                }
            }

            // Write the split nodes
            write_node(&node, storage, &self.btree_name_file)?;
            write_node(&sibling, storage, &self.btree_name_file)?;

            if node.is_root {
                // Create new root
                let new_root_idx = self.node_counter;
                self.node_counter += 1;
                let mut new_root = BtreeNode::new(new_root_idx, 0);
                new_root.is_root = true;
                new_root.is_leaf = false;
                new_root.keys.push(promote_key);
                new_root.children.push(node.index);
                new_root.children.push(sibling.index);

                node.is_root = false;
                node.parent_idx = new_root_idx;
                sibling.parent_idx = new_root_idx;

                write_node(&node, storage, &self.btree_name_file)?;
                write_node(&sibling, storage, &self.btree_name_file)?;
                write_node(&new_root, storage, &self.btree_name_file)?;

                self.root_index = new_root_idx;
            } else {
                // Insert promoted key into this node’s parent
                let mut parent = self.load_node(node.parent_idx, storage)?;
                parent.keys.push(promote_key);
                parent
                    .keys
                    .sort_by(|a, b| a.key.partial_cmp(&b.key).unwrap_or(Ordering::Equal)); // Unwrap is safe

                if let Some(pos) = parent.children.iter().position(|&c| c == node.index) {
                    parent.children.insert(pos + 1, sibling.index);
                } else {
                    return Err(DbError::BTree(
                        format!(
                            "Child index {} not found in parent children {:?}",
                            node.index, parent.children
                        ),
                    ));
                }

                write_node(&parent, storage, &self.btree_name_file)?;
                self.insert_into_parent(storage, parent)?; // recurse
            }
        } else {
            // No split needed—just rewrite the node
            write_node(&node, storage, &self.btree_name_file)?;
        }

        Ok(())
    }
}

    
impl Btree {

    pub fn validate_node(&self, node: &BtreeNode) -> Result<(), DbError> {
        if node.keys.is_empty() && node.children.is_empty() && !node.is_leaf {
            // This case specifically prevents an internal node from having no keys and no children.
            // A root node can be empty, but it must be a leaf if it's empty.
            if node.is_root {
                // An empty root node should be a leaf.
                if !node.is_leaf {
                     return Err(DbError::BTree(format!(
                        "Root BTreeNode {} is not a leaf but has no keys or children. Keys: {:?}, Children: {:?}",
                        node.index, node.keys, node.children
                    )));
                }
            } else {
                return Err(DbError::BTree(format!(
                    "Internal BTreeNode {} has no children but was treated as internal node. Keys: {:?}, Children: {:?}",
                    node.index, node.keys, node.children
                )));
            }
        }
        Ok(())
    }

    /// Print every node’s raw contents in the B-tree. Uses StorageManager.
    pub fn debug_print_all_nodes(
        &self,
        storage: &StorageManager,
    ) -> Result<(), DbError> {
        println!("All BTree Nodes (raw):");

        let mut index = 0;
        loop {
            // Attempt to load node; stop when `load_node` errors
            match self.load_node(index, storage) {
                Ok(node) => {
                    println!(
                        "Node {} [is_leaf: {}, free_space: {}]",
                        node.index, node.is_leaf, node.free_space
                    );
                    for triplet in &node.keys {
                        println!("  Key: {:?}", triplet.key);
                    }
                    index += 1;
                }
                Err(e) => {
                    // Check if the error is genuinely due to end of file/not found,
                    // or a more serious decoding/IO error.
                    if let DbError::Io(io_err) = &e {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof || io_err.kind() == std::io::ErrorKind::NotFound {
                            break; // Reached end of file or page doesn't exist, stop
                        }
                    }
                    return Err(e); // Propagate other errors
                }
            }
        }
        Ok(())
    }
}

impl Btree {
    fn print_node_recursive2(
        &self,
        storage: &StorageManager,
        index: usize,
        depth: usize,
    ) -> Result<(), DbError> {
        // Load the node from disk using StorageManager
        let node = self.load_node(index, storage)?;

        // Indent by depth (2 spaces per level)
        let indent = "  ".repeat(depth);

        // Print current page and its children indexes (if any)
        if node.children.is_empty() {
            println!("{}PAGE {} (leaf)", indent, node.index);
        } else {
            let children_str = node
                .children
                .iter()
                .map(|child_idx| child_idx.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            println!("{}PAGE {} -> Children: [{}]", indent, node.index, children_str);
        }

        // Recurse into children if not a leaf
        if !node.is_leaf {
            for &child_index in &node.children {
                self.print_node_recursive2(storage, child_index, depth + 1)?;
            }
        }
        Ok(())
    }
}


impl Btree {
    /// Load a single node (by index) via StorageManager’s FileHandle.
    pub fn load_node(
        &self,
        index: usize,
        storage: &StorageManager,
    ) -> Result<BtreeNode, DbError> {
        // Get the same logical name used elsewhere:
        let logical_name = self.btree_name_file.clone();
        let handle = storage.get_handle(&logical_name).ok_or_else(|| {
            DbError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("B-tree file not registered: {}", logical_name),
            ))
        })?;

        // Read the 1024-byte page
        let mut buf = vec![0u8; PAGE_SIZE];
        handle.read_at((index * PAGE_SIZE) as u64, &mut buf)?;

        // Decode into BtreeNode
        let (node, _) = bintuco::decode_from_slice(&buf)
            .map_err(|e| DbError::Bintuco(e.to_string()))?;
        Ok(node)
    }
}

impl Btree {
    /// Returns an iterator over all Triplets in the Btree (in sorted order).
    /// Now takes a `&StorageManager` instead of `&Path`.
    pub fn iter_triplets<'a>(
        &'a self,
        storage: &'a StorageManager,
    ) -> Result<BtreeTripletIter<'a>, DbError> {
        // Load the root node using our new load_node(storage)
        let root = self.load_node(self.root_index, storage)?;
        Ok(BtreeTripletIter {
            btree: self,
            storage,
            stack: vec![(root, 0)],
        })
    }

    /// Returns an iterator over page indices (usize) for every Triplet.
    pub fn all_pages<'a>(
        &'a self,
        storage: &'a StorageManager,
    ) -> Result<impl Iterator<Item = Result<usize, DbError>> + 'a, DbError> {
        let triplet_iter = self.iter_triplets(storage)?;
        Ok(triplet_iter.map(|res| res.map(|triplet| triplet.page)))
    }
}

// ══════════════════════════════════════ BTree Triplet Iterator ══════════════════════════════════════

/// I implemented it for method search_value_at_list, but the idea is use it as alternative
/// to TableScan when the scan is doing over a primary key or an index. But is TODO
pub struct BtreeTripletIter<'a> {
    btree: &'a Btree,
    storage: &'a StorageManager,
    stack: Vec<(BtreeNode, usize)>, // (node, next_key_index)
}

impl<'a> BtreeTripletIter<'a> {
    /// Construct by loading the root via StorageManager.
    pub fn new(
        btree: &'a Btree,
        storage: &'a StorageManager,
    ) -> Result<Self, DbError> {
        let root = btree.load_node(btree.root_index, storage)?;
        Ok(Self {
            btree,
            storage,
            stack: vec![(root, 0)],
        })
    }

    /// Push the leftmost path from this node (for an in-order traversal).
    fn push_leftmost_path(&mut self, mut node: BtreeNode) -> Result<(), DbError> {
        while !node.is_leaf {
            if node.children.is_empty() {
                break;
            }
            let child_index = node.children[0];
            self.stack.push((node.clone(), 0));
            node = self.btree.load_node(child_index, self.storage)?;
        }
        self.stack.push((node, 0));
        Ok(())
    }
}

impl<'a> Iterator for BtreeTripletIter<'a> {
    type Item = Result<Triplet, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (node, key_idx) = match self.stack.pop() {
                Some(val) => val,
                None => return None,
            };

            if key_idx < node.keys.len() {
                let triplet = node.keys[key_idx].clone();

                // If internal node and there's a right child to explore:
                if !node.is_leaf && (key_idx + 1) < node.children.len() {
                    // Push this node back with next key index
                    self.stack.push((node.clone(), key_idx + 1));

                    // Load the child node and then push its leftmost path
                    match self.btree.load_node(node.children[key_idx + 1], self.storage) {
                        Ok(child_node) => {
                            let mut temp_node = child_node;
                            while !temp_node.is_leaf {
                                if temp_node.children.is_empty() {
                                    break;
                                }
                                let next_child = temp_node.children[0];
                                self.stack.push((temp_node.clone(), 0));
                                match self.btree.load_node(next_child, self.storage) {
                                    Ok(cn) => temp_node = cn,
                                    Err(e) => return Some(Err(e)),
                                }
                            }
                            self.stack.push((temp_node, 0));
                            return Some(Ok(triplet));
                        }
                        Err(e) => return Some(Err(e)),
                    }
                } else {
                    // Leaf node or no further child: just push this node back with next key index
                    self.stack.push((node.clone(), key_idx + 1));
                    return Some(Ok(triplet));
                }
            }
            // Otherwise, key_idx >= node.keys.len(): keep popping
        }
    }
}
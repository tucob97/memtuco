//! Bintuco is a simple binary encoding/decoding module designed to serialize and deserialize
//! Rust data structures into a compact binary format for disk storage.
//!
//! ## How Bintuco Works
//!
//! - **Primitives:**  
//!   All integer and floating-point types are encoded in little-endian byte order, using
//!   their fixed-size representation in bytes. For example, `u32` always uses 4 bytes, `f64` uses 8 bytes, etc.
//!
//! - **Booleans:**  
//!   Encoded as a single byte: `0` for `false`, `1` for `true`.
//!
//! - **Strings:**  
//!   Encoded as a 4-byte `u32` length prefix (little-endian), followed by the UTF-8 bytes of the string.
//!
//! - **Vectors:**  
//!   Encoded as a 4-byte `u32` length prefix, then each element is encoded in sequence.
//!
//! - **Option\<T\>:**  
//!   Encoded as a single byte tag: `0` for `None`, `1` for `Some`, followed by the encoding of the inner value if present.
//!
//! - **HashMap:**  
//!   Encoded as a 4-byte `u32` length prefix (number of entries), then each key and value is encoded in sequence.
//!
//! - **Custom Structs and Enums:**  
//!   Encoded field by field, in the order defined in the struct. Enums use a tag byte to indicate the variant,
//!   followed by the encoded fields for that variant (if any).
//!
//! ## Decoding
//!
//! Decoding always follows the same order and types used during encoding. Bintuco does not include any
//! schema or type information in the binary, so encoding and decoding logic must match exactly.
//!
//! ## Endian-ness
//!
//! All numbers are encoded as little-endian. This means the least significant byte comes first in the byte stream.
//!
//! Bintuco is a module I implemented to eliminate dependencies on other libraries like `bincode` or `serde`.
//! It is not as performant or fast as those solutions, and is implemented for only the struct defined.

// src/bintuco.rs

use crate::page::{SqlValue, SqlColumnType, Row, Cell, OverflowPage, TablePage, DatabasePage, TableRoot, TableSchema};
use crate::btree::{Triplet, Btree, BtreeNode, FindResult};
use std::collections::HashMap;
use std::path::PathBuf;
use crate::dberror::DbError;
/// Main Traits
pub trait BintucoEncode {
    fn encode(&self, out: &mut Vec<u8>);
}
pub trait BintucoDecode: Sized {
    fn decode(input: &[u8]) -> Option<(Self, usize)>;
}

/// Drop-in API: just like bincode
pub fn encode_to_vec<T: BintucoEncode>(value: &T) -> Result<Vec<u8>, DbError> {
    let mut buf = Vec::new();
    value.encode(&mut buf);
    Ok(buf)
}
pub fn decode_from_slice<'a, T: BintucoDecode>(bytes: &'a [u8]) -> Result<(T, usize), DbError> {
    T::decode(bytes).ok_or_else(|| DbError::Bintuco("Decode error".to_string()))
}


// ----------- Primitives ------------

impl BintucoEncode for bool {
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(if *self { 1 } else { 0 });
    }
}
impl BintucoDecode for bool {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        input.get(0).map(|&b| (b != 0, 1))
    }
}

macro_rules! encode_primitive {
    ($t:ty, $size:expr, $to_bytes:ident, $from_bytes:ident) => {
        impl BintucoEncode for $t {
            fn encode(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.$to_bytes());
            }
        }
        impl BintucoDecode for $t {
            fn decode(input: &[u8]) -> Option<(Self, usize)> {
                if input.len() < $size { return None; }
                let mut arr = [0u8; $size];
                arr.copy_from_slice(&input[0..$size]);
                Some((<$t>::$from_bytes(arr), $size))
            }
        }
    };
}

encode_primitive!(u8, 1, to_le_bytes, from_le_bytes);
encode_primitive!(u16, 2, to_le_bytes, from_le_bytes);
encode_primitive!(u32, 4, to_le_bytes, from_le_bytes);
encode_primitive!(u64, 8, to_le_bytes, from_le_bytes);
encode_primitive!(i8, 1, to_le_bytes, from_le_bytes);
encode_primitive!(i16, 2, to_le_bytes, from_le_bytes);
encode_primitive!(i32, 4, to_le_bytes, from_le_bytes);
encode_primitive!(i64, 8, to_le_bytes, from_le_bytes);
encode_primitive!(f32, 4, to_le_bytes, from_le_bytes);
encode_primitive!(f64, 8, to_le_bytes, from_le_bytes);

// usize as u64
impl BintucoEncode for usize {
    fn encode(&self, out: &mut Vec<u8>) {
        (*self as u64).encode(out);
    }
}
impl BintucoDecode for usize {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (v, n) = u64::decode(input)?;
        Some((v as usize, n))
    }
}

// ----------- String ---------------

impl BintucoEncode for String {
    fn encode(&self, out: &mut Vec<u8>) {
        let bytes = self.as_bytes();
        (bytes.len() as u32).encode(out);
        out.extend_from_slice(bytes);
    }
}
impl BintucoDecode for String {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (len, n) = u32::decode(input)?;
        let len = len as usize;
        if input.len() < n + len { return None; }
        let s = String::from_utf8(input[n..n+len].to_vec()).ok()?;
        Some((s, n + len))
    }
}

// ----------- Vec<T> ---------------

impl<T: BintucoEncode> BintucoEncode for Vec<T> {
    fn encode(&self, out: &mut Vec<u8>) {
        (self.len() as u32).encode(out);
        for item in self {
            item.encode(out);
        }
    }
}
impl<T: BintucoDecode> BintucoDecode for Vec<T> {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (len, mut n) = u32::decode(input)?;
        let mut vec = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let (item, used) = T::decode(&input[n..])?;
            vec.push(item);
            n += used;
        }
        Some((vec, n))
    }
}

// ----------- Option<T> ------------

impl<T: BintucoEncode> BintucoEncode for Option<T> {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Some(v) => {
                1u8.encode(out);
                v.encode(out);
            },
            None => 0u8.encode(out),
        }
    }
}
impl<T: BintucoDecode> BintucoDecode for Option<T> {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (tag, mut n) = u8::decode(input)?;
        if tag == 0 {
            Some((None, n))
        } else {
            let (v, m) = T::decode(&input[n..])?;
            Some((Some(v), n + m))
        }
    }
}

// -------- HashMap<K, V> -----------

impl<K, V> BintucoEncode for HashMap<K, V>
where K: BintucoEncode, V: BintucoEncode
{
    fn encode(&self, out: &mut Vec<u8>) {
        (self.len() as u32).encode(out);
        for (k, v) in self.iter() {
            k.encode(out);
            v.encode(out);
        }
    }
}
impl<K, V> BintucoDecode for HashMap<K, V>
where K: BintucoDecode + std::cmp::Eq + std::hash::Hash, V: BintucoDecode
{
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (len, mut n) = u32::decode(input)?;
        let mut map = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let (k, kn) = K::decode(&input[n..])?;
            n += kn;
            let (v, vn) = V::decode(&input[n..])?;
            n += vn;
            map.insert(k, v);
        }
        Some((map, n))
    }
}

impl BintucoEncode for Triplet {
    fn encode(&self, out: &mut Vec<u8>) {
        self.key.encode(out);
        self.rowid.encode(out);
        self.page.encode(out);
    }
}
impl BintucoDecode for Triplet {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (key, n0) = SqlValue::decode(input)?;
        let (rowid, n1) = usize::decode(&input[n0..])?;
        let (page, n2) = usize::decode(&input[n0+n1..])?;
        Some((Triplet { key, rowid, page }, n0 + n1 + n2))
    }
}

impl BintucoEncode for FindResult {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            FindResult::ExactMatch(t) => {
                0u8.encode(out);
                t.encode(out);
            }
            FindResult::ChildIndex(idx) => {
                1u8.encode(out);
                idx.encode(out);
            }
        }
    }
}
impl BintucoDecode for FindResult {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (tag, n0) = u8::decode(input)?;
        match tag {
            0 => {
                let (t, n1) = Triplet::decode(&input[n0..])?;
                Some((FindResult::ExactMatch(t), n0 + n1))
            }
            1 => {
                let (idx, n1) = usize::decode(&input[n0..])?;
                Some((FindResult::ChildIndex(idx), n0 + n1))
            }
            _ => None,
        }
    }
}

impl BintucoEncode for BtreeNode {
    fn encode(&self, out: &mut Vec<u8>) {
        self.is_root.encode(out);
        self.parent_idx.encode(out);
        self.index.encode(out);
        self.is_leaf.encode(out);
        self.free_space.encode(out);
        self.keys.encode(out);
        self.children.encode(out);
    }
}
impl BintucoDecode for BtreeNode {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (is_root, n0) = bool::decode(input)?;
        let (parent_idx, n1) = usize::decode(&input[n0..])?;
        let (index, n2) = usize::decode(&input[n0+n1..])?;
        let (is_leaf, n3) = bool::decode(&input[n0+n1+n2..])?;
        let (free_space, n4) = usize::decode(&input[n0+n1+n2+n3..])?;
        let (keys, n5) = Vec::<Triplet>::decode(&input[n0+n1+n2+n3+n4..])?;
        let (children, n6) = Vec::<usize>::decode(&input[n0+n1+n2+n3+n4+n5..])?;
        Some((
            BtreeNode {
                is_root,
                parent_idx,
                index,
                is_leaf,
                free_space,
                keys,
                children,
            },
            n0 + n1 + n2 + n3 + n4 + n5 + n6,
        ))
    }
}

impl BintucoEncode for Btree {
    fn encode(&self, out: &mut Vec<u8>) {
        self.key_name.encode(out);
        self.btree_name_file.encode(out);
        self.node_counter.encode(out);
        self.root_index.encode(out);
    }
}
impl BintucoDecode for Btree {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (key_name, n0) = String::decode(input)?;
        let (btree_name_file, n1) = String::decode(&input[n0..])?;
        let (node_counter, n2) = usize::decode(&input[n0+n1..])?;
        let (root_index, n3) = usize::decode(&input[n0+n1+n2..])?;
        Some((
            Btree {
                key_name,
                btree_name_file,
                node_counter,
                root_index,
            },
            n0 + n1 + n2 + n3,
        ))
    }
}


/// ///////////////////////////////////////////////////////////////////////////////
/// /// #. Data Serialization and Deserialization
/// ///////////////////////////////////////////////////////////////////////////////
/// /// Abstract data structures are converted to and from byte streams by 
/// /// `bintuco` module.
/// 
/// /// ### On-Disk Row Format (Visualized)
/// 
/// 
/// Example to understand what happen for SQLValue encoding
/// 
/// [ 02 00 00 00 ] [ 02 ] [ 02 00 00 00 ]  [ 09 ]   [ 03 00 00 00 ]  [ 42 6F 62 ]
/// |-------------| |----| |-------------|  |----|   |-------------|  |---------|
///  Vec length: 2   Tag:   INT value: 2     Tag:     VARCHAR len: 3     "Bob"
/// (u32, little-e)  INT   (i32, little-e)  VARCHAR  (u32, little-e)
/// 
///
/// 
/// /// So A `Row` is a `Vec<SqlValue>`. We use a tag-based system for `SqlValue` enums
/// /// and length-prefixing for variable-sized data like vectors and strings.
/// ///
/// /// /// Byte layout for `Row { values: vec![BIGINT(2), VARCHAR("Bob"), INT(40)] }`:
/// ///
/// /// /// +---------------------------------------------------------------------------------------------------------------+
/// /// /// | [ 03 00 00 00 ] [ 0A ] [ 02 00 00 00 00 00 00 00 ] [ 09 ] [ 03 00 00 00 ]  [ 42 6F 62 ] [ 02 ] [ 28 00 00 00 ]|
/// /// /// +-----------------+------+---------------------------+------+----------------+------------+------+--------------+
/// /// /// | Vec Length (u32)| Tag  | BIGINT Value (i64)        | Tag  | VARCHAR        | "B""o""b"  | Tag  | INT Value    |
/// /// /// | = 3             | (i64)| = 2                       | (Vec)| Len (u32) = 3  |            | (i32)| (i32) = 40   |
/// /// /// +---------------------------------------------------------------------------------------------------------------+
/// /// /// | Bintuco-encoded Vec<SqlValue>                                                                                 |
/// /// /// +---------------------------------------------------------------------------------------------------------------+
/// ///



impl BintucoEncode for SqlValue {
    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            SqlValue::TINYINT(v) => { 0u8.encode(out); v.encode(out); }
            SqlValue::SMALLINT(v) => { 1u8.encode(out); v.encode(out); }
            SqlValue::INT(v) => { 2u8.encode(out); v.encode(out); }
            SqlValue::UTINYINT(v) => { 3u8.encode(out); v.encode(out); }
            SqlValue::USMALLINT(v) => { 4u8.encode(out); v.encode(out); }
            SqlValue::UINT(v) => { 5u8.encode(out); v.encode(out); }
            SqlValue::FLOAT(v) => { 6u8.encode(out); v.encode(out); }
            SqlValue::DOUBLE(v) => { 7u8.encode(out); v.encode(out); }
            SqlValue::CHAR(v) => { 8u8.encode(out); v.encode(out); }
            SqlValue::VARCHAR(v) => { 9u8.encode(out); v.encode(out); }
            SqlValue::BIGINT(v)     => { 10u8.encode(out); v.encode(out); }
            SqlValue::UBIGINT(v)    => { 11u8.encode(out); v.encode(out); }


        }
    }
}
impl BintucoDecode for SqlValue {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (tag, n0) = u8::decode(input)?;
        match tag {
            0 => { let (v, n1) = i8::decode(&input[n0..])?; Some((SqlValue::TINYINT(v), n0 + n1)) }
            1 => { let (v, n1) = i16::decode(&input[n0..])?; Some((SqlValue::SMALLINT(v), n0 + n1)) }
            2 => { let (v, n1) = i32::decode(&input[n0..])?; Some((SqlValue::INT(v), n0 + n1)) }
            3 => { let (v, n1) = u8::decode(&input[n0..])?; Some((SqlValue::UTINYINT(v), n0 + n1)) }
            4 => { let (v, n1) = u16::decode(&input[n0..])?; Some((SqlValue::USMALLINT(v), n0 + n1)) }
            5 => { let (v, n1) = u32::decode(&input[n0..])?; Some((SqlValue::UINT(v), n0 + n1)) }
            6 => { let (v, n1) = f32::decode(&input[n0..])?; Some((SqlValue::FLOAT(v), n0 + n1)) }
            7 => { let (v, n1) = f64::decode(&input[n0..])?; Some((SqlValue::DOUBLE(v), n0 + n1)) }
            8 => { let (v, n1) = Vec::<u8>::decode(&input[n0..])?; Some((SqlValue::CHAR(v), n0 + n1)) }
            9 => { let (v, n1) = Vec::<u8>::decode(&input[n0..])?; Some((SqlValue::VARCHAR(v), n0 + n1)) }
            10 => { let (v,n1)=i64::decode(&input[n0..])?; Some((SqlValue::BIGINT(v), n0+n1)) }
            11 => { let (v,n1)=u64::decode(&input[n0..])?; Some((SqlValue::UBIGINT(v), n0+n1)) }

            _ => None,
        }
    }
}

impl BintucoEncode for SqlColumnType {
    fn encode(&self, out: &mut Vec<u8>) {
        let tag = match self {
            SqlColumnType::TINYINT => 0u8,
            SqlColumnType::SMALLINT => 1,
            SqlColumnType::INT => 2,
            SqlColumnType::UTINYINT => 3,
            SqlColumnType::USMALLINT => 4,
            SqlColumnType::UINT => 5,
            SqlColumnType::FLOAT => 6,
            SqlColumnType::DOUBLE => 7,
            SqlColumnType::CHAR => 8,
            SqlColumnType::VARCHAR => 9,
            SqlColumnType::BIGINT    => 10,
            SqlColumnType::UBIGINT    => 11,


        };
        tag.encode(out);
    }
}
impl BintucoDecode for SqlColumnType {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (tag, n0) = u8::decode(input)?;
        match tag {
            0 => Some((SqlColumnType::TINYINT, n0)),
            1 => Some((SqlColumnType::SMALLINT, n0)),
            2 => Some((SqlColumnType::INT, n0)),
            3 => Some((SqlColumnType::UTINYINT, n0)),
            4 => Some((SqlColumnType::USMALLINT, n0)),
            5 => Some((SqlColumnType::UINT, n0)),
            6 => Some((SqlColumnType::FLOAT, n0)),
            7 => Some((SqlColumnType::DOUBLE, n0)),
            8 => Some((SqlColumnType::CHAR, n0)),
            9 => Some((SqlColumnType::VARCHAR, n0)),
            10 => Some((SqlColumnType::BIGINT,    n0)),
            11 => Some((SqlColumnType::UBIGINT,    n0)),

            _ => None,
        }
    }
}

impl BintucoEncode for Row {
    fn encode(&self, out: &mut Vec<u8>) {
        self.values.encode(out);
    }
}
impl BintucoDecode for Row {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (values, n0) = Vec::<SqlValue>::decode(input)?;
        Some((Row { values }, n0))
    }
}

impl BintucoEncode for Cell {
    fn encode(&self, out: &mut Vec<u8>) {
        self.overflow_page.encode(out);
        self.row_payload.encode(out);
    }
}
impl BintucoDecode for Cell {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (overflow_page, n0) = usize::decode(input)?;
        let (row_payload, n1) = Vec::<u8>::decode(&input[n0..])?;
        Some((Cell { overflow_page, row_payload }, n0 + n1))
    }
}

impl BintucoEncode for OverflowPage {
    fn encode(&self, out: &mut Vec<u8>) {
        self.page_num.encode(out); 
        self.overflow_page.encode(out);
        self.row_payload.encode(out);
    }
}
impl BintucoDecode for OverflowPage {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (page_num, n0) = usize::decode(input)?;
        let (overflow_page, n1) = usize::decode(&input[n0..])?;
        let (row_payload, n2) = Vec::<u8>::decode(&input[n0 + n1..])?;
        Some((
            OverflowPage {
                page_num,
                overflow_page,
                row_payload,
            },
            n0 + n1 + n2,
        ))
    }
}

impl BintucoEncode for TableSchema {
    fn encode(&self, out: &mut Vec<u8>) {
        self.name.encode(out);
        self.columns.encode(out);
        self.name_col.encode(out);
        self.primary_key.encode(out);
    }
}
impl BintucoDecode for TableSchema {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (name, n0) = String::decode(input)?;
        let (columns, n1) = Vec::<SqlColumnType>::decode(&input[n0..])?;
        let (name_col, n2) = Vec::<String>::decode(&input[n0+n1..])?;
        let (primary_key, n3) = Option::<String>::decode(&input[n0+n1+n2..])?;
        Some((TableSchema { name, columns, name_col, primary_key }, n0 + n1 + n2 + n3))
    }
}

impl BintucoEncode for DatabasePage {
    fn encode(&self, out: &mut Vec<u8>) {
        self.i_magic.encode(out);
        self.db_folder.to_string_lossy().to_string().encode(out);
        self.all_tableroot.encode(out);
    }
}
impl BintucoDecode for DatabasePage {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (i_magic, n0) = usize::decode(input)?;
        let (db_folder_str, n1) = String::decode(&input[n0..])?;
        let (all_tableroot, n2) = Vec::<TableRoot>::decode(&input[n0+n1..])?;
        let db_folder = PathBuf::from(db_folder_str);

        Some((
            DatabasePage {
                i_magic,
                db_folder,
                all_tableroot,
            },
            n0 + n1 + n2,
        ))
    }
}

impl BintucoEncode for TableRoot {
    fn encode(&self, out: &mut Vec<u8>) {
        self.tb_schema.encode(out);
        self.name_root.encode(out);
        self.name_file.encode(out);
        self.page_num.encode(out);
        self.row_id.encode(out);
        self.page_vec.encode(out);
        self.index_vec.encode(out);
    }
}
impl BintucoDecode for TableRoot {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (tb_schema, n0) = TableSchema::decode(input)?;
        let (name_root, n1) = String::decode(&input[n0..])?;
        let (name_file, n2) = String::decode(&input[n0+n1..])?;
        let (page_num, n3) = usize::decode(&input[n0+n1+n2..])?;
        let (row_id, n4) = usize::decode(&input[n0+n1+n2+n3..])?;
        let (page_vec, n5) = Btree::decode(&input[n0+n1+n2+n3+n4..])?;
        let (index_vec, n6) = Vec::<Btree>::decode(&input[n0+n1+n2+n3+n4+n5..])?;
        Some((TableRoot {
            tb_schema, name_root, name_file, page_num, row_id, page_vec, index_vec
        }, n0+n1+n2+n3+n4+n5+n6))
    }
}

impl BintucoEncode for TablePage {
    fn encode(&self, out: &mut Vec<u8>) {
        self.index.encode(out);
        self.num_cell.encode(out);
        self.row_idnum.encode(out);
        self.pointer_cell.encode(out);
        self.veccell.encode(out);
    }
}
impl BintucoDecode for TablePage {
    fn decode(input: &[u8]) -> Option<(Self, usize)> {
        let (index, n0) = usize::decode(input)?;
        let (num_cell, n1) = u16::decode(&input[n0..])?;
        let (row_idnum, n2) = usize::decode(&input[n0+n1..])?;
        let (pointer_cell, n3) = HashMap::<usize, u16>::decode(&input[n0+n1+n2..])?;
        let (veccell, n4) = Vec::<Cell>::decode(&input[n0+n1+n2+n3..])?;
        Some((TablePage {
            index, num_cell, row_idnum, pointer_cell, veccell
        }, n0 + n1 + n2 + n3 + n4))
    }
}

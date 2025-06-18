//! MEMTUCO RDBMS CONSTANTs

pub const PAGE_SIZE: usize = 1024;
pub const PAGE_CAPACITY: usize = 950;
pub const PAYLOAD_SIZE: usize = 950;
pub const BTREE_INIT_SPACE: usize = 950;
pub const NODE_CAPACITY: usize = 950;
pub const OVERFLOW_SENTINEL: usize = 200;
pub const OVERFLOW_KEY: u32 = (PAGE_SIZE as u32) + 1;
pub const DEFAULT_NUM_ROWS: usize = 20;
pub const CHUNK_SIZE_K_ORDER: usize = 1000;
pub const PAYLOAD_MIN: u32= 235;
pub const PADDING_ROW_BYTES: usize =20;
pub const SAMPLE_SIZE: usize = 10; // for csv inference
pub const CHUNK_CSV_SIZE: usize = 1_000; // for csv import/load

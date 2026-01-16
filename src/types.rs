use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct ChunkRange {
    pub index: usize,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone)]
pub struct ContractRow {
    pub block_number: u32,
    pub block_hash: Vec<u8>,
    pub create_index: u32,
    pub transaction_hash: Option<Vec<u8>>,
    pub contract_address: Vec<u8>,
    pub deployer: Vec<u8>,
    pub factory: Vec<u8>,
    pub init_code: Vec<u8>,
    pub code: Vec<u8>,
    pub init_code_hash: Vec<u8>,
    pub n_init_code_bytes: u32,
    pub n_code_bytes: u32,
    pub code_hash: Vec<u8>,
    pub chain_id: u64,
}

#[derive(Debug, Serialize)]
pub struct ChunkReport {
    pub index: usize,
    pub start_block: u64,
    pub end_block: u64,
    pub rows: usize,
    pub output_path: PathBuf,
    pub size_bytes: Option<u64>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub skipped: bool,
}

#[derive(Debug, Serialize)]
pub struct RunReport {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub status: String,
    pub error: Option<String>,
    pub chain_id: u64,
    pub rpc_url: String,
    pub start_block: u64,
    pub end_block: u64,
    pub chunk_size: u64,
    pub batch_size: usize,
    pub max_concurrent_requests: usize,
    pub max_concurrent_chunks: usize,
    pub output_dir: PathBuf,
    pub chunks: Vec<ChunkReport>,
}

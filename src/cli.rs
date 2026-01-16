use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "blink")]
#[command(about = "Fast contract bytecode indexer")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Extract contract creation bytecode data
    Contracts(ContractsArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct ContractsArgs {
    /// RPC URL (fallback: ETH_RPC_URL)
    #[arg(long)]
    pub rpc: Option<String>,
    /// Start block (inclusive)
    #[arg(long)]
    pub start_block: u64,
    /// End block (inclusive) or "latest"
    #[arg(long, default_value = "latest")]
    pub end_block: String,
    /// Blocks per chunk/file
    #[arg(long, default_value_t = 100_000)]
    pub chunk_size: u64,
    /// Blocks per JSON-RPC batch request
    #[arg(long, default_value_t = 50)]
    pub batch_size: usize,
    /// Max concurrent HTTP requests
    #[arg(long, default_value_t = 32)]
    pub max_concurrent_requests: usize,
    /// Max concurrent chunks
    #[arg(long, default_value_t = 4)]
    pub max_concurrent_chunks: usize,
    /// Output directory
    #[arg(long, default_value = "./data/blink")]
    pub output_dir: PathBuf,
    /// Overwrite existing chunk files
    #[arg(long)]
    pub overwrite: bool,
    /// Max retries per batch request
    #[arg(long, default_value_t = 5)]
    pub max_retries: u32,
    /// Initial retry backoff in ms
    #[arg(long, default_value_t = 1000)]
    pub initial_backoff_ms: u64,
    /// Max retry backoff in ms
    #[arg(long, default_value_t = 30_000)]
    pub max_backoff_ms: u64,
    /// Report directory (default: {output_dir}/.blink/reports)
    #[arg(long)]
    pub report_dir: Option<PathBuf>,
    /// Use aggressive defaults for fastest extraction
    #[arg(long)]
    pub fast: bool,
}

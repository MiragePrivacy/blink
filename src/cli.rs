use std::path::PathBuf;

use clap::{
    builder::styling::{Color, RgbColor, Style, Styles},
    Command, CommandFactory, FromArgMatches, Parser, Subcommand,
};

use crate::util::format_header;

/// Match the dashboard palette — lime-on-black for headers/literals, dim grey
/// for placeholders, red for errors. Applied to every `--help` screen.
const fn cli_styles() -> Styles {
    let lime = Color::Rgb(RgbColor(189, 255, 0));
    let text = Color::Rgb(RgbColor(237, 237, 237));
    let dim = Color::Rgb(RgbColor(112, 112, 112));
    let red = Color::Rgb(RgbColor(255, 77, 77));
    Styles::styled()
        .header(Style::new().bold().fg_color(Some(lime)))
        .usage(Style::new().bold().fg_color(Some(lime)))
        .literal(Style::new().fg_color(Some(text)))
        .placeholder(Style::new().fg_color(Some(dim)))
        .valid(Style::new().fg_color(Some(lime)))
        .invalid(Style::new().fg_color(Some(red)))
        .error(Style::new().bold().fg_color(Some(red)))
}

#[derive(Parser, Debug)]
#[command(name = "blink")]
#[command(about = "Fast contract bytecode indexer + monitoring dashboard")]
#[command(styles = cli_styles())]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Extract contract creation bytecode data
    Contracts(ContractsArgs),
    /// Load local contract datasets into Blink
    Load(LoadArgs),
    /// Decode bytecode locally: compiler version, language, ERC standards, proxy detection
    Decode(DecodeArgs),
    /// Serve the public monitoring dashboard
    Serve(ServeArgs),
}

pub fn parse() -> Cli {
    let matches = command().get_matches();
    Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit())
}

fn command() -> Command {
    let mut command = Cli::command().before_help(format_header("blink"));
    for subcommand in command.get_subcommands_mut() {
        let title = match subcommand.get_name() {
            "contracts" => Some("blink contracts"),
            "load" => Some("blink load"),
            "decode" => Some("blink decode"),
            "serve" => Some("blink serve"),
            _ => None,
        };
        if let Some(title) = title {
            *subcommand = subcommand.clone().before_help(format_header(title));
        }
    }
    command
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

#[derive(Parser, Debug, Clone)]
pub struct LoadArgs {
    /// Directory containing contract Parquet files or normalized CSV files
    #[arg(long, default_value = "./data/blink")]
    pub contracts_dir: PathBuf,
    /// Verifier Alliance export directory containing contract_deployments/ and verified_contracts/
    #[arg(long = "va")]
    pub verifier_alliance_dir: Option<PathBuf>,
    /// Blink data directory where blink.duckdb and Parquet links are stored
    #[arg(long, default_value = "./data/blink")]
    pub data_dir: PathBuf,
    /// Glob for contract Parquet files when loading a Parquet directory
    #[arg(long, default_value = "*.parquet")]
    pub contracts_glob: String,
    /// Ethereum chain id stored with imported rows
    #[arg(long, default_value_t = 1)]
    pub chain_id: u64,
    /// Rebuild existing CSV import tables or replace existing Parquet links
    #[arg(long)]
    pub overwrite: bool,
    /// DuckDB memory limit for CSV and verification imports
    #[arg(long, default_value = "8GB")]
    pub memory_limit: String,
    /// DuckDB worker threads for CSV imports (default: DuckDB decides)
    #[arg(long)]
    pub threads: Option<usize>,
}

#[derive(Parser, Debug, Clone)]
pub struct DecodeArgs {
    /// Data directory containing contract parquet files
    #[arg(long, default_value = "./data/blink")]
    pub data_dir: PathBuf,
    /// Glob (relative to data_dir) for contract parquet files
    #[arg(long, default_value = "*.parquet")]
    pub contracts_glob: String,
    /// Rows per analyze + insert batch. Larger = better parallelism but more
    /// peak memory; 5K keeps even worst-case bytecode loads under ~150 MB.
    #[arg(long, default_value_t = 5_000)]
    pub batch_size: usize,
    /// Re-decode every contract even if it's already in the metadata table
    #[arg(long)]
    pub overwrite: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct ServeArgs {
    /// Bind address
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub bind: String,
    /// Data directory containing contract parquet files
    #[arg(long, default_value = "./data/blink")]
    pub data_dir: PathBuf,
    /// Glob (relative to data_dir) for contract parquet files
    #[arg(long, default_value = "*.parquet")]
    pub contracts_glob: String,
    /// Open DuckDB in read-only mode so the dashboard can run alongside an
    /// active `blink decode`. Disables `--tail-rpc` automatically.
    #[arg(long)]
    pub read_only: bool,
    /// Run continuous tail extraction against this RPC URL
    #[arg(long)]
    pub tail_rpc: Option<String>,
    /// Tail extraction poll interval (seconds)
    #[arg(long, default_value_t = 60)]
    pub tail_interval_secs: u64,
    /// Tail extraction confirmation depth (lag behind head by this many blocks)
    #[arg(long, default_value_t = 12)]
    pub tail_confirmations: u64,
    /// Tail extraction batch size (blocks per JSON-RPC request)
    #[arg(long, default_value_t = 50)]
    pub tail_batch_size: usize,
    /// Tail extraction max concurrent HTTP requests
    #[arg(long, default_value_t = 16)]
    pub tail_max_concurrent_requests: usize,
}

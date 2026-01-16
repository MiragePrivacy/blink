mod batch;
mod cli;
mod extract;
mod parquet_io;
mod runner;
mod types;
mod util;

use anyhow::Result;
use clap::Parser;

use crate::{cli::Cli, runner::run_contracts};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        cli::Commands::Contracts(args) => run_contracts(args).await,
    }
}

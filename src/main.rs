//! `blink` binary entry point.

mod blocks;
mod chains;
mod cli;
mod db;
mod decode;
mod extract;
mod load;
mod serve;
mod types;
mod util;

use anyhow::Result;
use tracing_subscriber::{fmt::time::ChronoLocal, EnvFilter};

use crate::extract::run_contracts;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = cli::parse();
    match cli.command {
        cli::Commands::Contracts(args) => run_contracts(args).await,
        cli::Commands::Load(args) => load::run_load(args).await,
        cli::Commands::Decode(args) => decode::run_decode(args).await,
        cli::Commands::Serve(args) => serve::run_serve(args).await,
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("blink=info,axum=info,tower_http=warn"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_timer(ChronoLocal::new("%H:%M:%S".into()))
        .with_target(false)
        .compact()
        .init();
}

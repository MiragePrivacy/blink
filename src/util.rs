use std::{
    fs::File,
    path::{Path, PathBuf},
};

use alloy::providers::Provider;
use anyhow::{anyhow, Context, Result};
use chrono::Duration;

use crate::types::{ChunkRange, RunReport};

pub fn build_chunks(start: u64, end: u64, chunk_size: u64) -> Vec<ChunkRange> {
    let mut chunks = Vec::new();
    let mut index = 0usize;
    let mut current = start;
    while current <= end {
        let chunk_end = (current + chunk_size - 1).min(end);
        chunks.push(ChunkRange {
            index,
            start: current,
            end: chunk_end,
        });
        index += 1;
        if chunk_end == end {
            break;
        }
        current = chunk_end + 1;
    }
    chunks
}

pub fn chunk_path(output_dir: &Path, start: u64, end: u64) -> PathBuf {
    output_dir.join(format!("contracts__{:010}__{:010}.parquet", start, end))
}

pub fn resolve_rpc_url(rpc: Option<String>) -> Result<String> {
    let mut url = match rpc {
        Some(url) => url,
        None => {
            std::env::var("ETH_RPC_URL").map_err(|_| anyhow!("missing --rpc and ETH_RPC_URL"))?
        }
    };
    if !url.starts_with("http") {
        url = format!("http://{}", url);
    }
    Ok(url)
}

pub async fn resolve_end_block<P: Provider>(
    provider: &P,
    end_block: &str,
) -> Result<u64> {
    if end_block.eq_ignore_ascii_case("latest") {
        provider
            .get_block_number()
            .await
            .context("failed to fetch latest block")
    } else {
        end_block
            .parse::<u64>()
            .map_err(|_| anyhow!("invalid end block: {}", end_block))
    }
}

pub fn write_report(path: &Path, report: &RunReport) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    serde_json::to_writer_pretty(file, report).context("write report")
}

pub fn format_duration(d: Duration) -> String {
    let total_secs = d.num_seconds();
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    if hours > 0 {
        format!("{}h {}m {}s", hours, mins, secs)
    } else if mins > 0 {
        format!("{}m {}s", mins, secs)
    } else {
        format!("{}s", secs)
    }
}

pub fn color_white(text: &str) -> String {
    format!("\x1b[97m{}\x1b[0m", text)
}

pub fn color_green(text: &str) -> String {
    format!("\x1b[32m{}\x1b[0m", text)
}

pub fn color_red(text: &str) -> String {
    format!("\x1b[31m{}\x1b[0m", text)
}

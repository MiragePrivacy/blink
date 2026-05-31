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

pub fn chain_chunk_path(output_dir: &Path, chain_id: u64, start: u64, end: u64) -> PathBuf {
    output_dir.join(format!(
        "contracts__chain_{:010}__{:010}__{:010}.parquet",
        chain_id, start, end
    ))
}

pub fn resolve_rpc_url(rpc: Option<String>) -> Result<String> {
    let mut url = match rpc {
        Some(url) => url,
        None => std::env::var("BLINK_CONTRACTS_RPC")
            .map_err(|_| anyhow!("missing --rpc and BLINK_CONTRACTS_RPC"))?,
    };
    if !url.starts_with("http") {
        url = format!("http://{}", url);
    }
    Ok(url)
}

pub async fn resolve_end_block<P: Provider>(provider: &P, end_block: &str) -> Result<u64> {
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

/// Format an integer with US-style thousands separators.
pub fn format_count(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

/// Tiny glob: supports '*' and literal chars. Intended for simple file names
/// like `*.parquet`, not full path globbing.
pub fn match_simple_glob(pattern: &str, name: &str) -> bool {
    fn helper(pat: &[u8], s: &[u8]) -> bool {
        if pat.is_empty() {
            return s.is_empty();
        }
        if pat[0] == b'*' {
            for i in 0..=s.len() {
                if helper(&pat[1..], &s[i..]) {
                    return true;
                }
            }
            return false;
        }
        if !s.is_empty() && pat[0] == s[0] {
            return helper(&pat[1..], &s[1..]);
        }
        false
    }
    helper(pattern.as_bytes(), name.as_bytes())
}

pub fn color_red(text: &str) -> String {
    format!("\x1b[38;2;255;77;77m{}\x1b[0m", text)
}

pub fn color_text(text: &str) -> String {
    format!("\x1b[38;2;237;237;237m{}\x1b[0m", text)
}

pub fn color_accent(text: &str) -> String {
    format!("\x1b[38;2;189;255;0m{}\x1b[0m", text)
}

pub fn color_dim(text: &str) -> String {
    format!("\x1b[38;2;112;112;112m{}\x1b[0m", text)
}

pub fn color_faint(text: &str) -> String {
    format!("\x1b[38;2;64;64;64m{}\x1b[0m", text)
}

/// Section header block: `░ TITLE` in lime + a hairline rule below.
pub fn format_header(title: &str) -> String {
    format!(
        "{}\n{} {}\n{}",
        color_faint(""),
        color_accent("░"),
        color_accent(&title.to_uppercase()),
        color_faint(&"─".repeat(60))
    )
}

pub fn print_header(title: &str) {
    println!("{}", format_header(title));
}

/// Key/value line: `  key:  value` with dim label and bright value.
pub fn print_kv(key: &str, value: &str) {
    println!(
        "  {}  {}",
        color_dim(&format!("{:<14}", format!("{}:", key))),
        color_text(value)
    );
}

/// Same as [`print_kv`] but the value is rendered in the lime accent color.
pub fn print_kv_accent(key: &str, value: &str) {
    println!(
        "  {}  {}",
        color_dim(&format!("{:<14}", format!("{}:", key))),
        color_accent(value)
    );
}

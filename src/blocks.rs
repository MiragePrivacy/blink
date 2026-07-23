//! Approximate block-number → wall-clock-time conversion for supported chains.
//!
//! Blink loads sparse, exact `(block_number, unix_timestamp)` checkpoints from DuckDB,
//! interpolates between them, and extrapolates from the latest measured rate.

use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock},
};

use chrono::{DateTime, TimeZone, Utc};

use crate::chains::GNOSIS_CHAIN_ID;

pub type BlockCheckpoint = (u64, i64);
pub type ChainCheckpoints = HashMap<u64, Vec<BlockCheckpoint>>;

static RUNTIME_CHECKPOINTS: OnceLock<RwLock<ChainCheckpoints>> = OnceLock::new();

fn checkpoint_store() -> &'static RwLock<ChainCheckpoints> {
    RUNTIME_CHECKPOINTS.get_or_init(|| RwLock::new(HashMap::new()))
}

#[doc(hidden)]
pub fn replace_runtime_checkpoints(checkpoints: ChainCheckpoints) {
    if let Ok(mut store) = checkpoint_store().write() {
        *store = checkpoints;
    }
}

pub(crate) fn upsert_runtime_checkpoint(chain_id: u64, block_number: u64, timestamp: i64) {
    if let Ok(mut store) = checkpoint_store().write() {
        let chain = store.entry(chain_id).or_default();
        match chain.binary_search_by_key(&block_number, |(block, _)| *block) {
            Ok(index) => chain[index].1 = timestamp,
            Err(index) => chain.insert(index, (block_number, timestamp)),
        }
    }
}

/// Ideal slot times, used only to size chart buckets — the timestamp math
/// uses measured checkpoint rates instead.
const SECS_PER_BLOCK_POST_MERGE: i64 = 12;
const POST_MERGE_BLOCK: u64 = 15_537_393;
const GNOSIS_SECS_PER_BLOCK: i64 = 5;

/// Approximate the block timestamp for a given chain and block number.
pub fn block_timestamp(chain_id: u64, block_number: u64) -> DateTime<Utc> {
    let checkpoints = checkpoint_store()
        .read()
        .ok()
        .and_then(|store| store.get(&chain_id).cloned())
        .unwrap_or_default();
    let secs = checkpoint_timestamp_secs(chain_id, &checkpoints, block_number);
    Utc.timestamp_opt(secs, 0).single().unwrap_or_else(Utc::now)
}

/// Approximate the block number for a given chain and timestamp.
pub fn block_number_at_time(chain_id: u64, timestamp: DateTime<Utc>) -> u64 {
    let checkpoints = checkpoint_store()
        .read()
        .ok()
        .and_then(|store| store.get(&chain_id).cloned())
        .unwrap_or_default();
    checkpoint_block_at_time(chain_id, &checkpoints, timestamp.timestamp())
}

/// Milliseconds per block over the final checkpoint span — the measured
/// recent rate, used to extrapolate past the newest checkpoint.
fn fallback_ms_per_block(chain_id: u64, block_number: u64) -> i128 {
    match chain_id {
        GNOSIS_CHAIN_ID => GNOSIS_SECS_PER_BLOCK as i128 * 1000,
        _ if block_number >= POST_MERGE_BLOCK => SECS_PER_BLOCK_POST_MERGE as i128 * 1000,
        _ => 14_000,
    }
}

fn trailing_ms_per_block(
    chain_id: u64,
    checkpoints: &[BlockCheckpoint],
    block_number: u64,
) -> i128 {
    if checkpoints.len() < 2 {
        return fallback_ms_per_block(chain_id, block_number);
    }
    let (b1, t1) = checkpoints[checkpoints.len() - 1];
    let (b0, t0) = checkpoints[checkpoints.len() - 2];
    if b1 <= b0 || t1 <= t0 {
        return fallback_ms_per_block(chain_id, block_number);
    }
    ((t1 - t0) as i128 * 1000) / (b1 - b0) as i128
}

fn checkpoint_timestamp_secs(
    chain_id: u64,
    checkpoints: &[BlockCheckpoint],
    block_number: u64,
) -> i64 {
    if checkpoints.is_empty() {
        return 0;
    }
    let (last_block, last_timestamp) = checkpoints[checkpoints.len() - 1];
    if block_number >= last_block {
        let ms = (block_number - last_block) as i128
            * trailing_ms_per_block(chain_id, checkpoints, block_number);
        return last_timestamp + (ms / 1000) as i64;
    }
    interpolate_checkpoint_timestamp(checkpoints, block_number)
}

fn checkpoint_block_at_time(chain_id: u64, checkpoints: &[BlockCheckpoint], timestamp: i64) -> u64 {
    if checkpoints.is_empty() {
        return 0;
    }
    let (last_block, last_timestamp) = checkpoints[checkpoints.len() - 1];
    if timestamp >= last_timestamp {
        let blocks = (timestamp - last_timestamp) as i128 * 1000
            / trailing_ms_per_block(chain_id, checkpoints, last_block);
        return last_block + blocks.max(0) as u64;
    }
    interpolate_checkpoint_block(checkpoints, timestamp)
}

fn interpolate_checkpoint_timestamp(checkpoints: &[(u64, i64)], block_number: u64) -> i64 {
    let idx = match checkpoints.binary_search_by_key(&block_number, |(b, _)| *b) {
        Ok(i) => return checkpoints[i].1,
        Err(i) => i,
    };
    if idx == 0 {
        return checkpoints[0].1;
    }
    if idx >= checkpoints.len() {
        return checkpoints[checkpoints.len() - 1].1;
    }
    let (b0, t0) = checkpoints[idx - 1];
    let (b1, t1) = checkpoints[idx];
    let span_blocks = (b1 - b0) as i128;
    let span_secs = (t1 - t0) as i128;
    let offset = (block_number - b0) as i128;
    t0 + ((offset * span_secs) / span_blocks) as i64
}

fn interpolate_checkpoint_block(checkpoints: &[(u64, i64)], timestamp: i64) -> u64 {
    let idx = match checkpoints.binary_search_by_key(&timestamp, |(_, t)| *t) {
        Ok(i) => return checkpoints[i].0,
        Err(i) => i,
    };
    if idx == 0 {
        return checkpoints[0].0;
    }
    if idx >= checkpoints.len() {
        return checkpoints[checkpoints.len() - 1].0;
    }
    let (b0, t0) = checkpoints[idx - 1];
    let (b1, t1) = checkpoints[idx];
    let span_secs = (t1 - t0) as i128;
    if span_secs <= 0 {
        return b0;
    }
    let span_blocks = (b1 - b0) as i128;
    let offset = (timestamp - t0) as i128;
    b0 + ((offset * span_blocks) / span_secs) as u64
}

/// Approximate blocks per day at the given block height (for choosing bucket widths).
pub fn blocks_per_day(chain_id: u64, block_number: u64) -> u64 {
    match chain_id {
        GNOSIS_CHAIN_ID => 86_400 / GNOSIS_SECS_PER_BLOCK as u64,
        _ => {
            if block_number >= POST_MERGE_BLOCK {
                86_400 / SECS_PER_BLOCK_POST_MERGE as u64
            } else {
                // Pre-Merge averaged ~13.5s/block.
                86_400 / 14
            }
        }
    }
}

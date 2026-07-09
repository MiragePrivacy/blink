//! Approximate block-number → wall-clock-time conversion for supported chains.
//!
//! The contract Parquet schema does not store block timestamps, so the
//! dashboard derives them from `block_number` via hardcoded checkpoints of
//! real `(block_number, unix_timestamp)` pairs. Between checkpoints we
//! interpolate; beyond the newest checkpoint we extrapolate at the average
//! rate of the final checkpoint span.
//!
//! Extrapolating at the *ideal* slot time is not good enough: chains miss
//! slots, so real average block time runs slightly long (Ethereum ~12.09s
//! rather than 12s, Gnosis ~5.15s rather than 5s). At 12.0s flat, Ethereum's
//! merge-anchored clock had drifted ten days behind reality by mid-2026 —
//! the dashboard labeled today's deployments "June 29". Refresh the newest
//! checkpoint of each chain every few months to keep drift to minutes:
//!
//! ```sh
//! curl -s <rpc> -X POST -H 'content-type: application/json' \
//!   -d '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}'
//! ```

use chrono::{DateTime, TimeZone, Utc};

use crate::chains::GNOSIS_CHAIN_ID;

/// Real (block_number, unix_timestamp) checkpoints for Ethereum mainnet.
const CHECKPOINTS: &[(u64, i64)] = &[
    (0, 1_438_269_988),          // 2015-07-30 genesis
    (200_000, 1_443_534_600),    // 2015-09-29
    (1_000_000, 1_455_404_488),  // 2016-02-13
    (2_000_000, 1_470_173_578),  // 2016-08-02
    (3_000_000, 1_484_802_716),  // 2017-01-19
    (4_000_000, 1_499_633_567),  // 2017-07-09
    (4_370_000, 1_508_131_331),  // 2017-10-16 byzantium
    (5_000_000, 1_517_319_693),  // 2018-01-30
    (6_000_000, 1_532_118_564),  // 2018-07-21
    (7_000_000, 1_546_466_492),  // 2019-01-02
    (7_280_000, 1_551_383_524),  // 2019-02-28 constantinople
    (8_000_000, 1_561_100_149),  // 2019-06-21
    (9_000_000, 1_574_706_444),  // 2019-11-25
    (10_000_000, 1_588_598_533), // 2020-05-04
    (11_000_000, 1_602_667_372), // 2020-10-14
    (12_000_000, 1_617_270_478), // 2021-04-01
    (12_244_000, 1_618_481_223), // 2021-04-15 berlin
    (12_965_000, 1_628_166_822), // 2021-08-05 london
    (13_000_000, 1_628_643_581), // 2021-08-12
    (14_000_000, 1_642_114_795), // 2022-01-13
    (15_000_000, 1_656_586_444), // 2022-06-30
    (15_537_393, 1_663_224_162), // 2022-09-15 merge
    (18_000_000, 1_693_066_895), // 2023-08-26
    (20_000_000, 1_717_281_407), // 2024-06-01
    (22_000_000, 1_741_410_875), // 2025-03-08
    (24_000_000, 1_765_584_371), // 2025-12-13
    (25_497_188, 1_783_626_143), // 2026-07-09
];

/// Real (block_number, unix_timestamp) checkpoints for Gnosis Chain.
const GNOSIS_CHECKPOINTS: &[(u64, i64)] = &[
    (0, 1_539_024_180),          // 2018-10-08 xDai/Gnosis genesis
    (10_000_000, 1_589_952_315), // 2020-05-20
    (20_000_000, 1_641_651_650), // 2022-01-08
    (30_000_000, 1_694_855_615), // 2023-09-16
    (40_000_000, 1_746_925_820), // 2025-05-11
    (44_212_810, 1_768_666_350), // 2026-01-17
    (46_762_380, 1_781_798_438), // 2026-06-18
    (47_119_233, 1_783_626_150), // 2026-07-09
];

/// Ideal slot times, used only to size chart buckets — the timestamp math
/// uses measured checkpoint rates instead.
const SECS_PER_BLOCK_POST_MERGE: i64 = 12;
const POST_MERGE_BLOCK: u64 = 15_537_393;
const GNOSIS_SECS_PER_BLOCK: i64 = 5;

/// Approximate the block timestamp for a given chain and block number.
pub fn block_timestamp(chain_id: u64, block_number: u64) -> DateTime<Utc> {
    let checkpoints = chain_checkpoints(chain_id);
    let secs = checkpoint_timestamp_secs(checkpoints, block_number);
    Utc.timestamp_opt(secs, 0).single().unwrap_or_else(Utc::now)
}

/// Approximate the block number for a given chain and timestamp.
pub fn block_number_at_time(chain_id: u64, timestamp: DateTime<Utc>) -> u64 {
    let checkpoints = chain_checkpoints(chain_id);
    checkpoint_block_at_time(checkpoints, timestamp.timestamp())
}

fn chain_checkpoints(chain_id: u64) -> &'static [(u64, i64)] {
    match chain_id {
        GNOSIS_CHAIN_ID => GNOSIS_CHECKPOINTS,
        _ => CHECKPOINTS,
    }
}

/// Milliseconds per block over the final checkpoint span — the measured
/// recent rate, used to extrapolate past the newest checkpoint.
fn trailing_ms_per_block(checkpoints: &[(u64, i64)]) -> i128 {
    let (b1, t1) = checkpoints[checkpoints.len() - 1];
    let (b0, t0) = checkpoints[checkpoints.len() - 2];
    ((t1 - t0) as i128 * 1000) / (b1 - b0) as i128
}

fn checkpoint_timestamp_secs(checkpoints: &[(u64, i64)], block_number: u64) -> i64 {
    let (last_block, last_timestamp) = checkpoints[checkpoints.len() - 1];
    if block_number >= last_block {
        let ms = (block_number - last_block) as i128 * trailing_ms_per_block(checkpoints);
        return last_timestamp + (ms / 1000) as i64;
    }
    interpolate_checkpoint_timestamp(checkpoints, block_number)
}

fn checkpoint_block_at_time(checkpoints: &[(u64, i64)], timestamp: i64) -> u64 {
    let (last_block, last_timestamp) = checkpoints[checkpoints.len() - 1];
    if timestamp >= last_timestamp {
        let blocks =
            (timestamp - last_timestamp) as i128 * 1000 / trailing_ms_per_block(checkpoints);
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

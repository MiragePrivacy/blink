//! Approximate Ethereum mainnet block-number → wall-clock-time conversion.
//!
//! The contract Parquet schema does not store block timestamps, so the
//! dashboard derives them from `block_number` via piecewise-linear
//! interpolation between hardcoded checkpoints (genesis, hardforks, and a
//! handful of round numbers in between). Post-Merge it switches to the
//! exact 12-second slot cadence.

use chrono::{DateTime, TimeZone, Utc};

/// Hardcoded (block_number, unix_timestamp) checkpoints for Ethereum mainnet.
/// Used to interpolate timestamps without an extra timestamp lookup table.
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
];

const POST_MERGE_BLOCK: u64 = 15_537_393;
const POST_MERGE_TIMESTAMP: i64 = 1_663_224_162;
const SECS_PER_BLOCK_POST_MERGE: i64 = 12;

/// Approximate the Ethereum mainnet block timestamp for a given block number.
/// Post-Merge: exact (12s/block). Pre-Merge: linear interpolation between checkpoints.
pub fn block_timestamp(block_number: u64) -> DateTime<Utc> {
    let secs = if block_number >= POST_MERGE_BLOCK {
        POST_MERGE_TIMESTAMP + (block_number - POST_MERGE_BLOCK) as i64 * SECS_PER_BLOCK_POST_MERGE
    } else {
        interpolate_pre_merge(block_number)
    };
    Utc.timestamp_opt(secs, 0).single().unwrap_or_else(Utc::now)
}

fn interpolate_pre_merge(block_number: u64) -> i64 {
    let idx = match CHECKPOINTS.binary_search_by_key(&block_number, |(b, _)| *b) {
        Ok(i) => return CHECKPOINTS[i].1,
        Err(i) => i,
    };
    if idx == 0 {
        return CHECKPOINTS[0].1;
    }
    if idx >= CHECKPOINTS.len() {
        return CHECKPOINTS[CHECKPOINTS.len() - 1].1;
    }
    let (b0, t0) = CHECKPOINTS[idx - 1];
    let (b1, t1) = CHECKPOINTS[idx];
    let span_blocks = (b1 - b0) as i128;
    let span_secs = (t1 - t0) as i128;
    let offset = (block_number - b0) as i128;
    t0 + ((offset * span_secs) / span_blocks) as i64
}

/// Approximate blocks per day at the given block height (for choosing bucket widths).
pub fn blocks_per_day(block_number: u64) -> u64 {
    if block_number >= POST_MERGE_BLOCK {
        86_400 / SECS_PER_BLOCK_POST_MERGE as u64
    } else {
        // Pre-Merge averaged ~13.5s/block.
        86_400 / 14
    }
}

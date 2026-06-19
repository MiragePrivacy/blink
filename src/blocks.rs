//! Approximate block-number → wall-clock-time conversion for supported chains.
//!
//! The contract Parquet schema does not store block timestamps, so the
//! dashboard derives them from `block_number`. Ethereum uses piecewise-linear
//! interpolation before the Merge and exact slots after. Gnosis uses calibrated
//! checkpoints because its long-run average block cadence has drifted enough
//! that a plain 5-second-from-genesis estimate mislabels recent blocks by months.

use chrono::{DateTime, TimeZone, Utc};

use crate::chains::GNOSIS_CHAIN_ID;

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

const GNOSIS_GENESIS_TIMESTAMP: i64 = 1_539_024_180;
const GNOSIS_SECS_PER_BLOCK: i64 = 5;

const GNOSIS_CHECKPOINTS: &[(u64, i64)] = &[
    (0, GNOSIS_GENESIS_TIMESTAMP), // 2018-10-08 xDai/Gnosis genesis
    (44_212_810, 1_768_666_350),   // 2026-01-17 16:12:30
    (46_762_380, 1_781_798_438),   // 2026-06-18 16:00:38
];

/// Approximate the block timestamp for a given chain and block number.
pub fn block_timestamp(chain_id: u64, block_number: u64) -> DateTime<Utc> {
    let secs = match chain_id {
        GNOSIS_CHAIN_ID => gnosis_block_timestamp_secs(block_number),
        _ => ethereum_block_timestamp_secs(block_number),
    };
    Utc.timestamp_opt(secs, 0).single().unwrap_or_else(Utc::now)
}

/// Approximate the block number for a given chain and timestamp.
pub fn block_number_at_time(chain_id: u64, timestamp: DateTime<Utc>) -> u64 {
    match chain_id {
        GNOSIS_CHAIN_ID => gnosis_block_number_at_time(timestamp.timestamp()),
        _ => ethereum_block_number_at_time(timestamp.timestamp()),
    }
}

fn gnosis_block_timestamp_secs(block_number: u64) -> i64 {
    let (last_block, last_timestamp) = GNOSIS_CHECKPOINTS[GNOSIS_CHECKPOINTS.len() - 1];
    if block_number >= last_block {
        return last_timestamp + (block_number - last_block) as i64 * GNOSIS_SECS_PER_BLOCK;
    }
    interpolate_checkpoint_timestamp(GNOSIS_CHECKPOINTS, block_number)
}

fn gnosis_block_number_at_time(timestamp: i64) -> u64 {
    let (last_block, last_timestamp) = GNOSIS_CHECKPOINTS[GNOSIS_CHECKPOINTS.len() - 1];
    if timestamp >= last_timestamp {
        let blocks = (timestamp - last_timestamp) / GNOSIS_SECS_PER_BLOCK;
        return last_block + blocks.max(0) as u64;
    }
    interpolate_checkpoint_block(GNOSIS_CHECKPOINTS, timestamp)
}

fn ethereum_block_timestamp_secs(block_number: u64) -> i64 {
    if block_number >= POST_MERGE_BLOCK {
        POST_MERGE_TIMESTAMP + (block_number - POST_MERGE_BLOCK) as i64 * SECS_PER_BLOCK_POST_MERGE
    } else {
        interpolate_checkpoint_timestamp(CHECKPOINTS, block_number)
    }
}

fn ethereum_block_number_at_time(timestamp: i64) -> u64 {
    if timestamp >= POST_MERGE_TIMESTAMP {
        let blocks = (timestamp - POST_MERGE_TIMESTAMP) / SECS_PER_BLOCK_POST_MERGE;
        return POST_MERGE_BLOCK + blocks.max(0) as u64;
    }

    interpolate_checkpoint_block(CHECKPOINTS, timestamp)
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

#[cfg(test)]
mod tests {
    use chrono::{Datelike, TimeZone};

    use super::{block_number_at_time, block_timestamp};
    use crate::chains::GNOSIS_CHAIN_ID;

    #[test]
    fn gnosis_recent_blocks_map_to_current_calendar_dates() {
        assert_eq!(
            block_timestamp(GNOSIS_CHAIN_ID, 46_762_380),
            chrono::Utc
                .with_ymd_and_hms(2026, 6, 18, 16, 0, 38)
                .unwrap()
        );
    }

    #[test]
    fn gnosis_recent_time_ranges_convert_back_to_blocks() {
        let block = block_number_at_time(
            GNOSIS_CHAIN_ID,
            chrono::Utc.with_ymd_and_hms(2026, 6, 19, 0, 0, 0).unwrap(),
        );

        assert!(block > 46_762_380);
        assert_eq!(
            block_timestamp(GNOSIS_CHAIN_ID, block).date_naive().month(),
            6
        );
    }
}

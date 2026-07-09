//! Block <-> timestamp mapping tests.

use blink::blocks::{block_number_at_time, block_timestamp};
use blink::chains::{ETHEREUM_CHAIN_ID, GNOSIS_CHAIN_ID};
use chrono::{Datelike, TimeZone};

#[test]
fn ethereum_recent_blocks_map_to_current_calendar_dates() {
    // Real anchor: block 25,497,188 was mined 2026-07-09 19:42:23 UTC. The
    // pre-fix 12s-flat extrapolation from the merge put this ten days early.
    assert_eq!(
        block_timestamp(ETHEREUM_CHAIN_ID, 25_497_188),
        chrono::Utc
            .with_ymd_and_hms(2026, 7, 9, 19, 42, 23)
            .unwrap()
    );
    // Blocks between checkpoints interpolate to the right part of the
    // calendar: block 24.5M sits a third of the way through the
    // 2025-12-13 → 2026-07-09 span, i.e. late February 2026.
    let ts = block_timestamp(ETHEREUM_CHAIN_ID, 24_500_000);
    assert_eq!(ts.date_naive().year(), 2026);
    assert_eq!(ts.date_naive().month(), 2);
}

#[test]
fn ethereum_extrapolation_past_newest_checkpoint_uses_measured_rate() {
    // A day's worth of blocks past the newest checkpoint should land ~a day
    // later, not drift with the ideal 12s slot time.
    let later = block_timestamp(ETHEREUM_CHAIN_ID, 25_497_188 + 7_146); // ≈1 day at 12.09s
    let anchor = block_timestamp(ETHEREUM_CHAIN_ID, 25_497_188);
    let delta = (later - anchor).num_seconds();
    assert!((86_000..87_000).contains(&delta), "delta {delta}");

    let round_trip = block_number_at_time(ETHEREUM_CHAIN_ID, later);
    assert!((round_trip as i64 - (25_497_188 + 7_146) as i64).abs() < 5);
}

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

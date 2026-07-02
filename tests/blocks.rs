//! Block <-> timestamp mapping tests.

use blink::blocks::{block_number_at_time, block_timestamp};
use blink::chains::GNOSIS_CHAIN_ID;
use chrono::{Datelike, TimeZone};

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

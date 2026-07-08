//! Time-window parsing, cache-key normalization, and runtime-state tests for
//! the dashboard server.

use blink::chains::{ETHEREUM_CHAIN_ID, GNOSIS_CHAIN_ID};
use blink::serve::{
    bucket_cache_key, parse_time_series_window, range_cache_key_for_query, BucketQuery,
    RuntimeState,
};

fn query(range: Option<&str>, bucket: Option<&str>) -> BucketQuery {
    BucketQuery {
        chain_id: None,
        bucket: bucket.map(str::to_string),
        range: range.map(str::to_string),
        end_block: None,
        start_block: None,
        start_time: None,
        end_time: None,
        limit: None,
    }
}

#[test]
fn day_range_limits_chart_to_last_day_with_hourly_buckets() {
    let anchor_block = 20_000_000;
    let window =
        parse_time_series_window(&query(Some("day"), None), ETHEREUM_CHAIN_ID, anchor_block);

    assert_eq!(window.block_range, Some((19_992_801, 20_000_000)));
    assert_eq!(window.bucket_blocks, 300);
}

#[test]
fn week_range_limits_chart_to_last_week_with_daily_buckets() {
    let anchor_block = 20_000_000;
    let window =
        parse_time_series_window(&query(Some("week"), None), ETHEREUM_CHAIN_ID, anchor_block);

    assert_eq!(window.block_range, Some((19_949_601, 20_000_000)));
    assert_eq!(window.bucket_blocks, 7_200);
}

#[test]
fn year_range_limits_chart_to_last_year_with_monthly_buckets() {
    let anchor_block = 20_000_000;
    let window =
        parse_time_series_window(&query(Some("year"), None), ETHEREUM_CHAIN_ID, anchor_block);

    assert_eq!(window.block_range, Some((17_372_001, 20_000_000)));
    assert_eq!(window.bucket_blocks, 216_000);
}

#[test]
fn hour_range_uses_chain_specific_block_time() {
    let anchor_block = 46_000_000;
    let window =
        parse_time_series_window(&query(Some("hour"), None), GNOSIS_CHAIN_ID, anchor_block);

    assert_eq!(window.block_range, Some((45_999_281, 46_000_000)));
    assert_eq!(window.bucket_blocks, 60);
}

#[test]
fn legacy_bucket_query_keeps_full_history_behavior() {
    let anchor_block = 20_000_000;
    let window =
        parse_time_series_window(&query(None, Some("day")), ETHEREUM_CHAIN_ID, anchor_block);

    assert_eq!(window.block_range, None);
    assert_eq!(window.bucket_blocks, 7_200);
}

#[test]
fn range_end_block_moves_visible_window() {
    let anchor_block = 20_000_000;
    let mut q = query(Some("day"), None);
    q.end_block = Some(19_000_000);
    let window = parse_time_series_window(&q, ETHEREUM_CHAIN_ID, anchor_block);

    assert_eq!(window.block_range, Some((18_992_801, 19_000_000)));

    q.end_block = Some(21_000_000);
    let capped = parse_time_series_window(&q, ETHEREUM_CHAIN_ID, anchor_block);
    assert_eq!(capped.block_range, Some((19_992_801, 20_000_000)));
}

#[test]
fn relative_preset_cache_key_survives_tail_moves_inside_bucket() {
    let q = query(Some("day"), None);
    let first = parse_time_series_window(&q, ETHEREUM_CHAIN_ID, 20_000_001);
    let second = parse_time_series_window(&q, ETHEREUM_CHAIN_ID, 20_000_099);

    assert_ne!(first.block_range, second.block_range);
    assert_eq!(
        bucket_cache_key(ETHEREUM_CHAIN_ID, &q, first),
        bucket_cache_key(ETHEREUM_CHAIN_ID, &q, second)
    );
    assert_eq!(
        range_cache_key_for_query(ETHEREUM_CHAIN_ID, &q, first),
        range_cache_key_for_query(ETHEREUM_CHAIN_ID, &q, second)
    );
}

#[test]
fn explicit_block_range_cache_key_keeps_exact_window() {
    let anchor_block = 20_000_100;
    let mut first_query = query(Some("day"), None);
    first_query.end_block = Some(20_000_001);
    let first = parse_time_series_window(&first_query, ETHEREUM_CHAIN_ID, anchor_block);

    let mut second_query = query(Some("day"), None);
    second_query.end_block = Some(20_000_099);
    let second = parse_time_series_window(&second_query, ETHEREUM_CHAIN_ID, anchor_block);

    assert_ne!(first.block_range, second.block_range);
    assert_ne!(
        bucket_cache_key(ETHEREUM_CHAIN_ID, &first_query, first),
        bucket_cache_key(ETHEREUM_CHAIN_ID, &second_query, second)
    );
}

#[test]
fn explicit_start_block_creates_custom_window() {
    let anchor_block = 20_000_000;
    let mut q = query(None, None);
    q.start_block = Some(19_900_000);
    q.end_block = Some(19_950_000);
    let window = parse_time_series_window(&q, ETHEREUM_CHAIN_ID, anchor_block);

    assert_eq!(window.block_range, Some((19_900_000, 19_950_000)));
    assert_eq!(window.bucket_blocks, 520);
}

#[tokio::test]
async fn gnosis_tail_updates_do_not_overwrite_legacy_ethereum_runtime_block() {
    let runtime = RuntimeState::new(false, true, 60);

    runtime
        .mark_tail_ready(ETHEREUM_CHAIN_ID, Some(25_438_551))
        .await;
    runtime
        .mark_tail_ready(GNOSIS_CHAIN_ID, Some(46_981_003))
        .await;

    let response = runtime.response().await;
    assert_eq!(response.snapshot.tail_last_block, Some(25_438_551));

    let gnosis = response
        .snapshot
        .tail_chains
        .iter()
        .find(|chain| chain.chain_id == GNOSIS_CHAIN_ID)
        .expect("gnosis runtime state");
    assert_eq!(gnosis.tail_last_block, Some(46_981_003));

    runtime
        .mark_tail_ok(Some(GNOSIS_CHAIN_ID), Some(46_981_010), 8)
        .await;
    let response = runtime.response().await;
    assert_eq!(response.snapshot.tail_last_block, Some(25_438_551));

    runtime
        .mark_tail_ok(Some(ETHEREUM_CHAIN_ID), Some(25_438_552), 13)
        .await;
    let response = runtime.response().await;
    assert_eq!(response.snapshot.tail_last_block, Some(25_438_552));
}

#[tokio::test]
async fn ready_chain_does_not_flicker_to_tailing_during_background_scan() {
    let runtime = RuntimeState::new(false, true, 60);

    runtime
        .mark_tail_ready(ETHEREUM_CHAIN_ID, Some(25_438_551))
        .await;
    runtime.mark_tail_start(Some(ETHEREUM_CHAIN_ID)).await;

    let response = runtime.response().await;
    assert!(!response.snapshot.tail_running);
    assert_eq!(response.snapshot.tail_running_count, 0);

    let ethereum = response
        .snapshot
        .tail_chains
        .iter()
        .find(|chain| chain.chain_id == ETHEREUM_CHAIN_ID)
        .expect("ethereum runtime state");
    assert!(!ethereum.tail_running);
    assert_eq!(ethereum.tail_running_count, 0);
}

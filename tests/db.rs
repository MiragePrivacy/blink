//! Dashboard query-layer tests: rollup ingest correctness (dedup across
//! overlapping parquet files, zellic + parquet union), chain filtering, and
//! the read-only SQL endpoint.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use blink::chains::ETHEREUM_CHAIN_ID;
use blink::db::Db;
use duckdb::Connection;

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(name: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "blink_db_test_{}_{}_{}",
            std::process::id(),
            name,
            unique
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn make_bytes(value: u8, len: usize) -> Vec<u8> {
    vec![value; len]
}

fn insert_zellic_snapshot(data_dir: &Path) {
    let conn = Connection::open(data_dir.join("blink.duckdb")).unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE zellic_bytecodes (
            code_hash BLOB,
            code BLOB,
            n_code_bytes UINTEGER
        );
        CREATE TABLE zellic_contracts (
            contract_address BLOB,
            bytecode_hash BLOB,
            block_number UINTEGER,
            create_index UINTEGER,
            chain_id UBIGINT
        );
        "#,
    )
    .unwrap();
    conn.execute(
        "INSERT INTO zellic_bytecodes VALUES (?, ?, ?)",
        duckdb::params![make_bytes(1, 32), make_bytes(0x60, 4), 4u32],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO zellic_contracts VALUES (?, ?, ?, ?, ?)",
        duckdb::params![make_bytes(2, 20), make_bytes(1, 32), 100u32, 0u32, 1u64],
    )
    .unwrap();
}

fn write_contract_parquet(path: &Path, block_number: u32, fill: u8, chain_id: u64) {
    let path_sql = path.display().to_string().replace('\'', "''");
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        r#"
        COPY (
            SELECT
                {block_number}::UINTEGER AS block_number,
                unhex(repeat('{fill:02x}', 32)) AS block_hash,
                0::UINTEGER AS create_index,
                unhex(repeat('{fill:02x}', 32)) AS transaction_hash,
                unhex(repeat('{addr:02x}', 20)) AS contract_address,
                unhex(repeat('{deployer:02x}', 20)) AS deployer,
                unhex(repeat('{deployer:02x}', 20)) AS factory,
                unhex('6000') AS init_code,
                unhex('6001') AS code,
                unhex(repeat('{fill:02x}', 32)) AS init_code_hash,
                2::UINTEGER AS n_init_code_bytes,
                2::UINTEGER AS n_code_bytes,
                unhex(repeat('{hash:02x}', 32)) AS code_hash,
                {chain_id}::UBIGINT AS chain_id
        ) TO '{path_sql}' (FORMAT PARQUET);
        "#,
        fill = fill,
        addr = fill.wrapping_add(2),
        deployer = fill.wrapping_add(3),
        hash = fill.wrapping_add(6),
    ))
    .unwrap();
}

/// Many deployments in one file: one row per block in `blocks`, all sharing
/// one code_hash (`repeat('aa', 32)`) with n_code_bytes = 100.
fn write_multi_block_parquet(path: &Path, blocks: &[u32], chain_id: u64) {
    let path_sql = path.display().to_string().replace('\'', "''");
    let block_list = blocks
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        r#"
        COPY (
            SELECT
                block_number::UINTEGER AS block_number,
                md5(('bh' || block_number)::VARCHAR)::BLOB AS block_hash,
                0::UINTEGER AS create_index,
                md5(('tx' || block_number)::VARCHAR)::BLOB AS transaction_hash,
                substr(md5(('ad' || block_number)::VARCHAR), 1, 20)::BLOB AS contract_address,
                unhex(repeat('11', 20)) AS deployer,
                NULL::BLOB AS factory,
                unhex('6000') AS init_code,
                unhex(repeat('60', 100)) AS code,
                md5('ih')::BLOB AS init_code_hash,
                2::UINTEGER AS n_init_code_bytes,
                100::UINTEGER AS n_code_bytes,
                unhex(repeat('aa', 32)) AS code_hash,
                {chain_id}::UBIGINT AS chain_id
            FROM (SELECT unnest([{block_list}]) AS block_number)
        ) TO '{path_sql}' (FORMAT PARQUET);
        "#
    ))
    .unwrap();
}

fn write_backfill_parquet(data_dir: &Path) {
    write_contract_parquet(
        &data_dir.join("contracts__0000000200__0000000200.parquet"),
        200,
        0x03,
        1,
    );
    write_contract_parquet(
        &data_dir.join("contracts__chain_0000000100__0000000300__0000000300.parquet"),
        300,
        0x13,
        100,
    );
}

fn write_overlapping_ethereum_parquet(data_dir: &Path) {
    // The same deployment written into two files: a backfill chunk and a tail
    // file. Rollup ingest must count it once.
    write_contract_parquet(
        &data_dir.join("contracts__0000000250__0000000250.parquet"),
        250,
        0x23,
        1,
    );
    write_contract_parquet(
        &data_dir.join("tail__chain_0000000001__0000000250__0000000250.parquet"),
        250,
        0x23,
        1,
    );
}

#[tokio::test]
async fn stats_and_recent_include_parquet_rows_newer_than_zellic() {
    let dir = TestDir::new("parquet_newer_than_zellic");
    insert_zellic_snapshot(&dir.path);
    write_backfill_parquet(&dir.path);

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

    let stats = db.stats(ETHEREUM_CHAIN_ID).await.unwrap();
    assert_eq!(stats.total_contracts, 2);
    assert_eq!(stats.first_block, 100);
    assert_eq!(stats.last_block, 200);

    let deploys = db
        .deploys_over_time(ETHEREUM_CHAIN_ID, 100, Some((0, 250)))
        .await
        .unwrap();
    assert_eq!(
        deploys
            .iter()
            .map(|bucket| (bucket.block_start, bucket.count))
            .collect::<Vec<_>>(),
        vec![(100, 1), (200, 1)]
    );

    let recent = db
        .recent_contracts(ETHEREUM_CHAIN_ID, 5, None)
        .await
        .unwrap();
    assert_eq!(recent.contracts.len(), 2);
    assert_eq!(recent.contracts[0].block_number, 200);
    assert_eq!(
        recent.contracts[0].address,
        format!("0x{}", hex_string(0x05, 20))
    );
    assert_eq!(recent.contracts[1].block_number, 100);
    assert!(!recent.has_more);
}

#[tokio::test]
async fn rollups_are_idempotent_across_reopens() {
    let dir = TestDir::new("rollup_idempotent_reopen");
    insert_zellic_snapshot(&dir.path);
    write_backfill_parquet(&dir.path);

    {
        let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
        assert_eq!(
            db.stats(ETHEREUM_CHAIN_ID).await.unwrap().total_contracts,
            2
        );
    }
    // Second open must not re-ingest tracked sources.
    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
    assert_eq!(
        db.stats(ETHEREUM_CHAIN_ID).await.unwrap().total_contracts,
        2
    );
    assert_eq!(db.stats(100).await.unwrap().total_contracts, 1);
}

#[tokio::test]
async fn refresh_ingests_new_tail_files_incrementally() {
    let dir = TestDir::new("refresh_ingests_tail");
    write_backfill_parquet(&dir.path);

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
    assert_eq!(
        db.stats(ETHEREUM_CHAIN_ID).await.unwrap().total_contracts,
        1
    );

    write_contract_parquet(
        &dir.path
            .join("tail__chain_0000000001__0000000400__0000000400.parquet"),
        400,
        0x33,
        1,
    );
    db.refresh().await.unwrap();

    let stats = db.stats(ETHEREUM_CHAIN_ID).await.unwrap();
    assert_eq!(stats.total_contracts, 2);
    assert_eq!(stats.last_block, 400);
    assert_eq!(
        db.highest_contract_block(ETHEREUM_CHAIN_ID).await.unwrap(),
        Some(400)
    );
}

#[tokio::test]
async fn chart_queries_deduplicate_overlapping_parquet_deployments() {
    let dir = TestDir::new("charts_deduplicate_overlapping_parquet");
    write_overlapping_ethereum_parquet(&dir.path);

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

    let deploys = db
        .deploys_over_time(ETHEREUM_CHAIN_ID, 100, Some((250, 250)))
        .await
        .unwrap();
    assert_eq!(deploys.len(), 1);
    assert_eq!(deploys[0].count, 1);

    let verified = db
        .verified_ratio_over_time(ETHEREUM_CHAIN_ID, 100, Some((250, 250)))
        .await
        .unwrap();
    assert_eq!(verified.len(), 1);
    assert_eq!(verified[0].verified, 0);
    assert_eq!(verified[0].unverified, 0);
    assert_eq!(verified[0].unknown, 1);

    db.execute_batch(
        r#"
        INSERT INTO bytecode_metadata_by_hash (
            code_hash,
            language,
            compiler_version,
            has_source_hash,
            is_erc20,
            is_erc721,
            is_erc1155,
            is_proxy_eip1967,
            is_proxy_minimal,
            uses_push0
        ) VALUES (unhex(repeat('29', 32)), 'solidity', '0.8.20', true, true, false, false, false, false, true)
        "#
        .to_string(),
    )
    .await
    .unwrap();

    let sizes = db
        .bytecode_size_distribution(ETHEREUM_CHAIN_ID, Some((250, 250)))
        .await
        .unwrap();
    assert_eq!(sizes.iter().map(|bin| bin.count).sum::<u64>(), 1);
    assert_eq!(
        sizes
            .iter()
            .find(|bin| bin.label == "1-32 B")
            .map(|bin| bin.count),
        Some(1)
    );

    let compilers = db
        .top_compilers(ETHEREUM_CHAIN_ID, 12, Some((250, 250)))
        .await
        .unwrap();
    assert_eq!(compilers.len(), 1);
    assert_eq!(compilers[0].compiler_version, "0.8.20");
    assert_eq!(compilers[0].count, 1);
    assert_eq!(
        db.compiler_version_total(ETHEREUM_CHAIN_ID, Some((250, 250)))
            .await
            .unwrap(),
        1
    );

    let standards = db
        .standards_breakdown(ETHEREUM_CHAIN_ID, Some((250, 250)))
        .await
        .unwrap();

    assert_eq!(standards.total_decoded, 1);
    assert_eq!(standards.erc20, 1);
    assert_eq!(standards.uses_push0, 1);
    assert_eq!(standards.has_source_hash, 1);
}

#[tokio::test]
async fn chart_anchor_uses_latest_contract_row_not_parquet_filename_end() {
    let dir = TestDir::new("chart_anchor_actual_contract_block");
    // File name claims coverage up to block 500 but the only row is at 300.
    write_contract_parquet(
        &dir.path
            .join("contracts__chain_0000000100__0000000300__0000000500.parquet"),
        300,
        0x13,
        100,
    );

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

    assert_eq!(db.highest_block(100).await.unwrap(), Some(500));
    assert_eq!(db.highest_contract_block(100).await.unwrap(), Some(300));
}

#[tokio::test]
async fn stats_and_recent_filter_gnosis_chain() {
    let dir = TestDir::new("gnosis_chain_filter");
    insert_zellic_snapshot(&dir.path);
    write_backfill_parquet(&dir.path);

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

    let stats = db.stats(100).await.unwrap();
    assert_eq!(stats.total_contracts, 1);
    assert_eq!(stats.first_block, 300);
    assert_eq!(stats.last_block, 300);

    let recent = db.recent_contracts(100, 5, None).await.unwrap();
    assert_eq!(recent.contracts.len(), 1);
    assert_eq!(recent.contracts[0].block_number, 300);
    assert_eq!(
        recent.contracts[0].address,
        format!("0x{}", hex_string(0x15, 20))
    );
}

#[tokio::test]
async fn recent_does_not_fall_back_to_ethereum_for_other_chains() {
    let dir = TestDir::new("no_cross_chain_recent_fallback");
    insert_zellic_snapshot(&dir.path);

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

    let recent = db.recent_contracts(100, 5, None).await.unwrap();
    assert!(recent.contracts.is_empty());
    assert!(!recent.has_more);
}

#[tokio::test]
async fn dashboard_sql_scopes_contract_metadata_by_chain() {
    let dir = TestDir::new("query_chain_scope");
    insert_zellic_snapshot(&dir.path);
    write_backfill_parquet(&dir.path);

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

    let result = db
        .query_sql(
            "SELECT chain_id, block_number FROM contract_metadata ORDER BY block_number"
                .to_string(),
            10,
            Some(100),
        )
        .await
        .unwrap();

    assert_eq!(result.row_count, 1);
    assert_eq!(result.rows[0][0], serde_json::json!(100));
    assert_eq!(result.rows[0][1], serde_json::json!(300));
}

fn hex_string(byte: u8, len: usize) -> String {
    hex::encode(vec![byte; len])
}

/// The SQL explorer's `contract_metadata` view must return identical data
/// before materialization (live-join fallback), after the background build,
/// and for fresh blocks beyond the materialized bounds (live union) — and a
/// backfill below the bounds must trigger a rebuild.
#[tokio::test]
async fn explorer_materialization_stays_correct_and_fresh() {
    let dir = TestDir::new("explorer_materialization");
    write_backfill_parquet(&dir.path);

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
    let explorer_query = "SELECT chain_id, block_number, address, compiler_version, is_verified \
                          FROM contract_metadata ORDER BY block_number DESC, create_index DESC LIMIT 10";

    // Fallback live-join view, pre-materialization.
    let before = db
        .query_sql(explorer_query.to_string(), 10, Some(1))
        .await
        .unwrap();
    assert_eq!(before.row_count, 1);
    assert_eq!(before.rows[0][1], serde_json::json!(200));

    // Build the materialized table; a second refresh is a no-op.
    assert!(db.refresh_explorer().await.unwrap());
    assert!(!db.refresh_explorer().await.unwrap());
    let after = db
        .query_sql(explorer_query.to_string(), 10, Some(1))
        .await
        .unwrap();
    assert_eq!(after.rows, before.rows);

    // New tail blocks beyond the bounds appear live without a rebuild.
    write_contract_parquet(
        &dir.path
            .join("tail__chain_0000000001__0000000400__0000000400.parquet"),
        400,
        0x33,
        1,
    );
    db.refresh().await.unwrap();
    let live = db
        .query_sql(explorer_query.to_string(), 10, Some(1))
        .await
        .unwrap();
    assert_eq!(live.row_count, 2);
    assert_eq!(live.rows[0][1], serde_json::json!(400));
    assert!(!db.refresh_explorer().await.unwrap());

    // A backfill below the materialized head forces a rebuild and the row
    // shows up decorated.
    write_contract_parquet(
        &dir.path
            .join("contracts__chain_0000000001__0000000150__0000000150.parquet"),
        150,
        0x43,
        1,
    );
    db.refresh().await.unwrap();
    assert!(db.refresh_explorer().await.unwrap());
    let rebuilt = db
        .query_sql(explorer_query.to_string(), 10, Some(1))
        .await
        .unwrap();
    assert_eq!(rebuilt.row_count, 3);
    assert_eq!(
        rebuilt
            .rows
            .iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![
            serde_json::json!(400),
            serde_json::json!(200),
            serde_json::json!(150)
        ]
    );
}

/// The default explorer query asks for newest deployments first. Keep the
/// materialized table in that physical order so DuckDB's Top-N scan can prune
/// old row groups instead of walking the chain's full history.
#[tokio::test]
async fn explorer_materialization_is_clustered_newest_first() {
    let dir = TestDir::new("explorer_newest_first");
    write_multi_block_parquet(
        &dir.path
            .join("contracts__chain_0000000001__0000000100__0000000300.parquet"),
        &[100, 300, 200],
        ETHEREUM_CHAIN_ID,
    );

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
    assert!(db.refresh_explorer().await.unwrap());

    let rows = db
        .query_sql(
            "SELECT block_number FROM contract_metadata_native WHERE chain_id = 1".to_string(),
            10,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        rows.rows
            .iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![
            serde_json::json!(300),
            serde_json::json!(200),
            serde_json::json!(100),
        ]
    );
}

/// While a schema-upgrading explorer rebuild runs (or after a failed one),
/// the on-disk table is the previous generation without newer columns —
/// aggregates must fall back to the join path instead of binder-erroring.
#[tokio::test]
async fn aggregates_fall_back_when_explorer_table_has_old_schema() {
    let dir = TestDir::new("explorer_old_schema_fallback");
    write_backfill_parquet(&dir.path);

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
    // Simulate a previous-generation materialized table: right names, no
    // is_decoded column.
    db.execute_batch(
        r#"
        CREATE TABLE contract_metadata_native (
            chain_id UBIGINT, block_number UINTEGER, is_erc20 BOOLEAN
        );
        CREATE TABLE IF NOT EXISTS contract_metadata_bounds (
            chain_id UBIGINT PRIMARY KEY, max_block UBIGINT NOT NULL
        );
        "#
        .to_string(),
    )
    .await
    .unwrap();

    let sizes = db
        .bytecode_size_distribution(ETHEREUM_CHAIN_ID, Some((0, 1000)))
        .await
        .unwrap();
    assert_eq!(sizes.iter().map(|bin| bin.count).sum::<u64>(), 1);
    let standards = db
        .standards_breakdown(ETHEREUM_CHAIN_ID, Some((0, 1000)))
        .await
        .unwrap();
    assert_eq!(standards.total_decoded, 0);
    assert!(db.language_distribution(ETHEREUM_CHAIN_ID).await.is_ok());
}

/// The Zellic snapshot is ingested in ~1M-row block-range slices (one
/// transaction each) so it cannot OOM small hosts. 2.5M rows forces three
/// slices; totals must come out exact.
#[tokio::test]
async fn large_zellic_snapshot_ingests_in_slices() {
    let dir = TestDir::new("zellic_sliced_ingest");
    {
        let conn = Connection::open(dir.path.join("blink.duckdb")).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE zellic_bytecodes (
                code_hash BLOB,
                code BLOB,
                n_code_bytes UINTEGER
            );
            CREATE TABLE zellic_contracts AS
            SELECT
                substr(md5(('a' || i)::VARCHAR), 1, 20)::BLOB AS contract_address,
                md5(('h' || i % 1000)::VARCHAR)::BLOB AS bytecode_hash,
                (i % 500000)::UINTEGER AS block_number,
                (i // 500000)::UINTEGER AS create_index,
                1::UBIGINT AS chain_id
            FROM range(2500000) t(i);
            INSERT INTO zellic_bytecodes
            SELECT DISTINCT bytecode_hash, unhex('60'), 1::UINTEGER FROM zellic_contracts;
            "#,
        )
        .unwrap();
    }

    // A tight memory limit mirrors the production 4GB host where a
    // single-transaction zellic ingest OOMed; sliced ingest must fit.
    let db = Db::open(
        &dir.path,
        "*.parquet",
        blink::db::DbOptions {
            memory_limit: Some("500MB".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let stats = db.stats(ETHEREUM_CHAIN_ID).await.unwrap();
    assert_eq!(stats.total_contracts, 2_500_000);
    assert_eq!(stats.first_block, 0);
    assert_eq!(stats.last_block, 499_999);

    // Reopening must not re-ingest (source recorded once after all slices).
    drop(db);
    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
    assert_eq!(
        db.stats(ETHEREUM_CHAIN_ID).await.unwrap().total_contracts,
        2_500_000
    );
}

/// Ranged code aggregates read fully-covered 10k-block buckets from the
/// bucketed rollup and only scan the deployments table for partial edges —
/// results must be exact across all bucket-alignment cases.
#[tokio::test]
async fn ranged_code_aggregates_are_exact_across_bucket_boundaries() {
    let dir = TestDir::new("bucket_boundary_ranges");
    // Lead edge (5000), one full bucket (10000..=19999, 10 rows), trail edge (25000).
    let mut blocks: Vec<u32> = vec![5_000];
    blocks.extend((0..10).map(|i| 10_000 + i * 1_000));
    blocks.push(25_000);
    write_multi_block_parquet(
        &dir.path
            .join("contracts__chain_0000000001__0000000000__0000030000.parquet"),
        &blocks,
        1,
    );

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
    db.execute_batch(
        r#"
        INSERT INTO bytecode_metadata_by_hash (
            code_hash, language, compiler_version, has_source_hash,
            is_erc20, is_erc721, is_erc1155, is_proxy_eip1967,
            is_proxy_minimal, uses_push0
        ) VALUES (unhex(repeat('aa', 32)), 'solidity', '0.8.24', true, false, false, false, false, false, true)
        "#
        .to_string(),
    )
    .await
    .unwrap();

    // Edges + full interior bucket.
    assert_eq!(
        db.compiler_version_total(1, Some((5_000, 25_000)))
            .await
            .unwrap(),
        12
    );
    // Exactly one full bucket, aligned on both sides.
    assert_eq!(
        db.compiler_version_total(1, Some((10_000, 19_999)))
            .await
            .unwrap(),
        10
    );
    // No fully covered bucket: pure edge scan.
    assert_eq!(
        db.compiler_version_total(1, Some((5_000, 9_999)))
            .await
            .unwrap(),
        1
    );
    // Range wider than the data.
    assert_eq!(
        db.compiler_version_total(1, Some((0, 1_000_000)))
            .await
            .unwrap(),
        12
    );
    // Standards and sizes flow through the same source.
    let standards = db
        .standards_breakdown(1, Some((5_000, 25_000)))
        .await
        .unwrap();
    assert_eq!(standards.uses_push0, 12);
    assert_eq!(standards.total_decoded, 12);
    let sizes = db
        .bytecode_size_distribution(1, Some((5_000, 25_000)))
        .await
        .unwrap();
    assert_eq!(sizes.iter().map(|bin| bin.count).sum::<u64>(), 12);

    // Once the materialized explorer table exists, the same endpoints switch
    // to denormalized per-deployment scans — results must be identical.
    assert!(db.refresh_explorer().await.unwrap());
    assert_eq!(
        db.compiler_version_total(1, Some((5_000, 25_000)))
            .await
            .unwrap(),
        12
    );
    assert_eq!(
        db.compiler_version_total(1, Some((10_000, 19_999)))
            .await
            .unwrap(),
        10
    );
    assert_eq!(
        db.compiler_version_total(1, Some((5_000, 9_999)))
            .await
            .unwrap(),
        1
    );
    let compilers = db
        .top_compilers(1, 12, Some((5_000, 25_000)))
        .await
        .unwrap();
    assert_eq!(compilers.len(), 1);
    assert_eq!(compilers[0].count, 12);
    let standards = db
        .standards_breakdown(1, Some((5_000, 25_000)))
        .await
        .unwrap();
    assert_eq!(standards.uses_push0, 12);
    assert_eq!(standards.has_source_hash, 12);
    assert_eq!(standards.total_decoded, 12);
    let sizes = db
        .bytecode_size_distribution(1, Some((5_000, 25_000)))
        .await
        .unwrap();
    assert_eq!(sizes.iter().map(|bin| bin.count).sum::<u64>(), 12);
    let languages = db.language_distribution(1).await.unwrap();
    assert_eq!(languages.len(), 1);
    assert_eq!(languages[0].language, "solidity");
    assert_eq!(languages[0].count, 12);
}

/// `blink load --overwrite` re-imports the Zellic snapshot; the invalidation
/// hook must subtract the old rollup contribution so the fresh snapshot is
/// re-ingested on the next open.
#[tokio::test]
async fn zellic_overwrite_invalidation_reingests_fresh_snapshot() {
    let dir = TestDir::new("zellic_overwrite_invalidation");
    insert_zellic_snapshot(&dir.path);
    {
        let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
        assert_eq!(
            db.stats(ETHEREUM_CHAIN_ID).await.unwrap().total_contracts,
            1
        );
    }

    // Simulate the `load --overwrite` flow: invalidate, then rebuild the
    // snapshot tables with different contents (two contracts now).
    {
        let conn = Connection::open(dir.path.join("blink.duckdb")).unwrap();
        blink::db::invalidate_zellic_rollups(&conn).unwrap();
        conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS zellic_contracts;
            DROP TABLE IF EXISTS zellic_bytecodes;
            CREATE TABLE zellic_bytecodes (
                code_hash BLOB,
                code BLOB,
                n_code_bytes UINTEGER
            );
            CREATE TABLE zellic_contracts (
                contract_address BLOB,
                bytecode_hash BLOB,
                block_number UINTEGER,
                create_index UINTEGER,
                chain_id UBIGINT
            );
            INSERT INTO zellic_bytecodes VALUES (unhex(repeat('01', 32)), unhex('60606060'), 4);
            INSERT INTO zellic_contracts VALUES (unhex(repeat('02', 20)), unhex(repeat('01', 32)), 100, 0, 1);
            INSERT INTO zellic_contracts VALUES (unhex(repeat('04', 20)), unhex(repeat('01', 32)), 150, 0, 1);
            "#,
        )
        .unwrap();
    }

    let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();
    let stats = db.stats(ETHEREUM_CHAIN_ID).await.unwrap();
    assert_eq!(stats.total_contracts, 2);
    assert_eq!(stats.first_block, 100);
    assert_eq!(stats.last_block, 150);

    let deploys = db
        .deploys_over_time(ETHEREUM_CHAIN_ID, 50, Some((0, 200)))
        .await
        .unwrap();
    assert_eq!(
        deploys
            .iter()
            .map(|bucket| (bucket.block_start, bucket.count))
            .collect::<Vec<_>>(),
        vec![(100, 1), (150, 1)]
    );
}

//! In-process Verifier Alliance import tests.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use blink::db::Db;
use duckdb::Connection;

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "blink_va_sync_test_{}_{}",
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn sql_path(path: &Path) -> String {
    path.display().to_string().replace('\'', "''")
}

fn write_va_fixture(root: &Path) {
    let deployments = root.join("contract_deployments");
    let verified = root.join("verified_contracts");
    fs::create_dir_all(&deployments).unwrap();
    fs::create_dir_all(&verified).unwrap();

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        r#"
        COPY (
            SELECT
                1::UBIGINT AS id,
                1::UBIGINT AS chain_id,
                unhex(repeat('11', 20)) AS address,
                100::BIGINT AS block_number,
                0::BIGINT AS transaction_index
        ) TO '{}' (FORMAT PARQUET);

        COPY (
            SELECT
                1::UBIGINT AS deployment_id,
                TIMESTAMP '2026-01-01 00:00:00' AS created_at,
                true AS runtime_match,
                false AS creation_match,
                false AS runtime_metadata_match,
                false AS creation_metadata_match
        ) TO '{}' (FORMAT PARQUET);
        "#,
        sql_path(&deployments.join("contract_deployments_0_1000.parquet")),
        sql_path(&verified.join("verified_contracts_0_1000.parquet")),
    ))
    .unwrap();
}

fn remove_va_verification(root: &Path) {
    let path = root
        .join("verified_contracts")
        .join("verified_contracts_0_1000.parquet");
    fs::remove_file(&path).unwrap();

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        r#"
        COPY (
            SELECT
                1::UBIGINT AS deployment_id,
                TIMESTAMP '2026-01-01 00:00:00' AS created_at,
                true AS runtime_match,
                false AS creation_match,
                false AS runtime_metadata_match,
                false AS creation_metadata_match
            WHERE false
        ) TO '{}' (FORMAT PARQUET);
        "#,
        sql_path(&path),
    ))
    .unwrap();
}

#[tokio::test]
async fn running_server_writer_imports_va_incrementally_without_reopen() {
    let data = TestDir::new();
    let va = TestDir::new();
    write_va_fixture(&va.0);

    let db = Db::open_with_mode(&data.0, "*.parquet", false).unwrap();
    assert!(db.import_verifier_alliance(va.0.clone(), 1).await.unwrap());

    let result = db
        .query_sql(
            "SELECT COUNT(*) FROM enrichment WHERE chain_id = 1 AND is_verified".to_string(),
            10,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);

    assert!(!db.import_verifier_alliance(va.0.clone(), 1).await.unwrap());
}

#[tokio::test]
async fn changed_va_partition_replaces_previous_enrichment_rows() {
    let data = TestDir::new();
    let va = TestDir::new();
    write_va_fixture(&va.0);

    let db = Db::open_with_mode(&data.0, "*.parquet", false).unwrap();
    assert!(db.import_verifier_alliance(va.0.clone(), 1).await.unwrap());

    remove_va_verification(&va.0);
    assert!(db.import_verifier_alliance(va.0.clone(), 1).await.unwrap());

    let result = db
        .query_sql(
            "SELECT COUNT(*) FROM enrichment WHERE chain_id = 1 AND is_verified".to_string(),
            10,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![serde_json::json!(0)]]);

    let imported = db
        .query_sql(
            "SELECT verified_count FROM verification_registry_imports WHERE source = 'verifier_alliance' AND chain_id = 1".to_string(),
            10,
            None,
        )
        .await
        .unwrap();
    assert_eq!(imported.rows, vec![vec![serde_json::json!(0)]]);
}

#[tokio::test]
async fn unchanged_va_files_repair_missing_enrichment_rows() {
    let data = TestDir::new();
    let va = TestDir::new();
    write_va_fixture(&va.0);

    let db = Db::open_with_mode(&data.0, "*.parquet", false).unwrap();
    assert!(db.import_verifier_alliance(va.0.clone(), 1).await.unwrap());

    // Reproduce an interrupted legacy import: file manifests and tracked
    // addresses committed, but the replacement enrichment rows did not.
    db.execute_batch(
        "DELETE FROM enrichment WHERE verification_source = 'verifier_alliance' AND chain_id = 1"
            .to_string(),
    )
    .await
    .unwrap();

    assert!(db.import_verifier_alliance(va.0.clone(), 1).await.unwrap());
    let result = db
        .query_sql(
            "SELECT COUNT(*) FROM enrichment WHERE chain_id = 1 AND is_verified".to_string(),
            10,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);
}

#[tokio::test]
async fn unchanged_va_files_repair_legacy_rows_without_positions() {
    let data = TestDir::new();
    let va = TestDir::new();
    write_va_fixture(&va.0);

    let db = Db::open_with_mode(&data.0, "*.parquet", false).unwrap();
    assert!(db.import_verifier_alliance(va.0.clone(), 1).await.unwrap());

    // Keep the recorded/enriched counts equal while reproducing a row from
    // before block positions were persisted. This must still trigger repair.
    db.execute_batch(
        r#"
        DELETE FROM enrichment
        WHERE verification_source = 'verifier_alliance' AND chain_id = 1;
        INSERT INTO enrichment (
            contract_address, chain_id, is_verified, contract_name, checked_at,
            verification_source, match_type, block_number, create_index
        ) VALUES (
            unhex(repeat('11', 20)), 1, true, NULL, CURRENT_TIMESTAMP,
            'verifier_alliance', 'runtime', NULL, NULL
        );
        "#
        .to_string(),
    )
    .await
    .unwrap();

    assert!(db.import_verifier_alliance(va.0.clone(), 1).await.unwrap());
    let result = db
        .query_sql(
            "SELECT block_number, create_index FROM enrichment WHERE chain_id = 1".to_string(),
            10,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![serde_json::json!(100), serde_json::json!(0)]]
    );
}

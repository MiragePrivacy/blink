//! Persistent block-time checkpoint tests.

use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use blink::{blocks, db::Db};

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "blink_checkpoint_test_{}_{}",
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

#[tokio::test]
async fn checkpoints_survive_reopen_and_drive_interpolation() {
    const CHAIN_ID: u64 = 9_999;
    let dir = TestDir::new();
    {
        let db = Db::open_with_mode(&dir.0, "*.parquet", false).unwrap();
        db.record_block_checkpoint(CHAIN_ID, 100, 1_000)
            .await
            .unwrap();
        db.record_block_checkpoint(CHAIN_ID, 200, 2_000)
            .await
            .unwrap();
    }

    blocks::replace_runtime_checkpoints(Default::default());
    let _db = Db::open_with_mode(&dir.0, "*.parquet", false).unwrap();
    assert_eq!(blocks::block_timestamp(CHAIN_ID, 150).timestamp(), 1_500);
    assert_eq!(
        blocks::block_number_at_time(
            CHAIN_ID,
            chrono::DateTime::from_timestamp(1_750, 0).unwrap()
        ),
        175
    );
}

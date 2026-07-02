//! Input detection tests for `blink load`.

use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use blink::load::{
    detect_inputs, detect_verifier_alliance_inputs, list_parquet_files, load_parquet_links,
};

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
            "blink_load_test_{}_{}_{}",
            std::process::id(),
            name,
            unique
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn touch(&self, name: &str) -> PathBuf {
        let path = self.path.join(name);
        fs::write(&path, []).unwrap();
        path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn names(paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap()
                .to_string()
        })
        .collect()
}

#[test]
fn detect_inputs_requires_both_csv_files_for_csv_load() {
    let dir = TestDir::new("csv_pair");
    dir.touch("contracts.csv");

    let inputs = detect_inputs(&dir.path, "*.parquet").unwrap();

    assert!(!inputs.has_normalized_csv);
    assert!(inputs.parquet_files.is_empty());

    dir.touch("bytecodes.csv");
    let inputs = detect_inputs(&dir.path, "*.parquet").unwrap();

    assert!(inputs.has_normalized_csv);
    assert_eq!(inputs.csv_contracts, dir.path.join("contracts.csv"));
    assert_eq!(inputs.csv_bytecodes, dir.path.join("bytecodes.csv"));
}

#[test]
fn list_parquet_files_filters_hidden_and_non_matching_files() {
    let dir = TestDir::new("parquet_filter");
    dir.touch("b.parquet");
    dir.touch("a.parquet");
    dir.touch(".hidden.parquet");
    dir.touch("contracts.csv");
    dir.touch("notes.txt");

    let files = list_parquet_files(&dir.path, "*.parquet").unwrap();

    assert_eq!(names(&files), vec!["a.parquet", "b.parquet"]);
}

#[test]
fn detect_inputs_applies_parquet_glob() {
    let dir = TestDir::new("parquet_glob");
    dir.touch("ethereum__contracts__1_to_2.parquet");
    dir.touch("other__contracts__1_to_2.parquet");

    let inputs = detect_inputs(&dir.path, "ethereum__*.parquet").unwrap();

    assert_eq!(
        names(&inputs.parquet_files),
        vec!["ethereum__contracts__1_to_2.parquet"]
    );
}

#[test]
fn detect_verifier_alliance_inputs_requires_both_tables() {
    let dir = TestDir::new("va_missing_table");
    fs::create_dir_all(dir.path.join("contract_deployments")).unwrap();

    let err = detect_verifier_alliance_inputs(Some(&dir.path)).unwrap_err();

    assert!(err.to_string().contains("--va needs both"));
}

#[test]
fn detect_verifier_alliance_inputs_finds_required_parquet_files() {
    let dir = TestDir::new("va_tables");
    let deployments = dir.path.join("contract_deployments");
    let verifications = dir.path.join("verified_contracts");
    fs::create_dir_all(&deployments).unwrap();
    fs::create_dir_all(&verifications).unwrap();
    fs::write(deployments.join("contract_deployments_0_1.parquet"), []).unwrap();
    fs::write(verifications.join("verified_contracts_0_1.parquet"), []).unwrap();

    let inputs = detect_verifier_alliance_inputs(Some(&dir.path))
        .unwrap()
        .unwrap();

    assert_eq!(inputs.contract_deployments.len(), 1);
    assert_eq!(inputs.verified_contracts.len(), 1);
}

#[test]
fn load_parquet_links_creates_symlinks_without_copying() {
    let src = TestDir::new("link_src");
    let dst = TestDir::new("link_dst");
    let parquet = src.touch("contracts__0000000001__0000000002.parquet");

    load_parquet_links(&src.path, &dst.path, std::slice::from_ref(&parquet), false).unwrap();

    let linked = dst.path.join("contracts__0000000001__0000000002.parquet");
    let metadata = fs::symlink_metadata(&linked).unwrap();
    assert!(metadata.file_type().is_symlink());
    assert_eq!(
        fs::canonicalize(linked).unwrap(),
        fs::canonicalize(parquet).unwrap()
    );
}

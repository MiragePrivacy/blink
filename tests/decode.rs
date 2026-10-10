//! Bytecode metadata decoding tests.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use blink::decode::bytecode_meta::{analyze, Language};
use duckdb::Connection;

struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "blink_decode_test_{}_{}_{}",
            std::process::id(),
            name,
            unique
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn empty_bytecode() {
    let m = analyze(&[]);
    assert!(m.compiler_version.is_none());
    assert!(!m.is_erc20);
}

#[test]
fn detects_push0() {
    let code = [0x5f, 0x00];
    let m = analyze(&code);
    assert!(m.uses_push0);
}

#[test]
fn detects_erc20_selectors() {
    let mut code = vec![];
    for sel in [
        [0x18u8, 0x16, 0x0d, 0xdd],
        [0xa9, 0x05, 0x9c, 0xbb],
        [0xdd, 0x62, 0xed, 0x3e],
    ] {
        code.push(0x63);
        code.extend_from_slice(&sel);
    }
    let m = analyze(&code);
    assert!(m.is_erc20);
}

#[test]
fn parses_solc_metadata() {
    // CBOR: a1 64 73 6f 6c 63 43 00 08 14  → { "solc": h'000814' } (= 0.8.20)
    // length suffix: 00 0a (10 bytes)
    let code = vec![
        0xa1, 0x64, 0x73, 0x6f, 0x6c, 0x63, 0x43, 0x00, 0x08, 0x14, 0x00, 0x0a,
    ];
    let m = analyze(&code);
    assert_eq!(m.language, Some(Language::Solidity));
    assert_eq!(m.compiler_version.as_deref(), Some("0.8.20"));
}

#[test]
fn detects_eip1167_minimal_proxy() {
    // Construct the canonical 45-byte runtime with a dummy impl address.
    let mut code = vec![];
    code.extend_from_slice(&[0x36, 0x3d, 0x3d, 0x37, 0x3d, 0x3d, 0x3d, 0x36, 0x3d, 0x73]);
    code.extend_from_slice(&[0xab; 20]); // implementation address
    code.extend_from_slice(&[
        0x5a, 0xf4, 0x3d, 0x82, 0x80, 0x3e, 0x90, 0x3d, 0x91, 0x60, 0x2b, 0x57, 0xfd, 0x5b, 0xf3,
    ]);
    assert_eq!(code.len(), 45);
    let m = analyze(&code);
    assert!(m.is_proxy_minimal, "should detect EIP-1167 minimal proxy");
    // Should not also flag as a different proxy type.
    assert!(!m.is_proxy_eip1967);
}

#[test]
fn rejects_minimal_proxy_with_wrong_length() {
    // Same shape but one byte short — not a valid EIP-1167.
    let mut code = vec![0x36, 0x3d, 0x3d, 0x37, 0x3d, 0x3d, 0x3d, 0x36, 0x3d, 0x73];
    code.extend_from_slice(&[0xab; 19]);
    code.extend_from_slice(&[
        0x5a, 0xf4, 0x3d, 0x82, 0x80, 0x3e, 0x90, 0x3d, 0x91, 0x60, 0x2b, 0x57, 0xfd, 0x5b, 0xf3,
    ]);
    assert_eq!(code.len(), 44);
    let m = analyze(&code);
    assert!(!m.is_proxy_minimal);
}

#[test]
fn ignores_absurd_cbor_map_count() {
    let code = vec![
        0xba, 0x99, 0xb6, 0x26, 0x57, // map(2578851415)
        0x00, 0x05, // metadata length: 5 bytes
    ];
    let m = analyze(&code);
    assert!(m.language.is_none());
    assert!(m.compiler_version.is_none());
}

#[test]
fn decode_backfills_creation_and_runtime_opcode_sets_idempotently() {
    let dir = TestDir::new("opcode_sets");
    let parquet_path = dir.path().join("contracts__0000000100__0000000100.parquet");
    let parquet_path_sql = parquet_path.display().to_string().replace('\'', "''");
    Connection::open_in_memory()
        .unwrap()
        .execute_batch(&format!(
            r#"
            COPY (
                SELECT
                    100::UINTEGER AS block_number,
                    unhex(repeat('01', 32)) AS block_hash,
                    0::UINTEGER AS create_index,
                    unhex(repeat('02', 32)) AS transaction_hash,
                    unhex(repeat('03', 20)) AS contract_address,
                    unhex(repeat('04', 20)) AS deployer,
                    unhex(repeat('05', 20)) AS factory,
                    unhex('f500') AS init_code,
                    unhex('61f4f5f4') AS code,
                    unhex(repeat('06', 32)) AS init_code_hash,
                    2::UINTEGER AS n_init_code_bytes,
                    4::UINTEGER AS n_code_bytes,
                    unhex(repeat('07', 32)) AS code_hash,
                    1::UBIGINT AS chain_id
            ) TO '{parquet_path_sql}' (FORMAT PARQUET);
            "#
        ))
        .unwrap();

    for _ in 0..2 {
        let output = Command::new(env!("CARGO_BIN_EXE_blink"))
            .args([
                "decode",
                "--data-dir",
                dir.path().to_str().unwrap(),
                "--batch-size",
                "1",
            ])
            .env("PATH", "")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "blink decode failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let conn = Connection::open(dir.path().join("blink.duckdb")).unwrap();
    let (sets, links, runs): (u64, u64, u64) = conn
        .query_row(
            r#"
            SELECT
                (SELECT COUNT(*) FROM bytecode_opcode_sets),
                (SELECT COUNT(*) FROM contract_creation_bytecodes),
                (SELECT COUNT(*) FROM opcode_decode_runs)
            "#,
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((sets, links, runs), (2, 1, 1));

    let (creation_has_create2, runtime_has_delegatecall, runtime_has_create2): (bool, bool, bool) =
        conn.query_row(
            r#"
            SELECT
                (SELECT list_contains(opcodes, 'CREATE2')
                 FROM bytecode_opcode_sets WHERE code_kind = 'creation'),
                (SELECT list_contains(opcodes, 'DELEGATECALL')
                 FROM bytecode_opcode_sets WHERE code_kind = 'runtime'),
                (SELECT list_contains(opcodes, 'CREATE2')
                 FROM bytecode_opcode_sets WHERE code_kind = 'runtime')
            "#,
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert!(creation_has_create2);
    assert!(runtime_has_delegatecall);
    assert!(
        !runtime_has_create2,
        "CREATE2 bytes inside PUSH2 data must not be indexed as an instruction"
    );
}

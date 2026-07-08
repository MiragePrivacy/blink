//! Bytecode metadata decoding tests.

use blink::decode::bytecode_meta::{analyze, Language};

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

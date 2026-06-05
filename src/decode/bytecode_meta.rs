//! Static analysis of contract runtime bytecode.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BytecodeMetadata {
    pub language: Option<Language>,
    pub compiler_version: Option<String>,
    pub has_source_hash: bool,
    pub is_erc20: bool,
    pub is_erc721: bool,
    pub is_erc1155: bool,
    /// EIP-1167 minimal proxy — the 45-byte fixed-shape clone that DELEGATECALLs
    /// to a hardcoded implementation address. Dominates Ethereum by raw count
    /// (Uniswap pools, OZ Clones, Gnosis Safe wallets, AA accounts, NFT collections).
    pub is_proxy_minimal: bool,
    /// EIP-1967 transparent upgradeable proxy — detected by the keccak'd
    /// implementation storage slot constant appearing as a PUSH32 immediate.
    pub is_proxy_eip1967: bool,
    pub uses_push0: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Solidity,
    Vyper,
    Other,
}

impl Language {
    pub fn as_str(&self) -> &'static str {
        match self {
            Language::Solidity => "solidity",
            Language::Vyper => "vyper",
            Language::Other => "other",
        }
    }
}

/// Run all passes over a contract's runtime bytecode.
pub fn analyze(code: &[u8]) -> BytecodeMetadata {
    let mut meta = BytecodeMetadata::default();
    decode_cbor_tail(code, &mut meta);
    scan_opcodes(code, &mut meta);
    detect_minimal_proxy(code, &mut meta);
    meta
}

// ─── EIP-1167 minimal proxy ──────────────────────────────────────────────────

/// Standard EIP-1167 minimal proxy is exactly 45 bytes:
///   `363d3d373d3d3d363d73 <impl-address:20> 5af43d82803e903d91602b57fd5bf3`
/// The 10-byte prefix loads calldata, the 15-byte suffix DELEGATECALLs and
/// returns. Only the embedded 20-byte address varies between deployments.
const EIP1167_PREFIX: [u8; 10] = [0x36, 0x3d, 0x3d, 0x37, 0x3d, 0x3d, 0x3d, 0x36, 0x3d, 0x73];
const EIP1167_SUFFIX: [u8; 15] = [
    0x5a, 0xf4, 0x3d, 0x82, 0x80, 0x3e, 0x90, 0x3d, 0x91, 0x60, 0x2b, 0x57, 0xfd, 0x5b, 0xf3,
];

fn detect_minimal_proxy(code: &[u8], meta: &mut BytecodeMetadata) {
    if code.len() == 45 && code[0..10] == EIP1167_PREFIX && code[30..45] == EIP1167_SUFFIX {
        meta.is_proxy_minimal = true;
    }
}

fn decode_cbor_tail(code: &[u8], meta: &mut BytecodeMetadata) {
    if code.len() < 4 {
        return;
    }
    let n = code.len();
    let metadata_len = u16::from_be_bytes([code[n - 2], code[n - 1]]) as usize;
    if metadata_len == 0 || metadata_len + 2 > n {
        return;
    }
    let cbor = &code[n - 2 - metadata_len..n - 2];
    if let Ok(map) = parse_cbor_map(cbor) {
        for (key, value) in &map {
            match key.as_str() {
                "solc" => {
                    meta.language = Some(Language::Solidity);
                    if let CborValue::Bytes(b) = value {
                        meta.compiler_version = format_solc_version(b);
                    }
                }
                "vyper" => {
                    meta.language = Some(Language::Vyper);
                    if let CborValue::Bytes(b) = value {
                        meta.compiler_version = format_solc_version(b);
                    }
                }
                "ipfs" | "bzzr0" | "bzzr1" => {
                    meta.has_source_hash = true;
                }
                _ => {}
            }
        }
        if meta.language.is_none() && !map.is_empty() {
            meta.language = Some(Language::Other);
        }
    }
}

fn format_solc_version(bytes: &[u8]) -> Option<String> {
    if bytes.len() == 3 {
        Some(format!("{}.{}.{}", bytes[0], bytes[1], bytes[2]))
    } else if bytes.len() == 4 {
        // Vyper occasionally encodes a 4th nibble as build metadata.
        Some(format!(
            "{}.{}.{}-{}",
            bytes[0], bytes[1], bytes[2], bytes[3]
        ))
    } else {
        None
    }
}

#[derive(Debug, Clone)]
enum CborValue {
    Bytes(Vec<u8>),
    Text,
    Bool,
    Other,
}

fn parse_cbor_map(buf: &[u8]) -> Result<Vec<(String, CborValue)>, ()> {
    let mut p = 0usize;
    let map_count = read_map_header(buf, &mut p)?;
    if map_count > 32 || map_count > buf.len() / 2 {
        return Err(());
    }
    let mut out = Vec::with_capacity(map_count);
    for _ in 0..map_count {
        let key = read_text(buf, &mut p)?;
        let value = read_value(buf, &mut p)?;
        out.push((key, value));
    }
    Ok(out)
}

fn read_map_header(buf: &[u8], p: &mut usize) -> Result<usize, ()> {
    let b = *buf.get(*p).ok_or(())?;
    *p += 1;
    if (b >> 5) != 5 {
        return Err(());
    }
    read_length(buf, p, b & 0x1f)
}

fn read_text(buf: &[u8], p: &mut usize) -> Result<String, ()> {
    let b = *buf.get(*p).ok_or(())?;
    *p += 1;
    if (b >> 5) != 3 {
        return Err(());
    }
    let len = read_length(buf, p, b & 0x1f)?;
    let end = p.checked_add(len).ok_or(())?;
    let bytes = buf.get(*p..end).ok_or(())?;
    *p = end;
    String::from_utf8(bytes.to_vec()).map_err(|_| ())
}

fn read_value(buf: &[u8], p: &mut usize) -> Result<CborValue, ()> {
    let b = *buf.get(*p).ok_or(())?;
    *p += 1;
    let major = b >> 5;
    let info = b & 0x1f;
    match major {
        2 => {
            let len = read_length(buf, p, info)?;
            let end = p.checked_add(len).ok_or(())?;
            let bytes = buf.get(*p..end).ok_or(())?.to_vec();
            *p = end;
            Ok(CborValue::Bytes(bytes))
        }
        3 => {
            let len = read_length(buf, p, info)?;
            let end = p.checked_add(len).ok_or(())?;
            let bytes = buf.get(*p..end).ok_or(())?;
            *p = end;
            String::from_utf8(bytes.to_vec()).map_err(|_| ())?;
            Ok(CborValue::Text)
        }
        7 => match b {
            0xf4 | 0xf5 => Ok(CborValue::Bool),
            _ => Ok(CborValue::Other),
        },
        _ => {
            // Skip over unsupported value types so the parser can keep going.
            let _ = read_length(buf, p, info);
            Ok(CborValue::Other)
        }
    }
}

fn read_length(buf: &[u8], p: &mut usize, info: u8) -> Result<usize, ()> {
    match info {
        0..=23 => Ok(info as usize),
        24 => {
            let v = *buf.get(*p).ok_or(())?;
            *p += 1;
            Ok(v as usize)
        }
        25 => {
            let bytes = buf.get(*p..*p + 2).ok_or(())?;
            *p += 2;
            Ok(u16::from_be_bytes([bytes[0], bytes[1]]) as usize)
        }
        26 => {
            let bytes = buf.get(*p..*p + 4).ok_or(())?;
            *p += 4;
            Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize)
        }
        _ => Err(()),
    }
}

// ─── opcode scan ──────────────────────────────────────────────────────────────

/// EIP-1967 implementation storage slot:
///   `keccak256("eip1967.proxy.implementation") - 1`
const EIP1967_IMPL_SLOT: [u8; 32] = [
    0x36, 0x08, 0x94, 0xa1, 0x3b, 0xa1, 0xa3, 0x21, 0x06, 0x67, 0xc8, 0x28, 0x49, 0x2d, 0xb9, 0x8d,
    0xca, 0x3e, 0x20, 0x76, 0xcc, 0x37, 0x35, 0xa9, 0x20, 0xa3, 0xca, 0x50, 0x5d, 0x38, 0x2b, 0xbc,
];

const PUSH0: u8 = 0x5f;
const PUSH1: u8 = 0x60;
const PUSH4: u8 = 0x63;
const PUSH32: u8 = 0x7f;

/// Bit-packed flags for the 7 specific selectors we care about. Replaces the
/// previous `HashSet<[u8; 4]>` allocation per contract — for 28.6M contracts
/// that was 28.6M heap allocs and 28.6M pointer-chase lookups. A single byte
/// of flag bits + direct `match` on the 4-byte array is ~10× faster in release
/// and ~50× faster in debug.
#[derive(Default)]
struct SelectorFlags {
    bits: u8,
}

const SEL_ERC20_TOTAL_SUPPLY: u8 = 1 << 0; // 0x18160ddd
const SEL_ERC20_TRANSFER: u8 = 1 << 1; // 0xa9059cbb
const SEL_ERC20_ALLOWANCE: u8 = 1 << 2; // 0xdd62ed3e
const SEL_ERC721_OWNER_OF: u8 = 1 << 3; // 0x6352211e
const SEL_ERC721_SAFE_TX: u8 = 1 << 4; // 0x42842e0e
const SEL_ERC1155_BAL_BATCH: u8 = 1 << 5; // 0x4e1273f4
const SEL_ERC1155_SAFE_BATCH: u8 = 1 << 6; // 0x2eb2c2d6

const ERC20_MASK: u8 = SEL_ERC20_TOTAL_SUPPLY | SEL_ERC20_TRANSFER | SEL_ERC20_ALLOWANCE;
const ERC721_MASK: u8 = SEL_ERC721_OWNER_OF | SEL_ERC721_SAFE_TX;
const ERC1155_MASK: u8 = SEL_ERC1155_BAL_BATCH | SEL_ERC1155_SAFE_BATCH;

#[inline]
fn flag_selector(sel: &[u8], flags: &mut SelectorFlags) {
    flags.bits |= match sel {
        [0x18, 0x16, 0x0d, 0xdd] => SEL_ERC20_TOTAL_SUPPLY,
        [0xa9, 0x05, 0x9c, 0xbb] => SEL_ERC20_TRANSFER,
        [0xdd, 0x62, 0xed, 0x3e] => SEL_ERC20_ALLOWANCE,
        [0x63, 0x52, 0x21, 0x1e] => SEL_ERC721_OWNER_OF,
        [0x42, 0x84, 0x2e, 0x0e] => SEL_ERC721_SAFE_TX,
        [0x4e, 0x12, 0x73, 0xf4] => SEL_ERC1155_BAL_BATCH,
        [0x2e, 0xb2, 0xc2, 0xd6] => SEL_ERC1155_SAFE_BATCH,
        _ => 0,
    };
}

fn scan_opcodes(code: &[u8], meta: &mut BytecodeMetadata) {
    let mut flags = SelectorFlags::default();
    let len = code.len();
    let mut i = 0usize;
    while i < len {
        let op = unsafe { *code.get_unchecked(i) };
        if op == PUSH0 {
            meta.uses_push0 = true;
            i += 1;
            continue;
        }
        if op == PUSH4 && i + 5 <= len {
            flag_selector(&code[i + 1..i + 5], &mut flags);
        }
        if op == PUSH32 && i + 33 <= len && code[i + 1..i + 33] == EIP1967_IMPL_SLOT {
            meta.is_proxy_eip1967 = true;
        }
        if (PUSH1..=PUSH32).contains(&op) {
            i += 1 + (op - PUSH1 + 1) as usize;
        } else {
            i += 1;
        }
    }

    // Classify standards using bitmask checks — single AND per standard.
    // ERC-1155 first because its selectors are most distinctive.
    if (flags.bits & ERC1155_MASK) == ERC1155_MASK {
        meta.is_erc1155 = true;
    } else if (flags.bits & ERC721_MASK) == ERC721_MASK {
        meta.is_erc721 = true;
    } else if (flags.bits & ERC20_MASK) == ERC20_MASK {
        meta.is_erc20 = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            0x5a, 0xf4, 0x3d, 0x82, 0x80, 0x3e, 0x90, 0x3d, 0x91, 0x60, 0x2b, 0x57, 0xfd, 0x5b,
            0xf3,
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
            0x5a, 0xf4, 0x3d, 0x82, 0x80, 0x3e, 0x90, 0x3d, 0x91, 0x60, 0x2b, 0x57, 0xfd, 0x5b,
            0xf3,
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
}

//! EVM opcode-set extraction for creation and deployed runtime bytecode.
//!
//! The public dataset is presence-oriented, so this module returns each
//! mnemonic at most once. `PUSH1` through `PUSH32` payload bytes are always
//! skipped. Runtime bytecode is decoded linearly (minus recognized compiler
//! metadata); creation bytecode is decoded from PC 0 along statically
//! discoverable control-flow edges so embedded runtime code and constructor
//! arguments are not treated as constructor instructions merely because they
//! happen to contain opcode-shaped bytes.

use super::bytecode_meta::runtime_code_end;
use eot::OpCode;

/// Increment this whenever opcode parsing semantics change. Stored alongside
/// decoded rows so `blink decode` can backfill a new generation without
/// confusing it with older results.
pub const OPCODE_PARSER_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodeKind {
    Creation,
    Runtime,
}

impl CodeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Creation => "creation",
            Self::Runtime => "runtime",
        }
    }
}

/// Return the sorted, unique opcode mnemonics found in `code`.
pub fn opcode_names(code: &[u8], kind: CodeKind) -> Vec<String> {
    let mut present = [false; 256];
    match kind {
        CodeKind::Runtime => scan_linear(&code[..runtime_code_end(code)], &mut present),
        CodeKind::Creation => scan_reachable_creation(code, &mut present),
    }

    present
        .iter()
        .enumerate()
        .filter(|(_, found)| **found)
        .map(|(opcode, _)| opcode_name(opcode as u8))
        .collect()
}

fn scan_linear(code: &[u8], present: &mut [bool; 256]) {
    let mut pc = 0usize;
    while pc < code.len() {
        let opcode = code[pc];
        present[opcode as usize] = true;
        pc = next_pc(pc, opcode, code.len());
    }
}

/// Best-effort control-flow traversal for constructor bytecode.
///
/// Solidity and Vyper constructor branches use an immediate `PUSHn` target
/// directly before `JUMP`/`JUMPI`, which lets us follow their executable
/// basic blocks while stopping before the embedded runtime image. Dynamic
/// jump targets that cannot be resolved this way are conservatively not
/// followed: inventing instructions from the data tail would recreate the
/// raw-byte false positives this dataset exists to avoid.
fn scan_reachable_creation(code: &[u8], present: &mut [bool; 256]) {
    if code.is_empty() {
        return;
    }

    let mut worklist = vec![0usize];
    let mut visited = vec![false; code.len()];

    while let Some(mut pc) = worklist.pop() {
        let mut previous_push: Option<usize> = None;
        while pc < code.len() && !visited[pc] {
            visited[pc] = true;
            let opcode = code[pc];
            present[opcode as usize] = true;
            let following_pc = next_pc(pc, opcode, code.len());

            if opcode == 0x56 || opcode == 0x57 {
                if let Some(destination) = previous_push {
                    if destination < code.len() && code[destination] == 0x5b {
                        worklist.push(destination);
                    }
                }
                if opcode == 0x56 {
                    break;
                }
                previous_push = None;
                pc = following_pc;
                continue;
            }

            if is_halting(opcode) {
                break;
            }

            previous_push = push_value(code, pc, opcode);
            pc = following_pc;
        }
    }
}

#[inline]
fn next_pc(pc: usize, opcode: u8, code_len: usize) -> usize {
    let immediate_len = OpCode::from_byte(opcode)
        .info()
        .map(|info| usize::from(info.immediate_size))
        .unwrap_or(0);
    pc.saturating_add(1 + immediate_len).min(code_len)
}

fn push_value(code: &[u8], pc: usize, opcode: u8) -> Option<usize> {
    if !(0x60..=0x7f).contains(&opcode) {
        return None;
    }
    let width = usize::from(opcode - 0x5f);
    let immediate = code.get(pc + 1..pc + 1 + width)?;
    let mut value = 0usize;
    for &byte in immediate {
        value = value.checked_mul(256)?.checked_add(usize::from(byte))?;
    }
    Some(value)
}

#[inline]
fn is_halting(opcode: u8) -> bool {
    let opcode = OpCode::from_byte(opcode);
    opcode.terminates() || opcode == OpCode::SELFDESTRUCT
}

fn opcode_name(opcode: u8) -> String {
    let opcode = OpCode::from_byte(opcode);
    match opcode.info() {
        Some(info) => info.name.to_string(),
        None => format!("UNKNOWN_0x{:02X}", opcode.byte()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn push_payload_is_not_an_opcode() {
        let names = opcode_names(&[0x61, 0xf5, 0xf4, 0xf5], CodeKind::Runtime);
        assert_eq!(names, vec!["PUSH2", "CREATE2"]);
        assert!(!names.iter().any(|name| name == "DELEGATECALL"));
    }

    #[test]
    fn creation_scan_stops_before_data_after_halt() {
        let names = opcode_names(&[0x00, 0xf5], CodeKind::Creation);
        assert_eq!(names, vec!["STOP"]);
    }

    #[test]
    fn creation_scan_follows_static_jump_target() {
        let names = opcode_names(
            &[0x60, 0x04, 0x56, 0xaa, 0x5b, 0xf5, 0x00],
            CodeKind::Creation,
        );
        assert!(names.iter().any(|name| name == "CREATE2"));
        assert!(!names.iter().any(|name| name == "UNKNOWN_0xAA"));
    }

    #[test]
    fn every_opcode_byte_has_a_stable_name() {
        let names = (0u8..=u8::MAX).map(opcode_name).collect::<HashSet<_>>();
        assert_eq!(names.len(), 256);
    }
}

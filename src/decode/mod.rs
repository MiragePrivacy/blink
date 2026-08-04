//! `blink decode` runner — bytecode static-analysis pipeline.
//!
//! Walks every contract in the configured data directory, runs
//! [`bytecode_meta::analyze`] on the runtime bytecode, and bulk-inserts
//! results into the append-only `bytecode_metadata_v2` DuckDB table.
//! Resumable: re-running is a no-op for files already marked complete
//! (unless `--overwrite`).
//!
pub mod bytecode_meta;
pub mod opcodes;

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use duckdb::{params, Connection};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;

use self::{
    bytecode_meta::{analyze, BytecodeMetadata},
    opcodes::{opcode_names, CodeKind, OPCODE_PARSER_VERSION},
};
use crate::{
    cli::DecodeArgs,
    util::{format_count, match_simple_glob, print_header, print_kv, print_kv_accent},
};

const MAX_DECODE_CODE_BYTES: u64 = 65_536;

#[derive(Debug)]
pub(crate) struct OpcodeSetRow {
    pub bytecode_hash: Vec<u8>,
    pub code_kind: CodeKind,
    pub opcodes: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CreationBytecodeLink {
    pub chain_id: u64,
    pub block_number: u32,
    pub create_index: u32,
    pub contract_address: Vec<u8>,
    pub bytecode_hash: Vec<u8>,
}

struct OpcodeContractInput {
    chain_id: u64,
    block_number: u32,
    create_index: u32,
    contract_address: Vec<u8>,
    init_code_hash: Option<Vec<u8>>,
    init_code: Option<Vec<u8>>,
    code_hash: Vec<u8>,
    code: Vec<u8>,
}

const SCHEMA: &str = r#"
-- Scalable address-level decode storage. The legacy `bytecode_metadata` table
-- had a BLOB primary key and became memory-heavy at tens of millions of rows.
-- v2 is append-only by source file, so large decodes avoid global conflict
-- checks.
CREATE TABLE IF NOT EXISTS bytecode_metadata_v2 (
    contract_address  BLOB NOT NULL,
    language          VARCHAR,
    compiler_version  VARCHAR,
    has_source_hash   BOOLEAN NOT NULL,
    is_erc20          BOOLEAN NOT NULL,
    is_erc721         BOOLEAN NOT NULL,
    is_erc1155        BOOLEAN NOT NULL,
    is_proxy_eip1967  BOOLEAN NOT NULL,
    is_proxy_minimal  BOOLEAN NOT NULL DEFAULT false,
    uses_push0        BOOLEAN NOT NULL,
    source_file       VARCHAR NOT NULL,
    decoded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Metadata keyed by runtime bytecode hash. This is the efficient path for
-- normalized sources like Zellic, where many deployment addresses share the
-- exact same runtime bytecode.
CREATE TABLE IF NOT EXISTS bytecode_metadata_by_hash (
    code_hash         BLOB NOT NULL,
    language          VARCHAR,
    compiler_version  VARCHAR,
    has_source_hash   BOOLEAN NOT NULL,
    is_erc20          BOOLEAN NOT NULL,
    is_erc721         BOOLEAN NOT NULL,
    is_erc1155        BOOLEAN NOT NULL,
    is_proxy_eip1967  BOOLEAN NOT NULL,
    is_proxy_minimal  BOOLEAN NOT NULL DEFAULT false,
    uses_push0        BOOLEAN NOT NULL,
    decoded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Backfill the new column for users upgrading from an older schema that
-- predates EIP-1167 minimal proxy detection. DuckDB cannot add constrained
-- columns to an existing table, so add it nullable first and fill old rows.
ALTER TABLE bytecode_metadata_v2     ADD COLUMN IF NOT EXISTS is_proxy_minimal BOOLEAN;
ALTER TABLE bytecode_metadata_by_hash ADD COLUMN IF NOT EXISTS is_proxy_minimal BOOLEAN;
UPDATE bytecode_metadata_v2 SET is_proxy_minimal = false WHERE is_proxy_minimal IS NULL;
UPDATE bytecode_metadata_by_hash SET is_proxy_minimal = false WHERE is_proxy_minimal IS NULL;

-- Per-file completion marker so re-runs skip already-processed parquet
-- files instead of scanning + analyzing them just to hit ON CONFLICT.
-- Keyed on (path, size, mtime) so file replacements are detected.
CREATE TABLE IF NOT EXISTS decode_runs (
    file_path        VARCHAR PRIMARY KEY,
    file_size        UBIGINT NOT NULL,
    file_mtime_secs  BIGINT  NOT NULL,
    rows_processed   UBIGINT NOT NULL,
    completed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Compact presence sets keyed by bytecode hash and code kind. Keeping one
-- VARCHAR[] per unique bytecode avoids multiplying every instruction across
-- every deployment while remaining directly queryable with list_contains().
CREATE TABLE IF NOT EXISTS bytecode_opcode_sets (
    bytecode_hash    BLOB NOT NULL,
    code_kind        VARCHAR NOT NULL,
    opcodes          VARCHAR[] NOT NULL,
    parser_version   USMALLINT NOT NULL,
    decoded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS bytecode_opcode_sets_lookup_idx
    ON bytecode_opcode_sets(code_kind, bytecode_hash, parser_version);

-- Runtime hashes already live on contract_deployments_native. Creation
-- hashes do not, so retain this narrow address -> init-code-hash mapping.
CREATE TABLE IF NOT EXISTS contract_creation_bytecodes (
    chain_id         UBIGINT NOT NULL,
    block_number     UINTEGER NOT NULL,
    create_index     UINTEGER NOT NULL,
    contract_address BLOB NOT NULL,
    bytecode_hash    BLOB NOT NULL,
    source_file      VARCHAR NOT NULL,
    linked_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Independent from decode_runs: databases decoded before opcode sets were
-- introduced must backfill once even though their metadata pass is complete.
CREATE TABLE IF NOT EXISTS opcode_decode_runs (
    file_path        VARCHAR NOT NULL,
    parser_version   USMALLINT NOT NULL,
    file_size        UBIGINT NOT NULL,
    file_mtime_secs  BIGINT NOT NULL,
    rows_processed   UBIGINT NOT NULL,
    completed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (file_path, parser_version)
);
"#;

pub async fn run_decode(args: DecodeArgs) -> Result<()> {
    let data_dir = args.data_dir.clone();
    tokio::task::spawn_blocking(move || run_decode_blocking(args, data_dir))
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
}

fn run_decode_blocking(args: DecodeArgs, data_dir: PathBuf) -> Result<()> {
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("create data dir {}", data_dir.display()))?;
    let db_path = data_dir.join("blink.duckdb");

    let mut write_conn = Connection::open(&db_path)
        .with_context(|| format!("open write connection {}", db_path.display()))?;

    let _ = write_conn.execute_batch(
        "PRAGMA threads=1; SET memory_limit='3GB'; SET preserve_insertion_order=false;",
    );

    write_conn
        .execute_batch(SCHEMA)
        .context("create bytecode metadata tables")?;
    if args.overwrite {
        write_conn
            .execute_batch(
                "DROP TABLE IF EXISTS bytecode_metadata; DELETE FROM bytecode_metadata_v2; DELETE FROM bytecode_metadata_by_hash; DELETE FROM decode_runs; DELETE FROM bytecode_opcode_sets; DELETE FROM contract_creation_bytecodes; DELETE FROM opcode_decode_runs;",
            )
            .context("clear bytecode_metadata for --overwrite")?;
    }

    // Pull every previously-completed file marker into memory so we can match
    // O(1) per file, no roundtrip per check.
    let completed: std::collections::HashMap<String, (u64, i64)> = {
        let mut stmt =
            write_conn.prepare("SELECT file_path, file_size, file_mtime_secs FROM decode_runs")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut map = std::collections::HashMap::new();
        for r in rows {
            let (path, size, mtime) = r?;
            map.insert(path, (size, mtime));
        }
        map
    };

    let files = list_parquet_files(&data_dir, &args.contracts_glob)?;
    let has_zellic_bytecodes = table_exists(&write_conn, "zellic_bytecodes")?;
    let zellic_total = if has_zellic_bytecodes {
        count_zellic_bytecodes(&write_conn, false)?
    } else {
        0
    };
    let zellic_pending = if has_zellic_bytecodes {
        count_zellic_bytecodes(&write_conn, true)?
    } else {
        0
    };

    if files.is_empty() && !has_zellic_bytecodes {
        println!(
            "no parquet files matched {} and no Zellic import exists in {}",
            args.contracts_glob,
            data_dir.display()
        );
        return Ok(());
    }

    print_header("blink decode");
    print_kv("source", &data_dir.display().to_string());
    print_kv_accent("files", &files.len().to_string());
    if has_zellic_bytecodes {
        print_kv_accent(
            "zellic bytecodes",
            &format!(
                "{} unique · {} pending",
                format_count(zellic_total),
                format_count(zellic_pending)
            ),
        );
    }
    print_kv(
        "writing to",
        &format!(
            "{} · tables `bytecode_metadata_v2`, `bytecode_metadata_by_hash`",
            db_path.display()
        ),
    );
    print_kv("batch", &args.batch_size.to_string());
    if args.overwrite {
        print_kv_accent("mode", "overwrite (re-decode all)");
    }
    println!();

    let started = Instant::now();
    let mut total_rows: u64 = 0;
    let mut total_decoded: u64 = 0;
    let mut total_skipped_files: u64 = 0;

    if has_zellic_bytecodes {
        let (scanned, decoded) =
            decode_zellic_bytecodes(&db_path, &mut write_conn, args.batch_size, zellic_pending)?;
        total_rows += scanned;
        total_decoded += decoded;
    }

    for (i, file) in files.iter().enumerate() {
        let file_start = Instant::now();
        let file_name = file
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_string();
        let prefix = format!("[{}/{}]", i + 1, files.len());

        // Skip files we've already fully processed (matched by path + size + mtime).
        let file_path_str = file.display().to_string();
        let (file_size, file_mtime_secs) = match std::fs::metadata(file) {
            Ok(m) => (
                m.len(),
                m.modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            ),
            Err(_) => (0, 0),
        };
        if !args.overwrite {
            if let Some(&(prev_size, prev_mtime)) = completed.get(&file_path_str) {
                if prev_size == file_size && prev_mtime == file_mtime_secs {
                    total_skipped_files += 1;
                    println!(
                        "  \x1b[38;2;112;112;112m{} {} · already decoded · skipped\x1b[0m",
                        prefix, file_name
                    );
                    continue;
                }
            }
        }

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template(
                    "  \x1b[38;2;189;255;0m{spinner}\x1b[0m \x1b[38;2;112;112;112m{prefix}\x1b[0m \x1b[38;2;237;237;237m{wide_msg}\x1b[0m",
                )
                .unwrap()
                .tick_chars("▘▖▝▗"),
        );
        pb.set_prefix(prefix.clone());
        pb.set_message(format!("{} · scanning…", file_name));
        pb.enable_steady_tick(Duration::from_millis(120));

        // If this file was partially decoded before an interruption, clear
        // its append-only rows before replaying it. Complete files are skipped
        // above via `decode_runs`.
        write_conn
            .execute(
                "DELETE FROM bytecode_metadata_v2 WHERE source_file = ?",
                params![file_path_str.as_str()],
            )
            .context("clear partial v2 decode rows for file")?;

        // Two-stage pipeline per file:
        //   1. Let DuckDB copy bounded hex rows to a temp TSV file
        //   2. Stream N (addr, code) pairs from that file
        //   3. Hand them to rayon for parallel analyze (CPU-bound, all cores)
        //   4. Bulk-insert the results in one transaction
        // This avoids returning parquet BLOB columns through duckdb-rs/Arrow,
        // which is the path that can expose bogus 144 GB lengths.
        let raw_batch = args.batch_size.max(5_000);
        let temp_path = temp_decode_path(file);
        let _ = std::fs::remove_file(&temp_path);
        copy_decode_rows_to_tsv(file, &temp_path)
            .with_context(|| format!("copy decode rows from {}", file.display()))?;
        let temp_file = File::open(&temp_path)
            .with_context(|| format!("open decode temp {}", temp_path.display()))?;
        let reader = BufReader::new(temp_file);
        let mut raw: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(raw_batch);
        let mut in_file: u64 = 0;
        let mut written: u64 = 0;
        let mut last_tick = Instant::now();

        for line in reader.lines() {
            let line = line.with_context(|| format!("read decode temp {}", temp_path.display()))?;
            let Some((addr_hex, code_hex)) = line.split_once('\t') else {
                continue;
            };
            if addr_hex.len() != 40
                || code_hex.is_empty()
                || code_hex.len() > (MAX_DECODE_CODE_BYTES as usize * 2)
                || code_hex.len() % 2 != 0
            {
                continue;
            }
            let Ok(addr) = hex::decode(addr_hex) else {
                continue;
            };
            let Ok(code) = hex::decode(code_hex) else {
                continue;
            };
            if addr.len() != 20 || code.is_empty() || code.len() as u64 > MAX_DECODE_CODE_BYTES {
                continue;
            }
            raw.push((addr, code));
            in_file += 1;

            if raw.len() >= raw_batch {
                let analyzed: Vec<(Vec<u8>, Vec<u8>, BytecodeMetadata)> = std::mem::take(&mut raw)
                    .into_par_iter()
                    .map(|(address, code)| {
                        let code_hash = alloy::primitives::keccak256(&code).to_vec();
                        (address, code_hash, analyze(&code))
                    })
                    .collect();
                written += flush_batch(&mut write_conn, &file_path_str, &analyzed)?;
                raw = Vec::with_capacity(raw_batch);
            }

            if last_tick.elapsed() >= Duration::from_millis(250) {
                let elapsed = file_start.elapsed().as_secs_f64().max(0.001);
                pb.set_message(format!(
                    "{} · {} rows · {} decoded · {:.0} rows/s",
                    file_name,
                    format_count(in_file),
                    format_count(written),
                    in_file as f64 / elapsed
                ));
                last_tick = Instant::now();
            }
        }
        let _ = std::fs::remove_file(&temp_path);
        if !raw.is_empty() {
            let analyzed: Vec<(Vec<u8>, Vec<u8>, BytecodeMetadata)> = std::mem::take(&mut raw)
                .into_par_iter()
                .map(|(address, code)| {
                    let code_hash = alloy::primitives::keccak256(&code).to_vec();
                    (address, code_hash, analyze(&code))
                })
                .collect();
            written += flush_batch(&mut write_conn, &file_path_str, &analyzed)?;
        }

        total_rows += in_file;
        total_decoded += written;
        pb.finish_with_message(format!(
            "\x1b[38;2;237;237;237m{}\x1b[0m → \x1b[38;2;189;255;0m{} rows · {} decoded\x1b[0m \x1b[38;2;112;112;112m{:.1}s\x1b[0m",
            file_name,
            format_count(in_file),
            format_count(written),
            file_start.elapsed().as_secs_f64()
        ));

        // Record completion so the next run can skip this file outright.
        // CHECKPOINT forces the WAL to the main file so the marker survives
        // a hard kill (OOM, ^C, kernel panic) — without it, an interrupted
        // run loses every marker since the last automatic checkpoint.
        // CURRENT_TIMESTAMP goes in the VALUES clause; DuckDB rejects it on
        // the right-hand side of `ON CONFLICT DO UPDATE SET` (treats it as a
        // column reference), so we route it through `excluded.completed_at`.
        write_conn
            .execute(
                r#"
                INSERT INTO decode_runs (file_path, file_size, file_mtime_secs, rows_processed, completed_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (file_path) DO UPDATE SET
                    file_size       = excluded.file_size,
                    file_mtime_secs = excluded.file_mtime_secs,
                    rows_processed  = excluded.rows_processed,
                    completed_at    = excluded.completed_at
                "#,
                params![file_path_str, file_size, file_mtime_secs, in_file],
            )
            .context("record decode_runs marker")?;
        let _ = write_conn.execute_batch("CHECKPOINT");
    }

    let (opcode_scanned, opcode_sets_written) = decode_opcode_datasets(
        &db_path,
        &mut write_conn,
        &files,
        args.batch_size,
        has_zellic_bytecodes,
        args.overwrite,
    )?;

    let elapsed = started.elapsed();
    let work_rows = total_rows.max(opcode_scanned);
    println!();
    if total_skipped_files > 0 {
        print_kv(
            "skipped",
            &format!("{} files (already decoded)", total_skipped_files),
        );
    }
    print_kv_accent("scanned", &format_count(total_rows));
    print_kv_accent("decoded", &format!("{} rows", format_count(total_decoded)));
    print_kv_accent(
        "opcodes",
        &format!(
            "{} contracts scanned · {} bytecode sets",
            format_count(opcode_scanned),
            format_count(opcode_sets_written)
        ),
    );
    print_kv_accent(
        "speed",
        &if work_rows > 0 {
            format!(
                "{:.0} rows/sec · total {:.1}s",
                work_rows as f64 / elapsed.as_secs_f64().max(0.001),
                elapsed.as_secs_f64()
            )
        } else {
            format!("nothing to do · total {:.1}s", elapsed.as_secs_f64())
        },
    );

    Ok(())
}

fn copy_decode_rows_to_tsv(parquet_file: &Path, temp_path: &Path) -> Result<()> {
    let parquet_conn =
        Connection::open_in_memory().context("open bundled DuckDB for decode row export")?;
    let parquet_path = parquet_file.display().to_string().replace('\'', "''");
    let temp_path_str = temp_path.display().to_string().replace('\'', "''");
    // Cryo's schema has a precomputed `n_code_bytes` (uint32). Filtering on
    // that BEFORE materializing `code` is what lets us skip rows with corrupt
    // BLOB length prefixes (the 144 GB OOM case). Paradigm's schema omits
    // that column, so we fall back to `octet_length(code)` for those files —
    // they're the official paradigm dataset and don't have the corruption
    // problem in practice.
    let has_n_code_bytes = parquet_has_column(&parquet_conn, parquet_file, "n_code_bytes")?;
    let length_filter = if has_n_code_bytes {
        format!(
            "AND n_code_bytes IS NOT NULL
             AND n_code_bytes BETWEEN 1 AND {max_code_bytes}
             AND octet_length(code) BETWEEN 1 AND {max_code_bytes}
             AND octet_length(code) = n_code_bytes",
            max_code_bytes = MAX_DECODE_CODE_BYTES,
        )
    } else {
        format!(
            "AND octet_length(code) BETWEEN 1 AND {max_code_bytes}",
            max_code_bytes = MAX_DECODE_CODE_BYTES,
        )
    };
    let sql = format!(
        "COPY (
            SELECT hex(contract_address), hex(code)
            FROM read_parquet('{parquet_path}')
            WHERE contract_address IS NOT NULL
              AND octet_length(contract_address) = 20
              AND code IS NOT NULL
              {length_filter}
        ) TO '{temp_path_str}' (FORMAT CSV, HEADER false, DELIMITER '\\t');",
    );
    parquet_conn
        .execute_batch(&sql)
        .with_context(|| format!("export decode rows from {}", parquet_file.display()))?;
    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            params![table],
            |row| row.get(0),
        )
        .unwrap_or(0);
    Ok(count > 0)
}

fn count_zellic_bytecodes(conn: &Connection, pending_only: bool) -> Result<u64> {
    let sql = if pending_only {
        format!(
            r#"
            SELECT COUNT(*)
            FROM zellic_bytecodes b
            WHERE NOT EXISTS (
                SELECT 1
                FROM bytecode_metadata_by_hash m
                WHERE m.code_hash = b.code_hash
            )
              AND b.n_code_bytes BETWEEN 1 AND {max_code_bytes}
            "#,
            max_code_bytes = MAX_DECODE_CODE_BYTES
        )
    } else {
        format!(
            r#"
            SELECT COUNT(*)
            FROM zellic_bytecodes
            WHERE n_code_bytes BETWEEN 1 AND {max_code_bytes}
            "#,
            max_code_bytes = MAX_DECODE_CODE_BYTES
        )
    };
    let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
    Ok(count.max(0) as u64)
}

fn decode_zellic_bytecodes(
    db_path: &Path,
    write_conn: &mut Connection,
    batch_size: usize,
    pending: u64,
) -> Result<(u64, u64)> {
    if pending == 0 {
        println!("  \x1b[38;2;112;112;112mzellic bytecodes · already decoded · skipped\x1b[0m");
        return Ok((0, 0));
    }

    let started = Instant::now();
    let pb = ProgressBar::new(pending);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "  \x1b[38;2;189;255;0m▸\x1b[0m [{elapsed_precise}] \x1b[38;2;189;255;0m{bar:40}\x1b[38;2;64;64;64m{bar:0}\x1b[0m {pos}/{len} bytecodes · {percent}% · eta {eta} · {msg}",
            )
            .unwrap()
            .progress_chars("█░ "),
    );
    pb.enable_steady_tick(Duration::from_millis(200));
    pb.set_message("exporting unique Zellic bytecodes");

    let temp_path = temp_zellic_decode_path(db_path);
    if let Some(parent) = temp_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&temp_path);
    copy_zellic_decode_rows_to_tsv(write_conn, &temp_path)
        .with_context(|| format!("copy Zellic decode rows to {}", temp_path.display()))?;
    pb.set_message("decoding unique Zellic bytecodes");

    let raw_batch = batch_size.max(5_000);
    let temp_file =
        File::open(&temp_path).with_context(|| format!("open {}", temp_path.display()))?;
    let reader = BufReader::new(temp_file);
    let mut raw: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(raw_batch);
    let mut scanned = 0u64;
    let mut written = 0u64;
    let mut last_tick = Instant::now();

    for line in reader.lines() {
        let line = line.with_context(|| format!("read {}", temp_path.display()))?;
        let Some((hash_hex, code_hex)) = line.split_once('\t') else {
            continue;
        };
        if hash_hex.len() != 64
            || code_hex.is_empty()
            || code_hex.len() > (MAX_DECODE_CODE_BYTES as usize * 2)
            || code_hex.len() % 2 != 0
        {
            continue;
        }
        let Ok(code_hash) = hex::decode(hash_hex) else {
            continue;
        };
        let Ok(code) = hex::decode(code_hex) else {
            continue;
        };
        if code_hash.len() != 32 || code.is_empty() || code.len() as u64 > MAX_DECODE_CODE_BYTES {
            continue;
        }
        raw.push((code_hash, code));
        scanned += 1;

        if raw.len() >= raw_batch {
            let analyzed: Vec<(Vec<u8>, BytecodeMetadata)> = std::mem::take(&mut raw)
                .into_par_iter()
                .map(|(h, c)| (h, analyze(&c)))
                .collect();
            written += flush_hash_batch(write_conn, &analyzed)?;
            pb.inc(analyzed.len() as u64);
            raw = Vec::with_capacity(raw_batch);
        }

        if last_tick.elapsed() >= Duration::from_millis(250) {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            pb.set_message(format!(
                "{} decoded · {:.0} bytecodes/s",
                format_count(written),
                scanned as f64 / elapsed
            ));
            last_tick = Instant::now();
        }
    }
    let _ = std::fs::remove_file(&temp_path);

    if !raw.is_empty() {
        let analyzed: Vec<(Vec<u8>, BytecodeMetadata)> = std::mem::take(&mut raw)
            .into_par_iter()
            .map(|(h, c)| (h, analyze(&c)))
            .collect();
        written += flush_hash_batch(write_conn, &analyzed)?;
        pb.inc(analyzed.len() as u64);
    }

    let _ = write_conn.execute_batch("CHECKPOINT");
    pb.finish_with_message(format!(
        "\x1b[38;2;237;237;237mzellic bytecodes\x1b[0m → \x1b[38;2;189;255;0m{} unique · {} decoded\x1b[0m \x1b[38;2;112;112;112m{:.1}s\x1b[0m",
        format_count(scanned),
        format_count(written),
        started.elapsed().as_secs_f64()
    ));
    Ok((scanned, written))
}

fn copy_zellic_decode_rows_to_tsv(conn: &Connection, temp_path: &Path) -> Result<()> {
    let temp_path_str = temp_path.display().to_string().replace('\'', "''");
    let sql = format!(
        r#"
        COPY (
            SELECT hex(b.code_hash), hex(b.code)
            FROM zellic_bytecodes b
            WHERE NOT EXISTS (
                SELECT 1
                FROM bytecode_metadata_by_hash m
                WHERE m.code_hash = b.code_hash
            )
              AND b.n_code_bytes BETWEEN 1 AND {max_code_bytes}
              AND octet_length(b.code) = b.n_code_bytes
        ) TO '{temp_path_str}' (FORMAT CSV, HEADER false, DELIMITER '\t');
        "#,
        max_code_bytes = MAX_DECODE_CODE_BYTES
    );
    conn.execute_batch(&sql)
        .context("export Zellic decode rows")
}

fn decode_opcode_datasets(
    db_path: &Path,
    conn: &mut Connection,
    files: &[PathBuf],
    batch_size: usize,
    has_zellic_bytecodes: bool,
    overwrite: bool,
) -> Result<(u64, u64)> {
    println!();
    print_kv_accent(
        "opcode parser",
        &format!("eot · generation {}", OPCODE_PARSER_VERSION),
    );

    let mut scanned = 0u64;
    let mut written = 0u64;
    if has_zellic_bytecodes {
        let (source_scanned, source_written) =
            decode_zellic_opcode_sets(db_path, conn, batch_size)?;
        scanned += source_scanned;
        written += source_written;
    }

    let completed: std::collections::HashMap<String, (u64, i64)> = {
        let mut stmt = conn.prepare(
            "SELECT file_path, file_size, file_mtime_secs
             FROM opcode_decode_runs WHERE parser_version = ?",
        )?;
        let rows = stmt.query_map(params![OPCODE_PARSER_VERSION], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, u64>(1)?, row.get::<_, i64>(2)?),
            ))
        })?;
        rows.collect::<std::result::Result<_, _>>()?
    };

    for file in files {
        let source_file = file.display().to_string();
        let (file_size, file_mtime_secs) = match std::fs::metadata(file) {
            Ok(metadata) => (
                metadata.len(),
                metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|duration| duration.as_secs() as i64)
                    .unwrap_or(0),
            ),
            Err(_) => (0, 0),
        };
        if !overwrite
            && completed
                .get(&source_file)
                .is_some_and(|&(size, mtime)| size == file_size && mtime == file_mtime_secs)
        {
            continue;
        }

        // A failed earlier attempt may have appended only part of this
        // source's creation links. Opcode sets are hash-keyed and idempotent.
        conn.execute(
            "DELETE FROM contract_creation_bytecodes WHERE source_file = ?",
            params![source_file.as_str()],
        )
        .context("clear partial creation bytecode links")?;

        let (file_scanned, file_written) = decode_parquet_opcode_sets(conn, file, batch_size)?;
        scanned += file_scanned;
        written += file_written;

        conn.execute(
            r#"
            INSERT INTO opcode_decode_runs (
                file_path, parser_version, file_size, file_mtime_secs,
                rows_processed, completed_at
            ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (file_path, parser_version) DO UPDATE SET
                file_size       = excluded.file_size,
                file_mtime_secs = excluded.file_mtime_secs,
                rows_processed  = excluded.rows_processed,
                completed_at    = excluded.completed_at
            "#,
            params![
                source_file,
                OPCODE_PARSER_VERSION,
                file_size,
                file_mtime_secs,
                file_scanned
            ],
        )
        .context("record opcode_decode_runs marker")?;
        let _ = conn.execute_batch("CHECKPOINT");
    }

    Ok((scanned, written))
}

fn decode_zellic_opcode_sets(
    db_path: &Path,
    conn: &mut Connection,
    batch_size: usize,
) -> Result<(u64, u64)> {
    let pending: u64 = conn
        .query_row(
            &format!(
                r#"
                SELECT COUNT(*)::UBIGINT
                FROM zellic_bytecodes b
                WHERE b.n_code_bytes BETWEEN 1 AND {MAX_DECODE_CODE_BYTES}
                  AND NOT EXISTS (
                      SELECT 1 FROM bytecode_opcode_sets o
                      WHERE o.bytecode_hash = b.code_hash
                        AND o.code_kind = 'runtime'
                        AND o.parser_version = {OPCODE_PARSER_VERSION}
                  )
                "#
            ),
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    if pending == 0 {
        return Ok((0, 0));
    }

    let temp_path = temp_zellic_opcode_path(db_path);
    if let Some(parent) = temp_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let _ = std::fs::remove_file(&temp_path);
    let temp_path_sql = temp_path.display().to_string().replace('\'', "''");
    conn.execute_batch(&format!(
        r#"
        COPY (
            SELECT hex(b.code_hash), hex(b.code)
            FROM zellic_bytecodes b
            WHERE b.n_code_bytes BETWEEN 1 AND {MAX_DECODE_CODE_BYTES}
              AND octet_length(b.code) = b.n_code_bytes
              AND NOT EXISTS (
                  SELECT 1 FROM bytecode_opcode_sets o
                  WHERE o.bytecode_hash = b.code_hash
                    AND o.code_kind = 'runtime'
                    AND o.parser_version = {OPCODE_PARSER_VERSION}
              )
        ) TO '{temp_path_sql}' (FORMAT CSV, HEADER false, DELIMITER '\t');
        "#
    ))
    .context("export Zellic opcode rows")?;

    let file = File::open(&temp_path)
        .with_context(|| format!("open Zellic opcode export {}", temp_path.display()))?;
    let mut raw = Vec::with_capacity(batch_size.max(5_000));
    let mut scanned = 0u64;
    let mut written = 0u64;
    for line in BufReader::new(file).lines() {
        let line = line?;
        let Some((hash_hex, code_hex)) = line.split_once('\t') else {
            continue;
        };
        let (Ok(bytecode_hash), Ok(code)) = (hex::decode(hash_hex), hex::decode(code_hex)) else {
            continue;
        };
        if bytecode_hash.len() != 32 || code.is_empty() || code.len() as u64 > MAX_DECODE_CODE_BYTES
        {
            continue;
        }
        raw.push((bytecode_hash, code));
        scanned += 1;
        if raw.len() >= batch_size.max(5_000) {
            written += analyze_and_flush_runtime_opcode_batch(conn, std::mem::take(&mut raw))?;
            raw = Vec::with_capacity(batch_size.max(5_000));
        }
    }
    if !raw.is_empty() {
        written += analyze_and_flush_runtime_opcode_batch(conn, raw)?;
    }
    let _ = std::fs::remove_file(&temp_path);
    Ok((scanned, written))
}

fn analyze_and_flush_runtime_opcode_batch(
    conn: &mut Connection,
    raw: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<u64> {
    let rows = raw
        .into_par_iter()
        .map(|(bytecode_hash, code)| OpcodeSetRow {
            bytecode_hash,
            code_kind: CodeKind::Runtime,
            opcodes: opcode_names(&code, CodeKind::Runtime),
        })
        .collect::<Vec<_>>();
    flush_opcode_sets(conn, &rows)
}

fn decode_parquet_opcode_sets(
    conn: &mut Connection,
    parquet_file: &Path,
    batch_size: usize,
) -> Result<(u64, u64)> {
    let temp_path = temp_opcode_decode_path(parquet_file);
    let _ = std::fs::remove_file(&temp_path);
    copy_opcode_rows_to_tsv(parquet_file, &temp_path)?;

    let file = File::open(&temp_path)
        .with_context(|| format!("open opcode export {}", temp_path.display()))?;
    let mut raw = Vec::with_capacity(batch_size.max(5_000));
    let mut scanned = 0u64;
    let mut written = 0u64;
    let source_file = parquet_file.display().to_string();

    for line in BufReader::new(file).lines() {
        let line = line?;
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 8 {
            continue;
        }
        let (Ok(chain_id), Ok(block_number), Ok(create_index)) = (
            fields[0].parse::<u64>(),
            fields[1].parse::<u32>(),
            fields[2].parse::<u32>(),
        ) else {
            continue;
        };
        let Ok(contract_address) = hex::decode(fields[3]) else {
            continue;
        };
        let Ok(code) = hex::decode(fields[7]) else {
            continue;
        };
        if contract_address.len() != 20
            || code.is_empty()
            || code.len() as u64 > MAX_DECODE_CODE_BYTES
        {
            continue;
        }
        let code_hash = hex::decode(fields[6])
            .ok()
            .filter(|hash| hash.len() == 32)
            .unwrap_or_else(|| alloy::primitives::keccak256(&code).to_vec());
        let init_code = hex::decode(fields[5])
            .ok()
            .filter(|bytes| !bytes.is_empty() && bytes.len() as u64 <= MAX_DECODE_CODE_BYTES);
        let init_code_hash = init_code.as_ref().map(|init_code| {
            hex::decode(fields[4])
                .ok()
                .filter(|hash| hash.len() == 32)
                .unwrap_or_else(|| alloy::primitives::keccak256(init_code).to_vec())
        });

        raw.push(OpcodeContractInput {
            chain_id,
            block_number,
            create_index,
            contract_address,
            init_code_hash,
            init_code,
            code_hash,
            code,
        });
        scanned += 1;

        if raw.len() >= batch_size.max(5_000) {
            written += flush_opcode_contract_batch(conn, &source_file, std::mem::take(&mut raw))?;
            raw = Vec::with_capacity(batch_size.max(5_000));
        }
    }
    if !raw.is_empty() {
        written += flush_opcode_contract_batch(conn, &source_file, raw)?;
    }
    let _ = std::fs::remove_file(&temp_path);
    Ok((scanned, written))
}

fn flush_opcode_contract_batch(
    conn: &mut Connection,
    source_file: &str,
    raw: Vec<OpcodeContractInput>,
) -> Result<u64> {
    let analyzed = raw
        .into_par_iter()
        .map(|input| {
            let runtime = OpcodeSetRow {
                bytecode_hash: input.code_hash,
                code_kind: CodeKind::Runtime,
                opcodes: opcode_names(&input.code, CodeKind::Runtime),
            };
            let creation =
                input
                    .init_code
                    .zip(input.init_code_hash)
                    .map(|(init_code, init_code_hash)| {
                        (
                            OpcodeSetRow {
                                bytecode_hash: init_code_hash.clone(),
                                code_kind: CodeKind::Creation,
                                opcodes: opcode_names(&init_code, CodeKind::Creation),
                            },
                            CreationBytecodeLink {
                                chain_id: input.chain_id,
                                block_number: input.block_number,
                                create_index: input.create_index,
                                contract_address: input.contract_address,
                                bytecode_hash: init_code_hash,
                            },
                        )
                    });
            (runtime, creation)
        })
        .collect::<Vec<_>>();

    let mut seen = std::collections::HashSet::with_capacity(analyzed.len() * 2);
    let mut sets = Vec::with_capacity(analyzed.len() * 2);
    let mut links = Vec::with_capacity(analyzed.len());
    for (runtime, creation) in analyzed {
        if seen.insert((runtime.code_kind, runtime.bytecode_hash.clone())) {
            sets.push(runtime);
        }
        if let Some((creation, link)) = creation {
            if seen.insert((creation.code_kind, creation.bytecode_hash.clone())) {
                sets.push(creation);
            }
            links.push(link);
        }
    }

    let inserted = flush_opcode_sets(conn, &sets)?;
    flush_creation_links(conn, source_file, &links)?;
    Ok(inserted)
}

fn copy_opcode_rows_to_tsv(parquet_file: &Path, temp_path: &Path) -> Result<()> {
    let parquet_conn =
        Connection::open_in_memory().context("open bundled DuckDB for opcode row export")?;
    let columns = parquet_columns(&parquet_conn, parquet_file)?;
    if !columns.contains("contract_address") || !columns.contains("code") {
        return Err(anyhow!("parquet lacks contract_address/code columns"));
    }

    let expr = |column: &str, fallback: &str| {
        if columns.contains(column) {
            column.to_string()
        } else {
            fallback.to_string()
        }
    };
    let chain_id = expr("chain_id", "1");
    let block_number = expr("block_number", "0");
    let create_index = expr("create_index", "0");
    let init_code_hash = expr("init_code_hash", "NULL::BLOB");
    let code_hash = expr("code_hash", "NULL::BLOB");
    let init_code = if columns.contains("init_code") {
        format!(
            "CASE WHEN octet_length(init_code) BETWEEN 1 AND {MAX_DECODE_CODE_BYTES} \
             THEN init_code ELSE NULL::BLOB END"
        )
    } else {
        "NULL::BLOB".to_string()
    };
    let runtime_filter = if columns.contains("n_code_bytes") {
        format!(
            "n_code_bytes BETWEEN 1 AND {MAX_DECODE_CODE_BYTES} AND \
             octet_length(code) = n_code_bytes"
        )
    } else {
        format!("octet_length(code) BETWEEN 1 AND {MAX_DECODE_CODE_BYTES}")
    };

    let parquet_path = parquet_file.display().to_string().replace('\'', "''");
    let temp_path = temp_path.display().to_string().replace('\'', "''");
    let sql = format!(
        r#"
        COPY (
            SELECT
                COALESCE({chain_id}, 1)::VARCHAR,
                COALESCE({block_number}, 0)::VARCHAR,
                COALESCE({create_index}, 0)::VARCHAR,
                hex(contract_address),
                COALESCE(hex({init_code_hash}), ''),
                COALESCE(hex({init_code}), ''),
                COALESCE(hex({code_hash}), ''),
                hex(code)
            FROM read_parquet('{parquet_path}')
            WHERE contract_address IS NOT NULL
              AND octet_length(contract_address) = 20
              AND code IS NOT NULL
              AND {runtime_filter}
        ) TO '{temp_path}' (FORMAT CSV, HEADER false, DELIMITER '\t');
        "#
    );
    parquet_conn
        .execute_batch(&sql)
        .with_context(|| format!("export opcode rows from {}", parquet_file.display()))?;
    Ok(())
}

fn parquet_columns(
    conn: &Connection,
    parquet_file: &Path,
) -> Result<std::collections::HashSet<String>> {
    let parquet_path = parquet_file.display().to_string().replace('\'', "''");
    let sql =
        format!("SELECT string_agg(DISTINCT name, ',') FROM parquet_schema('{parquet_path}');");
    let names: Option<String> = conn
        .query_row(&sql, [], |row| row.get(0))
        .with_context(|| format!("read parquet schema for {}", parquet_file.display()))?;
    Ok(names
        .unwrap_or_default()
        .trim()
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

pub(crate) fn flush_opcode_sets(conn: &mut Connection, rows: &[OpcodeSetRow]) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE IF NOT EXISTS _opcode_set_stage (
            bytecode_hash BLOB,
            code_kind VARCHAR,
            opcodes_json VARCHAR
        );
        DELETE FROM _opcode_set_stage;
        "#,
    )?;
    {
        let mut appender = conn.appender("_opcode_set_stage")?;
        for row in rows {
            let opcodes_json = serde_json::to_string(&row.opcodes)?;
            appender.append_row(params![
                &row.bytecode_hash,
                row.code_kind.as_str(),
                opcodes_json
            ])?;
        }
        appender.flush()?;
    }

    let inserted = conn.execute(
        &format!(
            r#"
            INSERT INTO bytecode_opcode_sets (
                bytecode_hash, code_kind, opcodes, parser_version, decoded_at
            )
            SELECT
                s.bytecode_hash,
                s.code_kind,
                from_json(s.opcodes_json, '["VARCHAR"]')::VARCHAR[],
                {OPCODE_PARSER_VERSION},
                CURRENT_TIMESTAMP
            FROM _opcode_set_stage s
            WHERE NOT EXISTS (
                SELECT 1 FROM bytecode_opcode_sets existing
                WHERE existing.bytecode_hash = s.bytecode_hash
                  AND existing.code_kind = s.code_kind
                  AND existing.parser_version = {OPCODE_PARSER_VERSION}
            )
            "#
        ),
        [],
    )?;
    Ok(inserted as u64)
}

pub(crate) fn flush_creation_links(
    conn: &mut Connection,
    source_file: &str,
    links: &[CreationBytecodeLink],
) -> Result<u64> {
    if links.is_empty() {
        return Ok(0);
    }
    conn.execute_batch(
        r#"
        CREATE TEMP TABLE IF NOT EXISTS _creation_link_stage (
            chain_id UBIGINT,
            block_number UINTEGER,
            create_index UINTEGER,
            contract_address BLOB,
            bytecode_hash BLOB,
            source_file VARCHAR
        );
        DELETE FROM _creation_link_stage;
        "#,
    )?;
    {
        let mut appender = conn.appender("_creation_link_stage")?;
        for link in links {
            appender.append_row(params![
                link.chain_id,
                link.block_number,
                link.create_index,
                &link.contract_address,
                &link.bytecode_hash,
                source_file
            ])?;
        }
        appender.flush()?;
    }
    let inserted = conn.execute(
        r#"
        INSERT INTO contract_creation_bytecodes (
            chain_id, block_number, create_index, contract_address,
            bytecode_hash, source_file
        )
        SELECT
            chain_id, block_number, create_index, contract_address,
            bytecode_hash, source_file
        FROM _creation_link_stage
        "#,
        [],
    )?;
    Ok(inserted as u64)
}

fn parquet_has_column(conn: &Connection, parquet_file: &Path, column: &str) -> Result<bool> {
    let parquet_path = parquet_file.display().to_string().replace('\'', "''");
    let sql =
        format!("SELECT COUNT(*) FROM parquet_schema('{parquet_path}') WHERE name = '{column}';");
    let count: i64 = conn
        .query_row(&sql, [], |row| row.get(0))
        .with_context(|| format!("read parquet schema for {}", parquet_file.display()))?;
    Ok(count > 0)
}

fn temp_decode_path(file: &Path) -> PathBuf {
    let file_name = file
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("contracts");
    let safe_name: String = file_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    std::env::temp_dir().join(format!(
        "blink_decode_{}_{}.tsv",
        std::process::id(),
        safe_name
    ))
}

fn temp_zellic_decode_path(db_path: &Path) -> PathBuf {
    let dir = db_path
        .parent()
        .map(|p| p.join(".blink").join("tmp"))
        .unwrap_or_else(std::env::temp_dir);
    dir.join(format!("blink_zellic_decode_{}.tsv", std::process::id()))
}

fn temp_zellic_opcode_path(db_path: &Path) -> PathBuf {
    let dir = db_path
        .parent()
        .map(|path| path.join(".blink").join("tmp"))
        .unwrap_or_else(std::env::temp_dir);
    dir.join(format!("blink_zellic_opcodes_{}.tsv", std::process::id()))
}

fn temp_opcode_decode_path(file: &Path) -> PathBuf {
    let file_name = file
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("contracts");
    let safe_name = file_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    std::env::temp_dir().join(format!(
        "blink_opcode_decode_{}_{}.tsv",
        std::process::id(),
        safe_name
    ))
}

fn flush_batch(
    conn: &mut Connection,
    source_file: &str,
    buffer: &[(Vec<u8>, Vec<u8>, BytecodeMetadata)],
) -> Result<u64> {
    if buffer.is_empty() {
        return Ok(0);
    }

    // Append-only write path. No primary key and no ON CONFLICT means DuckDB
    // does not need to keep a global address index pinned while decoding
    // multi-million-row files. We stage first so `decoded_at` can keep its
    // default timestamp in the destination table.
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS _bm_stage_v2 (
            contract_address  BLOB,
            language          VARCHAR,
            compiler_version  VARCHAR,
            has_source_hash   BOOLEAN,
            is_erc20          BOOLEAN,
            is_erc721         BOOLEAN,
            is_erc1155        BOOLEAN,
            is_proxy_eip1967  BOOLEAN,
            is_proxy_minimal  BOOLEAN,
            uses_push0        BOOLEAN,
            source_file       VARCHAR
        );
        DELETE FROM _bm_stage_v2;",
    )
    .context("prepare v2 staging table")?;

    {
        let mut app = conn.appender("_bm_stage_v2").context("open v2 appender")?;
        for (addr, _, meta) in buffer {
            app.append_row(params![
                addr,
                meta.language.map(|l| l.as_str()),
                meta.compiler_version.as_deref(),
                meta.has_source_hash,
                meta.is_erc20,
                meta.is_erc721,
                meta.is_erc1155,
                meta.is_proxy_eip1967,
                meta.is_proxy_minimal,
                meta.uses_push0,
                source_file,
            ])
            .context("append metadata v2 row")?;
        }
        app.flush().context("flush metadata v2 appender")?;
    }

    conn.execute_batch(
        r#"
        INSERT INTO bytecode_metadata_v2 (
            contract_address, language, compiler_version, has_source_hash,
            is_erc20, is_erc721, is_erc1155, is_proxy_eip1967,
            is_proxy_minimal, uses_push0, source_file
        )
        SELECT
            contract_address, language, compiler_version, has_source_hash,
            is_erc20, is_erc721, is_erc1155, is_proxy_eip1967,
            is_proxy_minimal, uses_push0, source_file
        FROM _bm_stage_v2;
        "#,
    )
    .context("insert staged metadata v2 rows")?;

    let mut seen_hashes = std::collections::HashSet::with_capacity(buffer.len());
    let hash_metadata = buffer
        .iter()
        .filter(|(_, code_hash, _)| seen_hashes.insert(code_hash.as_slice()))
        .map(|(_, code_hash, metadata)| (code_hash.clone(), metadata.clone()))
        .collect::<Vec<_>>();
    flush_hash_batch(conn, &hash_metadata).context("insert hash metadata for parquet decode")?;

    Ok(buffer.len() as u64)
}

pub(crate) fn flush_hash_batch(
    conn: &mut Connection,
    buffer: &[(Vec<u8>, BytecodeMetadata)],
) -> Result<u64> {
    if buffer.is_empty() {
        return Ok(0);
    }

    let tx = conn
        .transaction()
        .context("open hash metadata transaction")?;
    let inserted = {
        let mut inserted = 0u64;
        let mut stmt = tx
            .prepare(
                r#"
                INSERT INTO bytecode_metadata_by_hash (
                    code_hash, language, compiler_version, has_source_hash,
                    is_erc20, is_erc721, is_erc1155, is_proxy_eip1967,
                    is_proxy_minimal, uses_push0, decoded_at
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
                WHERE NOT EXISTS (
                    SELECT 1 FROM bytecode_metadata_by_hash WHERE code_hash = ?
                )
                "#,
            )
            .context("prepare hash metadata insert")?;
        for (code_hash, meta) in buffer {
            inserted += stmt
                .execute(params![
                    code_hash,
                    meta.language.map(|l| l.as_str()),
                    meta.compiler_version.as_deref(),
                    meta.has_source_hash,
                    meta.is_erc20,
                    meta.is_erc721,
                    meta.is_erc1155,
                    meta.is_proxy_eip1967,
                    meta.is_proxy_minimal,
                    meta.uses_push0,
                    code_hash,
                ])
                .context("insert hash metadata row")? as u64;
        }
        inserted
    };
    tx.commit().context("commit hash metadata batch")?;

    Ok(inserted)
}

fn list_parquet_files(data_dir: &std::path::Path, pattern: &str) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(data_dir)
        .with_context(|| format!("read data dir {}", data_dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().and_then(|s| s.to_str()) == Some("parquet")
                && match_simple_glob(
                    pattern,
                    p.file_name().and_then(|s| s.to_str()).unwrap_or_default(),
                )
                && p.file_name()
                    .and_then(|s| s.to_str())
                    .map(|n| n != "enrichment.parquet")
                    .unwrap_or(true)
        })
        .collect();
    out.sort();
    Ok(out)
}

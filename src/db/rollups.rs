//! Native rollup tables that make dashboard queries parquet-free.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use duckdb::{params, Connection};

use crate::{chains::ETHEREUM_CHAIN_ID, util::match_simple_glob};

use super::{column_exists, table_exists};

const ZELLIC_SOURCE_KEY: &str = "zellic://contracts";

/// Block-bucket width of `rollup_code_block_counts`. Must match between
/// ingest and the ranged queries that read it.
pub(crate) const CODE_ROLLUP_BUCKET_BLOCKS: u64 = 10_000;

pub(crate) fn list_contract_parquet_files(
    data_dir: &Path,
    contracts_glob: &str,
) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(data_dir)
        .with_context(|| format!("read data dir {}", data_dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().and_then(|s| s.to_str()) == Some("parquet")
                && match_simple_glob(
                    contracts_glob,
                    p.file_name().and_then(|s| s.to_str()).unwrap_or_default(),
                )
                && p.file_name()
                    .and_then(|s| s.to_str())
                    .map(|n| n != "enrichment.parquet")
                    .unwrap_or(true)
        })
        .collect();
    files.sort();
    Ok(files)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ContractFileRange {
    pub chain_id: Option<u64>,
    pub end_block: Option<u64>,
}

/// Chain id and block range hints encoded in a contract parquet file name
/// (`contracts__chain_X__START__END.parquet`, `tail__chain_X__...`, or the
/// legacy chain-less `contracts__START__END.parquet`).
pub(crate) fn contract_file_range(path: &Path) -> ContractFileRange {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    let nums = decimal_runs(name);
    let chain_id = if name.starts_with("contracts__chain_") || name.starts_with("tail__chain_") {
        nums.first().copied()
    } else {
        None
    };
    let end_block = if nums.len() >= 2 {
        nums.last().copied()
    } else {
        None
    };
    ContractFileRange {
        chain_id,
        end_block,
    }
}

fn decimal_runs(input: &str) -> Vec<u64> {
    let mut out = Vec::new();
    let mut current: Option<u64> = None;
    for byte in input.bytes() {
        if byte.is_ascii_digit() {
            let digit = u64::from(byte - b'0');
            current = Some(
                current
                    .unwrap_or(0)
                    .saturating_mul(10)
                    .saturating_add(digit),
            );
        } else if let Some(value) = current.take() {
            out.push(value);
        }
    }
    if let Some(value) = current {
        out.push(value);
    }
    out
}

pub(crate) fn max_contract_file_block_for_chain(files: &[PathBuf], chain_id: u64) -> Option<u64> {
    files
        .iter()
        .map(|path| contract_file_range(path))
        .filter(|file| {
            file.chain_id == Some(chain_id)
                || (file.chain_id.is_none() && chain_id == ETHEREUM_CHAIN_ID)
        })
        .filter_map(|file| file.end_block)
        .max()
}

pub(crate) fn ensure_rollup_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS contract_deployments_native (
            chain_id UBIGINT NOT NULL,
            block_number UINTEGER NOT NULL,
            create_index UINTEGER NOT NULL,
            contract_address BLOB NOT NULL,
            deployer BLOB,
            code_hash BLOB,
            n_code_bytes UINTEGER,
            source_path VARCHAR
        );
        CREATE TABLE IF NOT EXISTS rollup_block_counts (
            chain_id UBIGINT NOT NULL,
            block_number UINTEGER NOT NULL,
            contract_count UBIGINT NOT NULL,
            PRIMARY KEY (chain_id, block_number)
        );
        CREATE TABLE IF NOT EXISTS rollup_code_counts (
            chain_id UBIGINT NOT NULL,
            code_hash BLOB NOT NULL,
            n_code_bytes UINTEGER,
            contract_count UBIGINT NOT NULL,
            PRIMARY KEY (chain_id, code_hash)
        );
        CREATE TABLE IF NOT EXISTS rollup_code_block_counts (
            chain_id UBIGINT NOT NULL,
            block_bucket UBIGINT NOT NULL,
            code_hash BLOB NOT NULL,
            n_code_bytes UINTEGER,
            contract_count UBIGINT NOT NULL,
            PRIMARY KEY (chain_id, block_bucket, code_hash)
        );
        CREATE TABLE IF NOT EXISTS rollup_sources (
            source_path VARCHAR PRIMARY KEY,
            start_block UBIGINT,
            end_block UBIGINT,
            row_count UBIGINT NOT NULL,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        -- Superseded: per-source counts double-counted overlapping files.
        DROP TABLE IF EXISTS parquet_block_counts;
        "#,
    )
    .context("create rollup schema")
}

pub(crate) fn rollups_ready(conn: &Connection) -> Result<bool> {
    Ok(table_exists(conn, "contract_deployments_native")?
        && table_exists(conn, "rollup_block_counts")?
        && table_exists(conn, "rollup_code_counts")?
        && table_exists(conn, "rollup_code_block_counts")?
        && table_exists(conn, "rollup_sources")?)
}

/// One-time migration: databases built before the bucketed code rollup
/// existed have deployments but an empty `rollup_code_block_counts`.
fn backfill_code_block_rollup(conn: &Connection) -> Result<()> {
    let (bucket_rows, deployment_rows): (i64, i64) = conn.query_row(
        r#"
        SELECT
            (SELECT COUNT(*) FROM rollup_code_block_counts),
            (SELECT COUNT(*) FROM contract_deployments_native)
        "#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if bucket_rows > 0 || deployment_rows == 0 {
        return Ok(());
    }
    tracing::info!("backfilling bucketed code rollup from native deployments (one-time)");
    conn.execute_batch(&format!(
        r#"
        INSERT INTO rollup_code_block_counts
        SELECT
            chain_id,
            (block_number // {CODE_ROLLUP_BUCKET_BLOCKS})::UBIGINT,
            code_hash,
            any_value(n_code_bytes)::UINTEGER,
            COUNT(*)::UBIGINT
        FROM contract_deployments_native
        WHERE code_hash IS NOT NULL
        GROUP BY chain_id, (block_number // {CODE_ROLLUP_BUCKET_BLOCKS}), code_hash;
        "#
    ))
    .context("backfill bucketed code rollup")
}

/// Bring the rollups in line with the current parquet file set (plus the
/// optional Zellic snapshot tables). Returns whether anything was ingested.
pub(crate) fn sync_rollups(
    conn: &Connection,
    data_dir: &Path,
    contracts_glob: &str,
) -> Result<bool> {
    backfill_code_block_rollup(conn)?;
    let files = list_contract_parquet_files(data_dir, contracts_glob)?;
    let mut tracked = tracked_sources(conn)?;

    let mut current: HashSet<String> = files.iter().map(|p| p.display().to_string()).collect();
    let has_zellic = table_exists(conn, "zellic_contracts")?;
    if has_zellic {
        current.insert(ZELLIC_SOURCE_KEY.to_string());
    }

    let removed: Vec<String> = tracked.difference(&current).cloned().collect();
    if !removed.is_empty() {
        tracing::warn!(
            "{} rollup source(s) disappeared (e.g. {}); rebuilding deployment rollups from scratch",
            removed.len(),
            removed[0]
        );
        conn.execute_batch(
            r#"
            BEGIN;
            DELETE FROM contract_deployments_native;
            DELETE FROM rollup_block_counts;
            DELETE FROM rollup_code_counts;
            DELETE FROM rollup_code_block_counts;
            DELETE FROM rollup_sources;
            COMMIT;
            "#,
        )
        .context("reset deployment rollups")?;
        tracked.clear();
    }

    let mut changed = !removed.is_empty();
    for file in &files {
        let key = file.display().to_string();
        if tracked.contains(&key) {
            continue;
        }
        match ingest_parquet(conn, file) {
            Ok(rows) => {
                changed = true;
                tracing::info!(
                    "rolled up {} ({} new deployments)",
                    file.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(key.as_str()),
                    rows
                );
            }
            Err(err) => {
                tracing::warn!("skipping rollup for {}: {:#}", file.display(), err);
            }
        }
    }
    if has_zellic && !tracked.contains(ZELLIC_SOURCE_KEY) {
        tracing::info!("rolling up Zellic snapshot into native deployments (one-time)");
        let rows = ingest_zellic(conn)?;
        tracing::info!("zellic rollup complete ({} new deployments)", rows);
        changed = true;
    }

    Ok(changed)
}

fn tracked_sources(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT source_path FROM rollup_sources")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    rows.collect::<Result<HashSet<_>, _>>()
        .context("list tracked rollup sources")
}

/// Column names of a parquet file, so ingest can adapt to the differing
/// schemas of blink/cryo/paradigm sources instead of failing outright.
fn parquet_columns(conn: &Connection, path_sql: &str) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(&format!(
        "DESCRIBE SELECT * FROM read_parquet('{path_sql}')"
    ))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    rows.collect::<Result<HashSet<_>, _>>()
        .context("describe parquet columns")
}

fn ingest_parquet(conn: &Connection, path: &Path) -> Result<u64> {
    let source_key = path.display().to_string();
    let path_sql = source_key.replace('\'', "''");
    let columns = parquet_columns(conn, &path_sql)?;
    if !columns.contains("block_number") || !columns.contains("contract_address") {
        bail!("parquet file lacks block_number/contract_address columns");
    }

    let fallback_chain = contract_file_range(path)
        .chain_id
        .unwrap_or(ETHEREUM_CHAIN_ID);
    let chain_expr = if columns.contains("chain_id") {
        format!("COALESCE(chain_id, {fallback_chain})")
    } else {
        fallback_chain.to_string()
    };
    let create_index_expr = if columns.contains("create_index") {
        "COALESCE(create_index, 0)"
    } else {
        "0"
    };
    let deployer_expr = if columns.contains("deployer") {
        "deployer"
    } else {
        "CAST(NULL AS BLOB)"
    };
    let code_hash_expr = if columns.contains("code_hash") {
        "code_hash"
    } else {
        "CAST(NULL AS BLOB)"
    };
    let n_code_bytes_expr = if columns.contains("n_code_bytes") {
        "n_code_bytes"
    } else if columns.contains("code") {
        "length(code)"
    } else {
        "CAST(NULL AS UINTEGER)"
    };

    let select = format!(
        r#"
        SELECT
            {chain_expr}::UBIGINT AS chain_id,
            block_number::UINTEGER AS block_number,
            {create_index_expr}::UINTEGER AS create_index,
            contract_address,
            {deployer_expr} AS deployer,
            {code_hash_expr} AS code_hash,
            {n_code_bytes_expr}::UINTEGER AS n_code_bytes
        FROM read_parquet('{path_sql}')
        WHERE block_number IS NOT NULL
          AND contract_address IS NOT NULL
        "#
    );
    ingest_source(conn, &source_key, &select)
}

fn ingest_zellic(conn: &Connection) -> Result<u64> {
    let chain_expr = if column_exists(conn, "zellic_contracts", "chain_id")? {
        "COALESCE(z.chain_id, 1)"
    } else {
        "1"
    };
    let (bytecode_join, n_code_bytes_expr) = if table_exists(conn, "zellic_bytecodes")? {
        (
            "LEFT JOIN zellic_bytecodes b ON z.bytecode_hash = b.code_hash",
            "b.n_code_bytes",
        )
    } else {
        ("", "CAST(NULL AS UINTEGER)")
    };
    let select = format!(
        r#"
        SELECT
            {chain_expr}::UBIGINT AS chain_id,
            z.block_number::UINTEGER AS block_number,
            COALESCE(z.create_index, 0)::UINTEGER AS create_index,
            z.contract_address,
            CAST(NULL AS BLOB) AS deployer,
            z.bytecode_hash AS code_hash,
            {n_code_bytes_expr}::UINTEGER AS n_code_bytes
        FROM zellic_contracts z
        {bytecode_join}
        WHERE z.block_number IS NOT NULL
          AND z.contract_address IS NOT NULL
        "#
    );
    ingest_source(conn, ZELLIC_SOURCE_KEY, &select)
}

/// Ingest one source inside a transaction: dedup the incoming rows within the
/// source and against everything already indexed for the affected block
/// window, append the survivors, and fold the same delta into the summary
/// tables so they can never drift from the deployments table.
fn ingest_source(conn: &Connection, source_key: &str, select: &str) -> Result<u64> {
    let source_sql = source_key.replace('\'', "''");
    let result = (|| -> Result<u64> {
        conn.execute_batch("BEGIN;")?;
        conn.execute_batch(&format!(
            r#"
            CREATE OR REPLACE TEMP TABLE rollup_ingest AS
            SELECT
                chain_id,
                block_number,
                create_index,
                contract_address,
                any_value(deployer) AS deployer,
                any_value(code_hash) AS code_hash,
                any_value(n_code_bytes) AS n_code_bytes
            FROM ({select}) src
            GROUP BY chain_id, block_number, create_index, contract_address;
            "#
        ))?;
        let (min_block, max_block): (Option<u32>, Option<u32>) = conn.query_row(
            "SELECT MIN(block_number), MAX(block_number) FROM rollup_ingest",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        let mut inserted = 0u64;
        if let (Some(min_block), Some(max_block)) = (min_block, max_block) {
            conn.execute_batch(&format!(
                r#"
                DELETE FROM rollup_ingest
                WHERE EXISTS (
                    SELECT 1 FROM contract_deployments_native c
                    WHERE c.block_number BETWEEN {min_block} AND {max_block}
                      AND c.chain_id = rollup_ingest.chain_id
                      AND c.block_number = rollup_ingest.block_number
                      AND c.create_index = rollup_ingest.create_index
                      AND c.contract_address = rollup_ingest.contract_address
                );

                INSERT INTO contract_deployments_native
                SELECT
                    chain_id, block_number, create_index, contract_address,
                    deployer, code_hash, n_code_bytes, '{source_sql}'
                FROM rollup_ingest;

                INSERT INTO rollup_block_counts
                SELECT chain_id, block_number, COUNT(*)::UBIGINT
                FROM rollup_ingest
                GROUP BY chain_id, block_number
                ON CONFLICT (chain_id, block_number)
                DO UPDATE SET contract_count = contract_count + excluded.contract_count;

                INSERT INTO rollup_code_counts
                SELECT chain_id, code_hash, any_value(n_code_bytes)::UINTEGER, COUNT(*)::UBIGINT
                FROM rollup_ingest
                WHERE code_hash IS NOT NULL
                GROUP BY chain_id, code_hash
                ON CONFLICT (chain_id, code_hash)
                DO UPDATE SET contract_count = contract_count + excluded.contract_count;

                INSERT INTO rollup_code_block_counts
                SELECT
                    chain_id,
                    (block_number // {CODE_ROLLUP_BUCKET_BLOCKS})::UBIGINT,
                    code_hash,
                    any_value(n_code_bytes)::UINTEGER,
                    COUNT(*)::UBIGINT
                FROM rollup_ingest
                WHERE code_hash IS NOT NULL
                GROUP BY chain_id, (block_number // {CODE_ROLLUP_BUCKET_BLOCKS}), code_hash
                ON CONFLICT (chain_id, block_bucket, code_hash)
                DO UPDATE SET contract_count = contract_count + excluded.contract_count;
                "#
            ))?;
            inserted = conn.query_row("SELECT COUNT(*) FROM rollup_ingest", [], |row| {
                row.get::<_, i64>(0)
            })? as u64;
        }

        conn.execute(
            "INSERT INTO rollup_sources (source_path, start_block, end_block, row_count) VALUES (?, ?, ?, ?)",
            params![
                source_key,
                min_block.map(u64::from),
                max_block.map(u64::from),
                inserted
            ],
        )?;
        conn.execute_batch("DROP TABLE IF EXISTS rollup_ingest; COMMIT;")?;
        Ok(inserted)
    })();
    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK;");
        let _ = conn.execute_batch("DROP TABLE IF EXISTS rollup_ingest;");
    }
    result.with_context(|| format!("ingest rollup source {source_key}"))
}

/// Subtract one source's exact contribution from the deployments table and
/// every summary rollup, and forget it in `rollup_sources` so the next sync
/// re-ingests it. Exact because every deployment row records which source
/// inserted it; rows this source lost to dedup belong to another source and
/// correctly stay.
fn invalidate_source(conn: &Connection, source_key: &str) -> Result<()> {
    if !rollups_ready(conn)? {
        return Ok(());
    }
    let source_sql = source_key.replace('\'', "''");
    let result = (|| -> Result<()> {
        conn.execute_batch(&format!(
            r#"
            BEGIN;
            CREATE OR REPLACE TEMP TABLE rollup_removed AS
            SELECT chain_id, block_number, code_hash, n_code_bytes
            FROM contract_deployments_native
            WHERE source_path = '{source_sql}';

            DELETE FROM contract_deployments_native
            WHERE source_path = '{source_sql}';

            UPDATE rollup_block_counts
            SET contract_count = rollup_block_counts.contract_count - d.cnt
            FROM (
                SELECT chain_id, block_number, COUNT(*)::UBIGINT AS cnt
                FROM rollup_removed
                GROUP BY chain_id, block_number
            ) d
            WHERE rollup_block_counts.chain_id = d.chain_id
              AND rollup_block_counts.block_number = d.block_number;
            DELETE FROM rollup_block_counts WHERE contract_count = 0;

            UPDATE rollup_code_counts
            SET contract_count = rollup_code_counts.contract_count - d.cnt
            FROM (
                SELECT chain_id, code_hash, COUNT(*)::UBIGINT AS cnt
                FROM rollup_removed
                WHERE code_hash IS NOT NULL
                GROUP BY chain_id, code_hash
            ) d
            WHERE rollup_code_counts.chain_id = d.chain_id
              AND rollup_code_counts.code_hash = d.code_hash;
            DELETE FROM rollup_code_counts WHERE contract_count = 0;

            UPDATE rollup_code_block_counts
            SET contract_count = rollup_code_block_counts.contract_count - d.cnt
            FROM (
                SELECT
                    chain_id,
                    (block_number // {CODE_ROLLUP_BUCKET_BLOCKS})::UBIGINT AS block_bucket,
                    code_hash,
                    COUNT(*)::UBIGINT AS cnt
                FROM rollup_removed
                WHERE code_hash IS NOT NULL
                GROUP BY chain_id, (block_number // {CODE_ROLLUP_BUCKET_BLOCKS}), code_hash
            ) d
            WHERE rollup_code_block_counts.chain_id = d.chain_id
              AND rollup_code_block_counts.block_bucket = d.block_bucket
              AND rollup_code_block_counts.code_hash = d.code_hash;
            DELETE FROM rollup_code_block_counts WHERE contract_count = 0;

            DELETE FROM rollup_sources WHERE source_path = '{source_sql}';
            DROP TABLE IF EXISTS rollup_removed;
            COMMIT;
            "#
        ))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK;");
        let _ = conn.execute_batch("DROP TABLE IF EXISTS rollup_removed;");
    }
    result.with_context(|| format!("invalidate rollup source {source_key}"))
}

/// Called by `blink load --overwrite` before it drops and re-imports the
/// Zellic snapshot tables: without this, the rebuilt snapshot would never be
/// re-rolled-up because `rollup_sources` still marks it as ingested.
pub fn invalidate_zellic_rollups(conn: &Connection) -> Result<()> {
    invalidate_source(conn, ZELLIC_SOURCE_KEY)
}

/// Fill enrichment rows that lack a block position from the deployments
/// table, so the verified-ratio chart can bucket them without joins at
/// request time.
pub(crate) fn backfill_enrichment_blocks(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "enrichment")? {
        return Ok(());
    }
    if !column_exists(conn, "enrichment", "block_number")?
        || !column_exists(conn, "enrichment", "create_index")?
    {
        return Ok(());
    }
    conn.execute_batch(
        r#"
        UPDATE enrichment AS e
        SET
            block_number = c.block_number,
            create_index = c.create_index
        FROM contract_deployments_native AS c
        WHERE e.contract_address = c.contract_address
          AND e.chain_id = c.chain_id
          AND (e.block_number IS NULL OR e.create_index IS NULL);
        "#,
    )
    .context("backfill enrichment block positions")
}

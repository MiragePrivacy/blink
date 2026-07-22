//! Materialized backing table for the SQL explorer (`POST /api/query`).

use anyhow::{Context, Result};
use duckdb::{params, Connection};

use super::{table_exists, views, Db};

const EXPLORER_FINGERPRINT_KEY: &str = "explorer_fingerprint";
/// Rows per build slice — bounds the join/sort working set per transaction.
const EXPLORER_SLICE_TARGET_ROWS: u64 = 250_000;

pub(crate) fn cleanup_stale_build(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "contract_metadata_native_build")? {
        return Ok(());
    }
    tracing::warn!("removing incomplete sql explorer build from an earlier run");
    conn.execute_batch("DROP TABLE contract_metadata_native_build;")
        .context("remove incomplete sql explorer build")
}

pub(crate) fn ensure_explorer_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS blink_state (
            key VARCHAR PRIMARY KEY,
            value VARCHAR NOT NULL
        );
        CREATE TABLE IF NOT EXISTS contract_metadata_bounds (
            chain_id UBIGINT PRIMARY KEY,
            max_block UBIGINT NOT NULL
        );
        "#,
    )
    .context("create explorer state schema")
}

/// Inputs that change what the materialized rows would contain (deployment
/// growth is handled separately: tail growth beyond the bounds is served
/// live, backfills below the bounds are detected by `counts_below_bounds`).
fn input_fingerprint(conn: &Connection) -> Result<String> {
    let (meta_rows, meta_latest): (i64, Option<String>) =
        if table_exists(conn, "bytecode_metadata_by_hash")? {
            conn.query_row(
                "SELECT COUNT(*), CAST(MAX(decoded_at) AS VARCHAR) FROM bytecode_metadata_by_hash",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or((0, None))
        } else {
            (0, None)
        };
    let (enrichment_rows, enrichment_latest): (i64, Option<String>) =
        if table_exists(conn, "enrichment")? {
            conn.query_row(
                "SELECT COUNT(*), CAST(MAX(checked_at) AS VARCHAR) FROM enrichment",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or((0, None))
        } else {
            (0, None)
        };
    let registry_rows: i64 = if table_exists(conn, "verification_registry_imports")? {
        conn.query_row(
            "SELECT COUNT(*) FROM verification_registry_imports",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0)
    } else {
        0
    };
    // Bump the version prefix when the table schema or physical layout
    // changes so upgraded servers rebuild instead of querying a stale copy.
    Ok(format!(
        "v3|meta:{meta_rows}@{}|enr:{enrichment_rows}@{}|reg:{registry_rows}",
        meta_latest.unwrap_or_default(),
        enrichment_latest.unwrap_or_default()
    ))
}

fn stored_fingerprint(conn: &Connection) -> Result<Option<String>> {
    if !table_exists(conn, "blink_state")? {
        return Ok(None);
    }
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM blink_state WHERE key = ?",
            params![EXPLORER_FINGERPRINT_KEY],
            |row| row.get(0),
        )
        .ok();
    Ok(value)
}

/// Detect deployments added *below* the materialized head (a backfill file
/// landing mid-history): per chain, the deployment count up to the bound must
/// match what was materialized.
fn deployments_backfilled_below_bounds(conn: &Connection) -> Result<bool> {
    if !table_exists(conn, "contract_metadata_native")? {
        return Ok(true);
    }
    let mismatch: i64 = conn
        .query_row(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT
                    b.chain_id,
                    (
                        SELECT COUNT(*) FROM contract_deployments_native c
                        WHERE c.chain_id = b.chain_id AND c.block_number <= b.max_block
                    ) AS deployed,
                    (
                        SELECT COUNT(*) FROM contract_metadata_native m
                        WHERE m.chain_id = b.chain_id
                    ) AS materialized
                FROM contract_metadata_bounds b
            )
            WHERE deployed != materialized
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap_or(1);
    if mismatch > 0 {
        return Ok(true);
    }
    // A chain that gained its first deployments after the last build has no
    // bounds row at all.
    let unbounded_chains: i64 = conn
        .query_row(
            r#"
            SELECT COUNT(*) FROM (
                SELECT DISTINCT chain_id FROM rollup_block_counts
                WHERE chain_id NOT IN (SELECT chain_id FROM contract_metadata_bounds)
            )
            "#,
            [],
            |row| row.get(0),
        )
        .unwrap_or(1);
    Ok(unbounded_chains > 0)
}

struct ChainSlice {
    chain_id: u64,
    start_block: u64,
    end_block: u64,
}

fn build_slices(conn: &Connection, target_rows: u64) -> Result<Vec<ChainSlice>> {
    let target_rows = target_rows.max(1);
    let mut stmt = conn.prepare(&format!(
        r#"
        WITH cumulative AS (
            SELECT
                chain_id,
                block_number,
                SUM(contract_count) OVER (
                    PARTITION BY chain_id
                    ORDER BY block_number DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::UBIGINT AS cumulative_rows
            FROM rollup_block_counts
        ), assigned AS (
            SELECT
                chain_id,
                block_number,
                ((cumulative_rows - 1) // {target_rows})::UBIGINT AS slice_id
            FROM cumulative
        )
        SELECT chain_id, MIN(block_number), MAX(block_number)
        FROM assigned
        GROUP BY chain_id, slice_id
        ORDER BY chain_id, slice_id
        "#
    ))?;
    let slices = stmt
        .query_map([], |row| {
            Ok(ChainSlice {
                chain_id: row.get(0)?,
                start_block: row.get::<_, u32>(1)?.into(),
                end_block: row.get::<_, u32>(2)?.into(),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(slices)
}

impl Db {
    /// Bring the materialized explorer table up to date. Returns whether a
    /// rebuild ran. Takes and releases the writer lock per slice so the tail
    /// loop keeps running during the minutes-long build.
    pub(crate) fn refresh_explorer_blocking(&self) -> Result<bool> {
        let result = self.refresh_explorer_blocking_inner();
        if result.is_err() {
            let conn = self.writer.blocking_lock();
            if let Err(error) = conn.execute_batch(
                r#"
                DROP TABLE IF EXISTS contract_metadata_native_build;
                DROP TABLE IF EXISTS explorer_meta_build;
                "#,
            ) {
                tracing::warn!("could not clean up failed sql explorer build: {error}");
            }
        }
        result
    }

    fn refresh_explorer_blocking_inner(&self) -> Result<bool> {
        if self.read_only {
            return Ok(false);
        }

        let fingerprint = {
            let conn = self.writer.blocking_lock();
            ensure_explorer_schema(&conn)?;
            let fingerprint = input_fingerprint(&conn)?;
            let unchanged = stored_fingerprint(&conn)?.as_deref() == Some(fingerprint.as_str())
                && !deployments_backfilled_below_bounds(&conn)?;
            if unchanged {
                return Ok(false);
            }
            tracing::info!("rebuilding sql explorer table (contract_metadata_native)");

            // One deduplicated copy of the decode metadata for the whole
            // build, so the per-slice joins don't re-run the window function.
            // Only the columns the join needs — no hex strings — to keep the
            // per-slice hash-build side small on 4GB hosts.
            let has_meta = table_exists(&conn, "bytecode_metadata_by_hash")?;
            let meta_source = if has_meta {
                r#"
                SELECT
                    code_hash, language, compiler_version, has_source_hash,
                    is_erc20, is_erc721, is_erc1155, is_proxy_eip1967,
                    is_proxy_minimal, uses_push0, decoded_at
                FROM decoded_bytecodes
                "#
            } else {
                r#"
                SELECT
                    CAST(NULL AS BLOB) AS code_hash,
                    CAST(NULL AS VARCHAR) AS language,
                    CAST(NULL AS VARCHAR) AS compiler_version,
                    CAST(false AS BOOLEAN) AS has_source_hash,
                    CAST(false AS BOOLEAN) AS is_erc20,
                    CAST(false AS BOOLEAN) AS is_erc721,
                    CAST(false AS BOOLEAN) AS is_erc1155,
                    CAST(false AS BOOLEAN) AS is_proxy_eip1967,
                    CAST(false AS BOOLEAN) AS is_proxy_minimal,
                    CAST(false AS BOOLEAN) AS uses_push0,
                    CAST(NULL AS TIMESTAMP) AS decoded_at
                WHERE FALSE
                "#
            };
            conn.execute_batch(&format!(
                r#"
                CREATE OR REPLACE TEMP TABLE explorer_meta_build AS {meta_source};
                DROP TABLE IF EXISTS contract_metadata_native_build;
                CREATE TABLE contract_metadata_native_build (
                    chain_id UBIGINT NOT NULL,
                    block_number UINTEGER NOT NULL,
                    create_index UINTEGER NOT NULL,
                    contract_address BLOB NOT NULL,
                    deployer BLOB,
                    code_hash BLOB,
                    n_code_bytes UINTEGER,
                    language VARCHAR,
                    compiler_version VARCHAR,
                    has_source_hash BOOLEAN,
                    is_erc20 BOOLEAN,
                    is_erc721 BOOLEAN,
                    is_erc1155 BOOLEAN,
                    is_proxy_eip1967 BOOLEAN,
                    is_proxy_minimal BOOLEAN,
                    uses_push0 BOOLEAN,
                    decoded_at TIMESTAMP,
                    is_verified BOOLEAN,
                    contract_name VARCHAR,
                    verification_source VARCHAR,
                    match_type VARCHAR,
                    verification_checked_at TIMESTAMP,
                    is_decoded BOOLEAN
                );
                "#
            ))
            .context("prepare explorer build")?;
            fingerprint
        };

        let slices = {
            let conn = self.writer.blocking_lock();
            build_slices(&conn, EXPLORER_SLICE_TARGET_ROWS)?
        };
        let total_slices = slices.len();
        let is_verified_expr = {
            let conn = self.writer.blocking_lock();
            if table_exists(&conn, "verification_registry_imports")? {
                "COALESCE(e.is_verified, false)"
            } else {
                "e.is_verified"
            }
        };

        for (index, slice) in slices.iter().enumerate() {
            let conn = self.writer.blocking_lock();
            let ChainSlice {
                chain_id,
                start_block,
                end_block,
            } = slice;
            conn.execute_batch(&format!(
                r#"
                INSERT INTO contract_metadata_native_build
                SELECT
                    c.chain_id, c.block_number, c.create_index, c.contract_address,
                    c.deployer, c.code_hash, c.n_code_bytes,
                    m.language, m.compiler_version,
                    COALESCE(m.has_source_hash, false),
                    COALESCE(m.is_erc20, false),
                    COALESCE(m.is_erc721, false),
                    COALESCE(m.is_erc1155, false),
                    COALESCE(m.is_proxy_eip1967, false),
                    COALESCE(m.is_proxy_minimal, false),
                    COALESCE(m.uses_push0, false),
                    m.decoded_at,
                    {is_verified_expr},
                    e.contract_name,
                    e.verification_source,
                    e.match_type,
                    e.checked_at,
                    m.code_hash IS NOT NULL
                FROM contract_deployments_native c
                LEFT JOIN explorer_meta_build m ON c.code_hash = m.code_hash
                -- The block-range condition relies on
                -- backfill_enrichment_blocks having run at open: every
                -- enrichment row whose address exists in the deployments
                -- table carries that deployment's block_number, so the join
                -- hash-builds only this slice's enrichment rows instead of
                -- the whole table.
                LEFT JOIN enrichment_current e
                  ON c.contract_address = e.contract_address
                 AND c.chain_id = e.chain_id
                 AND e.block_number BETWEEN {start_block} AND {end_block}
                WHERE c.chain_id = {chain_id}
                  AND c.block_number BETWEEN {start_block} AND {end_block}
                ORDER BY c.block_number DESC, c.create_index DESC;
                "#
            ))
            .with_context(|| {
                format!("explorer build slice chain={chain_id} blocks {start_block}-{end_block}")
            })?;
            if total_slices > 1 {
                tracing::info!(
                    "sql explorer build progress: slice {}/{}",
                    index + 1,
                    total_slices
                );
            }
        }

        {
            let conn = self.writer.blocking_lock();
            let fingerprint_sql = fingerprint.replace('\'', "''");
            conn.execute_batch(&format!(
                r#"
                BEGIN;
                DROP TABLE IF EXISTS contract_metadata_native;
                ALTER TABLE contract_metadata_native_build RENAME TO contract_metadata_native;
                DELETE FROM contract_metadata_bounds;
                INSERT INTO contract_metadata_bounds
                SELECT chain_id, MAX(block_number)::UBIGINT
                FROM contract_metadata_native
                GROUP BY chain_id;
                INSERT INTO blink_state VALUES ('{EXPLORER_FINGERPRINT_KEY}', '{fingerprint_sql}')
                ON CONFLICT (key) DO UPDATE SET value = excluded.value;
                COMMIT;
                DROP TABLE IF EXISTS explorer_meta_build;
                "#
            ))
            .context("swap explorer table")?;
        }

        // The contract_metadata view definition depends on the table's
        // existence — rebuild it on every pooled connection.
        {
            let conn = self.writer.blocking_lock();
            views::create_contract_metadata_view(&conn)?;
        }
        for reader in self.readers.iter() {
            let conn = reader.blocking_lock();
            views::create_contract_metadata_view(&conn)?;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explorer_slices_follow_row_density_newest_first() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE rollup_block_counts (
                chain_id UBIGINT,
                block_number UINTEGER,
                contract_count UBIGINT
            );
            INSERT INTO rollup_block_counts VALUES
                (1, 100, 1),
                (1, 90, 9),
                (1, 80, 6),
                (1, 70, 6),
                (100, 200, 3);
            "#,
        )
        .unwrap();

        let slices = build_slices(&conn, 10).unwrap();
        let ranges = slices
            .into_iter()
            .map(|slice| (slice.chain_id, slice.start_block, slice.end_block))
            .collect::<Vec<_>>();
        assert_eq!(
            ranges,
            vec![(1, 90, 100), (1, 80, 80), (1, 70, 70), (100, 200, 200)]
        );
    }

    #[test]
    fn stale_explorer_build_is_removed_at_open() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE contract_metadata_native_build (id INTEGER);")
            .unwrap();

        cleanup_stale_build(&conn).unwrap();
        assert!(!table_exists(&conn, "contract_metadata_native_build").unwrap());
        cleanup_stale_build(&conn).unwrap();
    }
}

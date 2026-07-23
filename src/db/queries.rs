use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use duckdb::{params, Connection, Row};

use super::{rollups, sql, table_exists, Db};

/// Fast path for the "recent deployments" page: scan only this many blocks
/// below the curso``r/head first, and fall back to an unbounded scan when the
/// window doesn't fill the page (tiny datasets, sparse chains).
const RECENT_SCAN_WINDOW_BLOCKS: u64 = 100_000;

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct Stats {
    pub total_contracts: u64,
    pub verified_count: u64,
    pub unverified_count: u64,
    pub verified_pct: f64,
    pub last_block: u64,
    pub first_block: u64,
    pub enrichment_coverage_pct: f64,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct DeployBucket {
    pub block_start: u64,
    pub block_end: u64,
    pub timestamp: DateTime<Utc>,
    pub count: u64,
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct VerifiedRatioBucket {
    pub block_start: u64,
    pub block_end: u64,
    pub timestamp: DateTime<Utc>,
    pub verified: u64,
    pub unverified: u64,
    pub unknown: u64,
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct SizeBin {
    pub label: String,
    pub size_min: u64,
    pub size_max: u64,
    pub count: u64,
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct CompilerCount {
    pub compiler_version: String,
    pub count: u64,
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct LanguageCount {
    pub language: String,
    pub count: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, utoipa::ToSchema)]
pub struct StandardsBreakdown {
    pub erc20: u64,
    pub erc721: u64,
    pub erc1155: u64,
    pub proxy_eip1967: u64,
    pub proxy_minimal: u64,
    pub uses_push0: u64,
    pub has_source_hash: u64,
    pub total_decoded: u64,
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct RecentContract {
    pub address: String,
    pub block_number: u64,
    pub create_index: u64,
    pub timestamp: DateTime<Utc>,
    pub deployer: String,
    pub n_code_bytes: u64,
    pub is_verified: Option<bool>,
    pub contract_name: Option<String>,
    pub compiler_version: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct RecentCursor {
    pub block_number: u64,
    pub create_index: u64,
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct RecentPage {
    pub contracts: Vec<RecentContract>,
    pub has_more: bool,
}

/// One row of the recent-deployments page before decoration.
struct RecentPageRow {
    address: Vec<u8>,
    block_number: u32,
    create_index: u32,
    deployer: Option<Vec<u8>>,
    n_code_bytes: Option<u32>,
    code_hash: Option<Vec<u8>>,
}

fn read_u64_pair(row: &Row<'_>) -> duckdb::Result<(u64, u64)> {
    Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?))
}

fn read_verified_bucket_row(row: &Row<'_>) -> duckdb::Result<(u64, u64, u64, u64)> {
    Ok((
        row.get::<_, u64>(0)?,
        row.get::<_, u64>(1)?,
        row.get::<_, u64>(2)?,
        row.get::<_, u64>(3)?,
    ))
}

fn read_string_u64_pair(row: &Row<'_>) -> duckdb::Result<(String, u64)> {
    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
}

fn range_filter(block_range: Option<(u64, u64)>) -> String {
    range_filter_on("block_number", block_range)
}

fn range_filter_on(column: &str, block_range: Option<(u64, u64)>) -> String {
    block_range
        .map(|(start, end)| format!("AND {column} BETWEEN {start} AND {end}"))
        .unwrap_or_default()
}

/// Per-code-hash deployment counts feeding the size/compiler/standards
/// breakdowns. No range → the full-history rollup. Ranged → fully covered
/// block buckets come from `rollup_code_block_counts` and only the partial
/// edge buckets scan the deployments table, so even year-wide custom ranges
/// stay rollup-speed. Consumers SUM over the rows, so a code_hash may appear
/// once per part.
fn code_counts_source(chain_id: u64, block_range: Option<(u64, u64)>) -> String {
    let Some((start, end)) = block_range else {
        return format!(
            r#"
            SELECT code_hash, n_code_bytes, contract_count
            FROM rollup_code_counts
            WHERE chain_id = {chain_id}
            "#
        );
    };

    let bucket = rollups::CODE_ROLLUP_BUCKET_BLOCKS;
    // Bucket b covers [b*bucket, (b+1)*bucket - 1]; it is fully inside the
    // range iff b >= ceil(start/bucket) and b <= (end+1)/bucket - 1.
    let first_full = start.div_ceil(bucket);
    let full_bucket_end = (end + 1) / bucket; // exclusive
    if first_full >= full_bucket_end {
        return deployments_code_scan(chain_id, start, end);
    }
    let interior_hi = full_bucket_end - 1;
    let mut parts = vec![format!(
        r#"
        SELECT code_hash, n_code_bytes, contract_count
        FROM rollup_code_block_counts
        WHERE chain_id = {chain_id}
          AND block_bucket BETWEEN {first_full} AND {interior_hi}
        "#
    )];
    let interior_start_block = first_full * bucket;
    if start < interior_start_block {
        parts.push(deployments_code_scan(
            chain_id,
            start,
            interior_start_block - 1,
        ));
    }
    let trailing_start_block = (interior_hi + 1) * bucket;
    if end >= trailing_start_block {
        parts.push(deployments_code_scan(chain_id, trailing_start_block, end));
    }
    parts.join("\nUNION ALL\n")
}

fn deployments_code_scan(chain_id: u64, start: u64, end: u64) -> String {
    format!(
        r#"
        SELECT
            code_hash,
            any_value(n_code_bytes)::UINTEGER AS n_code_bytes,
            COUNT(*)::UBIGINT AS contract_count
        FROM contract_deployments_native
        WHERE chain_id = {chain_id}
          AND code_hash IS NOT NULL
          AND block_number BETWEEN {start} AND {end}
        GROUP BY code_hash
        "#
    )
}

fn max_indexed_block(conn: &Connection, chain_id: u64) -> Result<Option<u64>> {
    let block: Option<u32> = conn
        .query_row(
            "SELECT MAX(block_number) FROM rollup_block_counts WHERE chain_id = ?",
            params![chain_id],
            |row| row.get(0),
        )
        .unwrap_or(None);
    Ok(block.map(u64::from))
}

impl Db {
    pub async fn query_sql(
        &self,
        sql: String,
        limit: u32,
        chain_id: Option<u64>,
    ) -> Result<super::SqlQueryResult> {
        let normalized = sql::normalize_read_only_sql(&sql)?;
        let limit = limit.clamp(1, 1_000);
        self.run_read(move |conn| {
            let started = Instant::now();
            let wrapped = sql::wrap_dashboard_query(&normalized, limit, chain_id);
            let mut stmt = conn.prepare(&wrapped).context("prepare dashboard query")?;
            let mut rows = stmt.query([]).context("execute dashboard query")?;
            let (columns, column_count) = {
                let stmt = rows
                    .as_ref()
                    .context("dashboard query statement metadata unavailable")?;
                (stmt.column_names(), stmt.column_count())
            };
            let mut out = Vec::new();
            while let Some(row) = rows.next().context("read dashboard query row")? {
                let mut values = Vec::with_capacity(column_count);
                for idx in 0..column_count {
                    values.push(sql::value_ref_to_json(row.get_ref(idx)?));
                }
                out.push(values);
            }
            let elapsed_ms = started.elapsed().as_millis();
            if elapsed_ms >= 1_000 {
                // The HTTP-level slow log can't see the request body; name
                // the offending SQL here so slow explorer queries are
                // diagnosable from the journal.
                tracing::warn!(
                    "slow sql explorer query ({}ms): {}",
                    elapsed_ms,
                    normalized.chars().take(300).collect::<String>()
                );
            }
            Ok(super::SqlQueryResult {
                columns,
                row_count: out.len(),
                rows: out,
                limit,
                elapsed_ms,
            })
        })
        .await
    }

    pub async fn stats(&self, chain_id: u64) -> Result<Stats> {
        self.run_read(move |conn| {
            let (total, first_block, last_block): (i64, Option<u32>, Option<u32>) = conn
                .query_row(
                    r#"
                    SELECT
                        COALESCE(SUM(contract_count), 0)::BIGINT,
                        MIN(block_number),
                        MAX(block_number)
                    FROM rollup_block_counts
                    WHERE chain_id = ?
                    "#,
                    params![chain_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap_or((0, None, None));
            let total = total.max(0) as u64;

            let verified: i64 = if table_exists(conn, "enrichment")? {
                conn.query_row(
                    r#"
                    SELECT COUNT(*)
                    FROM contract_deployments_native c
                    WHERE c.chain_id = ?
                      AND EXISTS (
                          SELECT 1
                          FROM enrichment e
                          WHERE e.chain_id = c.chain_id
                            AND e.contract_address = c.contract_address
                            AND e.is_verified
                      )
                    "#,
                    params![chain_id],
                    |row| row.get(0),
                )
                .unwrap_or(0)
            } else {
                0
            };

            let verified_count = (verified.max(0) as u64).min(total);
            let unverified_count = total.saturating_sub(verified_count);
            let verified_pct = if total == 0 {
                0.0
            } else {
                100.0 * verified_count as f64 / total as f64
            };
            let enrichment_coverage_pct = if total == 0 { 0.0 } else { 100.0 };

            Ok(Stats {
                total_contracts: total,
                verified_count,
                unverified_count,
                verified_pct,
                last_block: last_block.map(u64::from).unwrap_or(0),
                first_block: first_block.map(u64::from).unwrap_or(0),
                enrichment_coverage_pct,
                last_updated: Utc::now(),
            })
        })
        .await
    }

    pub async fn deploys_over_time(
        &self,
        chain_id: u64,
        bucket_blocks: u64,
        block_range: Option<(u64, u64)>,
    ) -> Result<Vec<DeployBucket>> {
        let bucket_blocks = bucket_blocks.max(1);
        self.run_read(move |conn| {
            let filter = range_filter(block_range);
            let sql = format!(
                r#"
                SELECT
                    (block_number // {bucket_blocks})::UBIGINT AS bucket_id,
                    SUM(contract_count)::UBIGINT AS cnt
                FROM rollup_block_counts
                WHERE chain_id = {chain_id}
                  {filter}
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], read_u64_pair)?;
            let mut out = Vec::new();
            for r in rows {
                let (bucket_id, count) = r?;
                let block_start = bucket_id * bucket_blocks;
                let block_end = block_start + bucket_blocks - 1;
                let mid = block_start + bucket_blocks / 2;
                out.push(DeployBucket {
                    block_start,
                    block_end,
                    timestamp: crate::blocks::block_timestamp(chain_id, mid),
                    count,
                });
            }
            Ok(out)
        })
        .await
    }

    pub async fn verified_ratio_over_time(
        &self,
        chain_id: u64,
        bucket_blocks: u64,
        block_range: Option<(u64, u64)>,
    ) -> Result<Vec<VerifiedRatioBucket>> {
        let bucket_blocks = bucket_blocks.max(1);
        self.run_read(move |conn| {
            let filter = range_filter(block_range);
            let deployment_filter = range_filter_on("c.block_number", block_range);
            let has_enrichment = table_exists(conn, "enrichment")?;
            let checked_select = if has_enrichment {
                format!(
                    r#"
                    SELECT
                        (c.block_number // {bucket_blocks})::UBIGINT AS bucket_id,
                        COUNT(*)::UBIGINT AS verified
                    FROM contract_deployments_native c
                    WHERE c.chain_id = {chain_id}
                      {deployment_filter}
                      AND EXISTS (
                          SELECT 1
                          FROM enrichment e
                          WHERE e.chain_id = c.chain_id
                            AND e.contract_address = c.contract_address
                            AND e.is_verified
                      )
                    GROUP BY bucket_id
                    "#
                )
            } else {
                r#"
                SELECT
                    CAST(NULL AS UBIGINT) AS bucket_id,
                    0::UBIGINT AS verified
                WHERE FALSE
                "#
                .to_string()
            };

            let verified_value = "LEAST(COALESCE(checked.verified, 0), totals.total)";
            let unverified_expr = format!("GREATEST(totals.total - {verified_value}, 0)::UBIGINT");
            let sql = format!(
                r#"
                WITH totals AS (
                    SELECT
                        (block_number // {bucket_blocks})::UBIGINT AS bucket_id,
                        SUM(contract_count)::UBIGINT AS total
                    FROM rollup_block_counts
                    WHERE chain_id = {chain_id}
                      {filter}
                    GROUP BY bucket_id
                ),
                checked AS (
                    {checked_select}
                )
                SELECT
                    totals.bucket_id,
                    {verified_value}::UBIGINT AS verified,
                    {unverified_expr} AS unverified,
                    0::UBIGINT AS unknown
                FROM totals
                LEFT JOIN checked ON totals.bucket_id = checked.bucket_id
                ORDER BY totals.bucket_id
                "#
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], read_verified_bucket_row)?;
            let mut out = Vec::new();
            for r in rows {
                let (bucket_id, verified, unverified, unknown) = r?;
                let block_start = bucket_id * bucket_blocks;
                let block_end = block_start + bucket_blocks - 1;
                let mid = block_start + bucket_blocks / 2;
                out.push(VerifiedRatioBucket {
                    block_start,
                    block_end,
                    timestamp: crate::blocks::block_timestamp(chain_id, mid),
                    verified,
                    unverified,
                    unknown,
                });
            }
            Ok(out)
        })
        .await
    }

    pub async fn bytecode_size_distribution(
        &self,
        chain_id: u64,
        block_range: Option<(u64, u64)>,
    ) -> Result<Vec<SizeBin>> {
        self.run_read(move |conn| {
            // Fixed semantic buckets keep the heavy tail readable and stop the
            // first 1KB range from hiding zero-byte and minimal-proxy contracts.
            let bucket_defs: [(u64, u64, &str); 12] = [
                (0, 0, "0 B"),
                (1, 32, "1-32 B"),
                (33, 44, "33-44 B"),
                (45, 45, "45 B minimal proxy"),
                (45, 45, "45 B other"),
                (46, 64, "46-64 B"),
                (65, 256, "65-256 B"),
                (257, 1_024, "257 B-1 KB"),
                (1_025, 4_096, "1-4 KB"),
                (4_097, 8_192, "4-8 KB"),
                (8_193, 16_384, "8-16 KB"),
                (16_385, 24_576, "16-24 KB"),
            ];
            let mut counts = vec![0u64; bucket_defs.len()];
            let bin_case = r#"
                    CASE
                        WHEN n_code_bytes = 0 THEN 0
                        WHEN n_code_bytes <= 32 THEN 1
                        WHEN n_code_bytes <= 44 THEN 2
                        WHEN n_code_bytes = 45 AND COALESCE(is_proxy_minimal, false) THEN 3
                        WHEN n_code_bytes = 45 THEN 4
                        WHEN n_code_bytes <= 64 THEN 5
                        WHEN n_code_bytes <= 256 THEN 6
                        WHEN n_code_bytes <= 1024 THEN 7
                        WHEN n_code_bytes <= 4096 THEN 8
                        WHEN n_code_bytes <= 8192 THEN 9
                        WHEN n_code_bytes <= 16384 THEN 10
                        ELSE 11
                    END AS bin_id
            "#;
            let count_source = code_counts_source(chain_id, block_range);
            let sql = format!(
                r#"
                WITH counts AS (
                    {count_source}
                )
                SELECT
                    {bin_case},
                    SUM(counts.contract_count)::UBIGINT AS cnt
                FROM (
                    SELECT
                        counts.n_code_bytes AS n_code_bytes,
                        m.is_proxy_minimal AS is_proxy_minimal,
                        counts.contract_count AS contract_count
                    FROM counts
                    LEFT JOIN bytecode_metadata_by_hash m ON counts.code_hash = m.code_hash
                ) counts
                WHERE counts.n_code_bytes IS NOT NULL
                  AND counts.n_code_bytes <= 24576
                GROUP BY bin_id
                ORDER BY bin_id
                "#
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], read_u64_pair)?;
            for r in rows {
                let (bin_id, count) = r?;
                if let Some(slot) = counts.get_mut(bin_id as usize) {
                    *slot = count;
                }
            }
            Ok(bucket_defs
                .into_iter()
                .enumerate()
                .map(|(i, (size_min, size_max, label))| SizeBin {
                    label: label.to_string(),
                    size_min,
                    size_max,
                    count: counts[i],
                })
                .collect())
        })
        .await
    }

    pub async fn top_compilers(
        &self,
        chain_id: u64,
        limit: u32,
        block_range: Option<(u64, u64)>,
    ) -> Result<Vec<CompilerCount>> {
        let limit = limit.clamp(1, 50);
        self.run_read(move |conn| {
            // Compiler distribution is bytecode-derived. Verification sources
            // can confirm source publication, but local decode remains the
            // source of truth for compiler metadata in this dashboard.
            let count_source = code_counts_source(chain_id, block_range);
            let sql = format!(
                r#"
                WITH counts AS (
                    {count_source}
                )
                SELECT m.compiler_version, SUM(c.contract_count)::UBIGINT AS cnt
                FROM counts c
                JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                WHERE m.compiler_version IS NOT NULL
                GROUP BY m.compiler_version
                ORDER BY cnt DESC
                LIMIT ?
                "#
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![limit as i64], read_string_u64_pair)?;
            let mut out = Vec::new();
            for r in rows {
                let (compiler_version, count) = r?;
                out.push(CompilerCount {
                    compiler_version,
                    count,
                });
            }
            Ok(out)
        })
        .await
    }

    pub async fn compiler_version_total(
        &self,
        chain_id: u64,
        block_range: Option<(u64, u64)>,
    ) -> Result<u64> {
        self.run_read(move |conn| {
            let count_source = code_counts_source(chain_id, block_range);
            let sql = format!(
                r#"
                WITH counts AS (
                    {count_source}
                )
                SELECT COALESCE(SUM(c.contract_count), 0)::BIGINT
                FROM counts c
                JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                WHERE m.compiler_version IS NOT NULL
                "#
            );
            let count: i64 = conn.query_row(&sql, [], |row| row.get(0)).unwrap_or(0);
            Ok(count.max(0) as u64)
        })
        .await
    }

    pub async fn language_distribution(&self, chain_id: u64) -> Result<Vec<LanguageCount>> {
        self.run_read(move |conn| {
            let sql = format!(
                r#"
                SELECT COALESCE(m.language, 'unknown') AS lang,
                       SUM(c.contract_count)::UBIGINT AS cnt
                FROM rollup_code_counts c
                LEFT JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                WHERE c.chain_id = {chain_id}
                GROUP BY lang
                ORDER BY cnt DESC
                "#
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], read_string_u64_pair)?;
            let mut out = Vec::new();
            for r in rows {
                let (language, count) = r?;
                out.push(LanguageCount { language, count });
            }
            Ok(out)
        })
        .await
    }

    pub async fn standards_breakdown(
        &self,
        chain_id: u64,
        block_range: Option<(u64, u64)>,
    ) -> Result<StandardsBreakdown> {
        self.run_read(move |conn| {
            let count_source = code_counts_source(chain_id, block_range);
            let sql = format!(
                r#"
                WITH counts AS (
                    {count_source}
                )
                SELECT
                    COALESCE(SUM(CASE WHEN m.is_erc20 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                    COALESCE(SUM(CASE WHEN m.is_erc721 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                    COALESCE(SUM(CASE WHEN m.is_erc1155 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                    COALESCE(SUM(CASE WHEN m.is_proxy_eip1967 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                    COALESCE(SUM(CASE WHEN m.is_proxy_minimal THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                    COALESCE(SUM(CASE WHEN m.uses_push0 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                    COALESCE(SUM(CASE WHEN m.has_source_hash THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                    COALESCE(SUM(CASE WHEN m.code_hash IS NOT NULL THEN c.contract_count ELSE 0 END), 0)::UBIGINT
                FROM counts c
                LEFT JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                "#
            );
            conn.query_row(&sql, [], |row| {
                Ok(StandardsBreakdown {
                    erc20: row.get(0)?,
                    erc721: row.get(1)?,
                    erc1155: row.get(2)?,
                    proxy_eip1967: row.get(3)?,
                    proxy_minimal: row.get(4)?,
                    uses_push0: row.get(5)?,
                    has_source_hash: row.get(6)?,
                    total_decoded: row.get(7)?,
                })
            })
            .or_else(|_| Ok(StandardsBreakdown::default()))
        })
        .await
    }

    pub async fn recent_contracts(
        &self,
        chain_id: u64,
        limit: u32,
        cursor: Option<RecentCursor>,
    ) -> Result<RecentPage> {
        let limit = limit.clamp(1, 200);
        self.run_read(move |conn| {
            let page_limit = limit as usize + 1;
            // Recent pages live near the head of the chain: try a bounded
            // window first so the top-N scan prunes to a handful of row
            // groups, then widen only if the page didn't fill.
            let upper = match cursor {
                Some(cursor) => Some(cursor.block_number),
                None => max_indexed_block(conn, chain_id)?,
            };
            let floor = upper.map(|top| top.saturating_sub(RECENT_SCAN_WINDOW_BLOCKS));
            let mut rows =
                recent_page_rows(conn, chain_id, cursor, page_limit, floor.filter(|f| *f > 0))?;
            if rows.len() < page_limit && floor.map(|f| f > 0).unwrap_or(false) {
                rows = recent_page_rows(conn, chain_id, cursor, page_limit, None)?;
            }

            let has_more = rows.len() > limit as usize;
            rows.truncate(limit as usize);

            // Decorate the page with indexed point lookups instead of hash
            // joins — a join rescans the multi-million-row enrichment and
            // metadata tables for a 20-row page.
            let enrichment = recent_enrichment_by_address(conn, chain_id, &rows)?;
            let compilers = recent_compilers_by_hash(conn, &rows)?;

            let contracts = rows
                .into_iter()
                .map(|row| {
                    let block_u64 = u64::from(row.block_number);
                    let (verified, contract_name) = enrichment
                        .get(&row.address)
                        .cloned()
                        .unwrap_or((None, None));
                    let is_verified = Some(verified.unwrap_or(false));
                    let compiler_version = row
                        .code_hash
                        .as_ref()
                        .and_then(|hash| compilers.get(hash).cloned())
                        .flatten();
                    RecentContract {
                        address: format!("0x{}", hex::encode(&row.address)),
                        block_number: block_u64,
                        create_index: u64::from(row.create_index),
                        timestamp: crate::blocks::block_timestamp(chain_id, block_u64),
                        deployer: format!("0x{}", hex::encode(row.deployer.unwrap_or_default())),
                        n_code_bytes: row.n_code_bytes.map(u64::from).unwrap_or(0),
                        is_verified,
                        contract_name,
                        compiler_version,
                    }
                })
                .collect();
            Ok(RecentPage {
                contracts,
                has_more,
            })
        })
        .await
    }

    /// Highest block with an indexed contract (rollup MAX — native and cheap).
    pub async fn highest_contract_block(&self, chain_id: u64) -> Result<Option<u64>> {
        self.run_read(move |conn| max_indexed_block(conn, chain_id))
            .await
    }

    /// Highest block covered by the dataset, whether or not it contained a
    /// contract: max of the parquet filename ranges and the indexed rollup.
    /// The tail loop resumes from here.
    pub async fn highest_block(&self, chain_id: u64) -> Result<Option<u64>> {
        let data_dir = self.data_dir().to_path_buf();
        let contracts_glob = self.contracts_glob().to_string();
        self.run_read(move |conn| {
            let files = rollups::list_contract_parquet_files(&data_dir, &contracts_glob)?;
            let filename_max = rollups::max_contract_file_block_for_chain(&files, chain_id);
            let indexed_max = max_indexed_block(conn, chain_id)?;
            Ok([filename_max, indexed_max].into_iter().flatten().max())
        })
        .await
    }
}

fn recent_page_rows(
    conn: &Connection,
    chain_id: u64,
    cursor: Option<RecentCursor>,
    page_limit: usize,
    floor: Option<u64>,
) -> Result<Vec<RecentPageRow>> {
    let cursor_filter = cursor
        .map(|c| {
            format!(
                "AND (block_number < {block} OR (block_number = {block} AND create_index < {idx}))",
                block = c.block_number,
                idx = c.create_index
            )
        })
        .unwrap_or_default();
    let floor_filter = floor
        .map(|f| format!("AND block_number >= {f}"))
        .unwrap_or_default();
    let sql = format!(
        r#"
        SELECT contract_address, block_number, create_index, deployer, n_code_bytes, code_hash
        FROM contract_deployments_native
        WHERE chain_id = {chain_id}
          {floor_filter}
          {cursor_filter}
        ORDER BY block_number DESC, create_index DESC
        LIMIT {page_limit}
        "#
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(RecentPageRow {
            address: row.get(0)?,
            block_number: row.get(1)?,
            create_index: row.get(2)?,
            deployer: row.get(3)?,
            n_code_bytes: row.get(4)?,
            code_hash: row.get(5)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>()
        .context("read recent contracts page")
}

fn blob_literal(bytes: &[u8]) -> String {
    format!("unhex('{}')", hex::encode(bytes))
}

/// `(is_verified, contract_name)` per address.
type EnrichmentByAddress = HashMap<Vec<u8>, (Option<bool>, Option<String>)>;

/// Verification status per address for one page: a UNION ALL of indexed
/// point probes (`enrichment_addr_idx`), ~ms even against millions of rows.
fn recent_enrichment_by_address(
    conn: &Connection,
    chain_id: u64,
    rows: &[RecentPageRow],
) -> Result<EnrichmentByAddress> {
    let addresses: HashSet<&Vec<u8>> = rows.iter().map(|row| &row.address).collect();
    if addresses.is_empty() {
        return Ok(HashMap::new());
    }
    let probes = addresses
        .iter()
        .map(|address| {
            format!(
                "SELECT contract_address, is_verified, contract_name FROM enrichment_current \
                 WHERE contract_address = {} AND chain_id = {chain_id}",
                blob_literal(address)
            )
        })
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");
    let mut stmt = conn.prepare(&probes)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Vec<u8>>(0)?,
            row.get::<_, Option<bool>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    let mut out = HashMap::new();
    for row in rows {
        let (address, is_verified, contract_name) = row?;
        out.insert(address, (is_verified, contract_name));
    }
    Ok(out)
}

/// Compiler version per code hash for one page, via
/// `bytecode_metadata_hash_idx` point probes.
fn recent_compilers_by_hash(
    conn: &Connection,
    rows: &[RecentPageRow],
) -> Result<HashMap<Vec<u8>, Option<String>>> {
    if !table_exists(conn, "bytecode_metadata_by_hash")? {
        return Ok(HashMap::new());
    }
    let hashes: HashSet<&Vec<u8>> = rows
        .iter()
        .filter_map(|row| row.code_hash.as_ref())
        .collect();
    if hashes.is_empty() {
        return Ok(HashMap::new());
    }
    let probes = hashes
        .iter()
        .map(|hash| {
            format!(
                "SELECT code_hash, compiler_version FROM bytecode_metadata_by_hash \
                 WHERE code_hash = {}",
                blob_literal(hash)
            )
        })
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");
    let mut stmt = conn.prepare(&probes)?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    let mut out = HashMap::new();
    for row in rows {
        let (hash, compiler_version) = row?;
        out.insert(hash, compiler_version);
    }
    Ok(out)
}

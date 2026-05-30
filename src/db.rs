//! DuckDB-backed query layer for the dashboard.
//!
//! Owns a single persistent DuckDB connection (file: `{data_dir}/blink.duckdb`)
//! that exposes:
//! - a `contracts` view over every `*.parquet` file in the data directory
//!   (multi-source: blink, cryo, paradigm — `union_by_name = true`);
//! - an `enrichment` table populated by bulk verification-registry imports.
//!
//! All query methods return owned, JSON-serializable structs and run on
//! `spawn_blocking` so axum handlers stay async. The connection is wrapped
//! in a `tokio::sync::Mutex`; queries serialize on it, which is acceptable
//! for an analytics dashboard's request rate.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use duckdb::{params, types::ValueRef, AccessMode, Config, Connection, Row};
use serde_json::{Number, Value};
use tokio::sync::Mutex;

use crate::{chains::ETHEREUM_CHAIN_ID, util::match_simple_glob};

const RECENT_PARQUET_FILE_LIMIT: usize = 12;

#[derive(Clone)]
pub struct Db {
    inner: Arc<Mutex<Connection>>,
    data_dir: PathBuf,
    contracts_glob: String,
}

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

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
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

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct SqlQueryResult {
    pub columns: Vec<String>,
    #[schema(value_type = Vec<Vec<Object>>)]
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
    pub limit: u32,
    pub elapsed_ms: u128,
}

type RecentRowData = (
    Vec<u8>,
    u32,
    u32,
    Option<Vec<u8>>,
    Option<u32>,
    Option<bool>,
    Option<String>,
    Option<String>,
);

fn read_recent_row(row: &Row<'_>) -> duckdb::Result<RecentRowData> {
    Ok((
        row.get::<_, Vec<u8>>(0)?,
        row.get::<_, u32>(1)?,
        row.get::<_, u32>(2)?,
        row.get::<_, Option<Vec<u8>>>(3)?,
        row.get::<_, Option<u32>>(4)?,
        row.get::<_, Option<bool>>(5)?,
        row.get::<_, Option<String>>(6)?,
        row.get::<_, Option<String>>(7)?,
    ))
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

#[derive(Debug)]
struct ContractParquetFile {
    path: PathBuf,
    chain_id: Option<u64>,
    start_block: Option<u64>,
    end_block: Option<u64>,
}

fn list_contract_parquet_files(data_dir: &Path, contracts_glob: &str) -> Result<Vec<PathBuf>> {
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

fn contract_file_with_range(path: PathBuf) -> ContractParquetFile {
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
    let len = nums.len();
    let (start_block, end_block) = if len >= 2 {
        (Some(nums[len - 2]), Some(nums[len - 1]))
    } else {
        (None, None)
    };
    ContractParquetFile {
        path,
        chain_id,
        start_block,
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

fn max_contract_file_block_for_chain(files: &[PathBuf], chain_id: u64) -> Option<u64> {
    files
        .iter()
        .cloned()
        .map(contract_file_with_range)
        .filter(|file| {
            file.chain_id == Some(chain_id)
                || (file.chain_id.is_none() && chain_id == ETHEREUM_CHAIN_ID)
        })
        .filter_map(|file| file.end_block)
        .max()
}

fn recent_contract_parquet_files(
    files: &[PathBuf],
    chain_id: u64,
    cursor: Option<RecentCursor>,
) -> Vec<PathBuf> {
    let cursor_block = cursor.map(|cursor| cursor.block_number);
    let mut ranged = files
        .iter()
        .cloned()
        .map(contract_file_with_range)
        .filter(|file| {
            file.chain_id == Some(chain_id)
                || (file.chain_id.is_none() && chain_id == ETHEREUM_CHAIN_ID)
        })
        .collect::<Vec<_>>();
    ranged.sort_by(|a, b| {
        b.end_block
            .unwrap_or(0)
            .cmp(&a.end_block.unwrap_or(0))
            .then_with(|| b.start_block.unwrap_or(0).cmp(&a.start_block.unwrap_or(0)))
            .then_with(|| b.path.cmp(&a.path))
    });

    ranged
        .into_iter()
        .filter(|file| {
            cursor_block
                .map(|block| file.start_block.unwrap_or(0) <= block)
                .unwrap_or(true)
        })
        .take(RECENT_PARQUET_FILE_LIMIT)
        .map(|file| file.path)
        .collect()
}

fn contract_parquet_files_for_block_range(
    files: &[PathBuf],
    chain_id: u64,
    block_range: Option<(u64, u64)>,
) -> Vec<PathBuf> {
    let mut ranged = files
        .iter()
        .cloned()
        .map(contract_file_with_range)
        .filter(|file| {
            file.chain_id == Some(chain_id)
                || (file.chain_id.is_none() && chain_id == ETHEREUM_CHAIN_ID)
        })
        .filter(|file| {
            if let Some((start, end)) = block_range {
                match (file.start_block, file.end_block) {
                    (Some(file_start), Some(file_end)) => file_end >= start && file_start <= end,
                    _ => true,
                }
            } else {
                true
            }
        })
        .collect::<Vec<_>>();
    ranged.sort_by(|a, b| {
        a.start_block
            .unwrap_or(0)
            .cmp(&b.start_block.unwrap_or(0))
            .then_with(|| a.end_block.unwrap_or(0).cmp(&b.end_block.unwrap_or(0)))
            .then_with(|| a.path.cmp(&b.path))
    });
    ranged.into_iter().map(|file| file.path).collect()
}

fn parquet_read_list(files: &[PathBuf]) -> String {
    files
        .iter()
        .map(|p| format!("'{}'", p.display().to_string().replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

fn create_contract_parquet_view(
    conn: &Connection,
    view_name: &str,
    files: &[PathBuf],
    chain_id: u64,
) -> Result<()> {
    let body = if files.is_empty() {
        r#"
            SELECT
                CAST(NULL AS UINTEGER) AS block_number,
                CAST(NULL AS BLOB) AS block_hash,
                CAST(NULL AS UINTEGER) AS create_index,
                CAST(NULL AS BLOB) AS transaction_hash,
                CAST(NULL AS BLOB) AS contract_address,
                CAST(NULL AS BLOB) AS deployer,
                CAST(NULL AS BLOB) AS factory,
                CAST(NULL AS BLOB) AS init_code,
                CAST(NULL AS BLOB) AS code,
                CAST(NULL AS BLOB) AS init_code_hash,
                CAST(NULL AS UINTEGER) AS n_init_code_bytes,
                CAST(NULL AS UINTEGER) AS n_code_bytes,
                CAST(NULL AS BLOB) AS code_hash,
                CAST(NULL AS UBIGINT) AS chain_id
            WHERE FALSE
        "#
        .to_string()
    } else {
        format!(
            r#"
                SELECT
                    block_number, block_hash, create_index, transaction_hash,
                    contract_address, deployer, factory, init_code, code,
                    init_code_hash, n_init_code_bytes, n_code_bytes,
                    code_hash, chain_id
                FROM read_parquet([{}], union_by_name = true)
                WHERE chain_id = {}
            "#,
            parquet_read_list(files),
            chain_id
        )
    };
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP VIEW {view_name} AS\n{body};"
    ))
    .with_context(|| {
        format!(
            "create {view_name} parquet contracts view ({} files)",
            files.len(),
        )
    })?;
    Ok(())
}

fn create_recent_parquet_contracts_view(
    conn: &Connection,
    files: &[PathBuf],
    chain_id: u64,
) -> Result<()> {
    create_contract_parquet_view(conn, "recent_parquet_contracts", files, chain_id)
}

fn ensure_parquet_block_counts(conn: &Connection, files: &[PathBuf]) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS parquet_block_counts (
            source_path VARCHAR NOT NULL,
            chain_id UBIGINT NOT NULL,
            block_number UINTEGER NOT NULL,
            contract_count UBIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS parquet_block_counts_chain_block_idx
            ON parquet_block_counts(chain_id, block_number);
        CREATE INDEX IF NOT EXISTS parquet_block_counts_source_idx
            ON parquet_block_counts(source_path);
        "#,
    )
    .context("create parquet block counts table")?;

    let existing = conn
        .prepare("SELECT DISTINCT source_path FROM parquet_block_counts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let current = files
        .iter()
        .map(|path| path.display().to_string())
        .collect::<HashSet<_>>();
    for source_path in existing {
        if !current.contains(&source_path) {
            conn.execute(
                "DELETE FROM parquet_block_counts WHERE source_path = ?",
                params![source_path],
            )
            .context("delete stale parquet block counts")?;
        }
    }

    let counted = conn
        .prepare("SELECT DISTINCT source_path FROM parquet_block_counts")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<HashSet<_>, _>>()?;
    for file in files {
        let source_path = file.display().to_string();
        if counted.contains(&source_path) {
            continue;
        }
        let source_path_sql = source_path.replace('\'', "''");
        conn.execute_batch(&format!(
            r#"
            INSERT INTO parquet_block_counts
            SELECT
                '{source_path_sql}' AS source_path,
                chain_id::UBIGINT AS chain_id,
                block_number::UINTEGER AS block_number,
                COUNT(*)::UBIGINT AS contract_count
            FROM read_parquet('{source_path_sql}', union_by_name = true)
            WHERE block_number IS NOT NULL
              AND chain_id IS NOT NULL
            GROUP BY chain_id, block_number;
            "#
        ))
        .with_context(|| format!("count parquet blocks in {}", file.display()))?;
    }

    Ok(())
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            r#"
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            "#,
            params![table, column],
            |row| row.get(0),
        )
        .unwrap_or(0);
    Ok(count > 0)
}

fn contains_sql_keyword(sql: &str, keyword: &str) -> bool {
    let bytes = sql.as_bytes();
    let needle = keyword.as_bytes();
    if needle.is_empty() || needle.len() > bytes.len() {
        return false;
    }

    bytes
        .windows(needle.len())
        .enumerate()
        .any(|(idx, window)| {
            if window != needle {
                return false;
            }
            let before = idx.checked_sub(1).and_then(|i| bytes.get(i)).copied();
            let after = bytes.get(idx + needle.len()).copied();
            !is_sql_ident_byte(before) && !is_sql_ident_byte(after)
        })
}

fn is_sql_ident_byte(byte: Option<u8>) -> bool {
    matches!(byte, Some(b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

fn normalize_read_only_sql(sql: &str) -> Result<String> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("query is empty"));
    }
    if trimmed.len() > 20_000 {
        return Err(anyhow!("query is too large"));
    }

    let without_trailing_semicolon = trimmed
        .strip_suffix(';')
        .map(str::trim_end)
        .unwrap_or(trimmed);
    if without_trailing_semicolon.contains(';') {
        return Err(anyhow!("only one read-only statement is allowed"));
    }

    let lower = without_trailing_semicolon.to_ascii_lowercase();
    let first = lower.split_whitespace().next().unwrap_or_default();
    if first != "select" && first != "with" {
        return Err(anyhow!("only SELECT and WITH queries are allowed"));
    }

    for keyword in [
        "alter",
        "attach",
        "call",
        "checkpoint",
        "copy",
        "create",
        "delete",
        "detach",
        "drop",
        "export",
        "import",
        "insert",
        "install",
        "load",
        "pragma",
        "set",
        "update",
        "vacuum",
    ] {
        if contains_sql_keyword(&lower, keyword) {
            return Err(anyhow!(
                "keyword `{}` is not allowed in dashboard queries",
                keyword
            ));
        }
    }

    for function in [
        "read_blob",
        "read_csv",
        "read_json",
        "read_parquet",
        "csv_scan",
        "parquet_scan",
        "sqlite_scan",
        "postgres_scan",
        "mysql_scan",
        "httpfs",
    ] {
        if lower.contains(function) {
            return Err(anyhow!(
                "file and extension access is not allowed in dashboard queries"
            ));
        }
    }

    Ok(without_trailing_semicolon.to_string())
}

fn wrap_dashboard_query(sql: &str, limit: u32, chain_id: Option<u64>) -> String {
    match chain_id {
        Some(chain_id) => format!(
            r#"
            WITH contract_metadata AS (
                SELECT *
                FROM contract_metadata_all
                WHERE chain_id = {chain_id}
            )
            SELECT *
            FROM ({sql}) AS _blink_dashboard_query
            LIMIT {limit}
            "#
        ),
        None => format!("SELECT * FROM ({sql}) AS _blink_dashboard_query LIMIT {limit}"),
    }
}

fn value_ref_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(value) => Value::Bool(value),
        ValueRef::TinyInt(value) => Value::Number(Number::from(value)),
        ValueRef::SmallInt(value) => Value::Number(Number::from(value)),
        ValueRef::Int(value) => Value::Number(Number::from(value)),
        ValueRef::BigInt(value) => Value::Number(Number::from(value)),
        ValueRef::HugeInt(value) => i64::try_from(value)
            .map(Number::from)
            .map(Value::Number)
            .unwrap_or_else(|_| Value::String(value.to_string())),
        ValueRef::UTinyInt(value) => Value::Number(Number::from(value)),
        ValueRef::USmallInt(value) => Value::Number(Number::from(value)),
        ValueRef::UInt(value) => Value::Number(Number::from(value)),
        ValueRef::UBigInt(value) => Value::Number(Number::from(value)),
        ValueRef::Float(value) => Number::from_f64(value as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ValueRef::Double(value) => Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ValueRef::Decimal(value) => Value::String(value.to_string()),
        ValueRef::Timestamp(unit, value) => {
            Value::String(format!("{} {:?}", value, unit).to_ascii_lowercase())
        }
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::String(format!("0x{}", hex::encode(value))),
        ValueRef::Date32(value) => Value::Number(Number::from(value)),
        ValueRef::Time64(unit, value) => {
            Value::String(format!("{} {:?}", value, unit).to_ascii_lowercase())
        }
        ValueRef::Interval {
            months,
            days,
            nanos,
        } => Value::String(format!("{months} months {days} days {nanos} ns")),
        other => Value::String(format!("{other:?}")),
    }
}

fn create_empty_metadata_current_view(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE OR REPLACE TEMP VIEW bytecode_metadata_current AS
        SELECT
            CAST(NULL AS BLOB) AS contract_address,
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
        WHERE FALSE;
        "#,
    )
    .context("create empty metadata view")
}

fn create_metadata_current_view(conn: &Connection) -> Result<()> {
    let has_v1 = table_exists(conn, "bytecode_metadata")?;
    let has_v2 = table_exists(conn, "bytecode_metadata_v2")?;

    if !has_v1 && !has_v2 {
        return create_empty_metadata_current_view(conn);
    }

    let address_meta = match (has_v1, has_v2) {
        (false, false) => {
            r#"
            SELECT
                CAST(NULL AS BLOB) AS contract_address,
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
        }
        (true, false) => {
            // v1 predates EIP-1167 detection — synthesize a false column so the
            // view shape matches.
            r#"
            SELECT
                contract_address, language, compiler_version, has_source_hash,
                is_erc20, is_erc721, is_erc1155, is_proxy_eip1967,
                CAST(false AS BOOLEAN) AS is_proxy_minimal,
                uses_push0, CAST(NULL AS TIMESTAMP) AS decoded_at
            FROM bytecode_metadata
            "#
        }
        (false, true) => {
            r#"
            SELECT
                contract_address, language, compiler_version, has_source_hash,
                is_erc20, is_erc721, is_erc1155, is_proxy_eip1967,
                is_proxy_minimal, uses_push0, decoded_at
            FROM bytecode_metadata_v2
            "#
        }
        (true, true) => {
            r#"
            SELECT
                contract_address, language, compiler_version, has_source_hash,
                is_erc20, is_erc721, is_erc1155, is_proxy_eip1967,
                is_proxy_minimal, uses_push0, decoded_at
            FROM bytecode_metadata_v2
            UNION ALL
            SELECT
                v1.contract_address, v1.language, v1.compiler_version,
                v1.has_source_hash, v1.is_erc20, v1.is_erc721, v1.is_erc1155,
                v1.is_proxy_eip1967, CAST(false AS BOOLEAN) AS is_proxy_minimal,
                v1.uses_push0, CAST(NULL AS TIMESTAMP) AS decoded_at
            FROM bytecode_metadata v1
            WHERE NOT EXISTS (
                SELECT 1
                FROM bytecode_metadata_v2 v2
                WHERE v2.contract_address = v1.contract_address
            )
            "#
        }
    };

    let sql = format!(
        r#"
        CREATE OR REPLACE TEMP VIEW bytecode_metadata_current AS
        {address_meta};
        "#
    );
    conn.execute_batch(&sql)
        .context("create combined metadata view")
}

fn create_enrichment_current_view(conn: &Connection) -> Result<()> {
    let sql = if table_exists(conn, "enrichment")? {
        let chain_id = if column_exists(conn, "enrichment", "chain_id")? {
            "chain_id"
        } else {
            "1::UBIGINT AS chain_id"
        };
        let verification_source = if column_exists(conn, "enrichment", "verification_source")? {
            "verification_source"
        } else {
            "CAST(NULL AS VARCHAR) AS verification_source"
        };
        let match_type = if column_exists(conn, "enrichment", "match_type")? {
            "match_type"
        } else {
            "CAST(NULL AS VARCHAR) AS match_type"
        };
        let block_number = if column_exists(conn, "enrichment", "block_number")? {
            "block_number"
        } else {
            "CAST(NULL AS UINTEGER) AS block_number"
        };
        let create_index = if column_exists(conn, "enrichment", "create_index")? {
            "create_index"
        } else {
            "CAST(NULL AS UINTEGER) AS create_index"
        };
        format!(
            r#"
        CREATE OR REPLACE TEMP VIEW enrichment_current AS
        SELECT
            contract_address,
            {chain_id},
            is_verified,
            contract_name,
            checked_at,
            {verification_source},
            {match_type},
            {block_number},
            {create_index}
        FROM enrichment;
        "#
        )
    } else {
        r#"
        CREATE OR REPLACE TEMP VIEW enrichment_current AS
        SELECT
            CAST(NULL AS BLOB) AS contract_address,
            CAST(NULL AS UBIGINT) AS chain_id,
            CAST(NULL AS BOOLEAN) AS is_verified,
            CAST(NULL AS VARCHAR) AS contract_name,
            CAST(NULL AS TIMESTAMP) AS checked_at,
            CAST(NULL AS VARCHAR) AS verification_source,
            CAST(NULL AS VARCHAR) AS match_type,
            CAST(NULL AS UINTEGER) AS block_number,
            CAST(NULL AS UINTEGER) AS create_index
        WHERE FALSE;
        "#
        .to_string()
    };
    conn.execute_batch(&sql)
        .context("create enrichment compatibility view")
}

fn verification_registry_loaded(conn: &Connection, chain_id: u64) -> Result<bool> {
    if !table_exists(conn, "verification_registry_imports")? {
        return Ok(false);
    }
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM verification_registry_imports WHERE source = 'verifier_alliance' AND chain_id = ?",
            params![chain_id],
            |row| row.get(0),
        )
        .unwrap_or(0);
    Ok(count > 0)
}

fn create_standard_query_views(conn: &Connection, has_zellic: bool) -> Result<()> {
    let has_hash = table_exists(conn, "bytecode_metadata_by_hash")?;
    let has_counts = table_exists(conn, "zellic_bytecode_counts")?;
    let hash_has_decoded_at =
        has_hash && column_exists(conn, "bytecode_metadata_by_hash", "decoded_at")?;

    let bytecodes_sql = if has_zellic {
        let count_join = if has_counts {
            r#"
                COALESCE(c.contract_count, 0)::UBIGINT AS contract_count
            FROM zellic_bytecodes b
            LEFT JOIN zellic_bytecode_counts c ON b.code_hash = c.code_hash
            "#
        } else {
            r#"
                CAST(NULL AS UBIGINT) AS contract_count
            FROM zellic_bytecodes b
            "#
        };
        format!(
            r#"
            CREATE OR REPLACE TEMP VIEW bytecodes AS
            SELECT
                b.code_hash,
                lower('0x' || hex(b.code_hash)) AS code_hash_hex,
                b.n_code_bytes,
                b.code,
                {count_join};
            "#
        )
    } else {
        r#"
        CREATE OR REPLACE TEMP VIEW bytecodes AS
        SELECT
            code_hash,
            lower('0x' || hex(code_hash)) AS code_hash_hex,
            any_value(n_code_bytes)::UINTEGER AS n_code_bytes,
            any_value(code) AS code,
            COUNT(*)::UBIGINT AS contract_count
        FROM contracts
        WHERE code_hash IS NOT NULL
        GROUP BY code_hash;
        "#
        .to_string()
    };
    conn.execute_batch(&bytecodes_sql)
        .context("create bytecodes query view")?;

    let decoded_sql = if has_hash {
        let decoded_at = if hash_has_decoded_at {
            "decoded_at"
        } else {
            "CAST(NULL AS TIMESTAMP) AS decoded_at"
        };
        let decoded_order = if hash_has_decoded_at {
            "decoded_at DESC NULLS LAST"
        } else {
            "code_hash"
        };
        format!(
            r#"
            CREATE OR REPLACE TEMP VIEW decoded_bytecodes AS
            SELECT
                code_hash,
                lower('0x' || hex(code_hash)) AS code_hash_hex,
                language,
                compiler_version,
                has_source_hash,
                is_erc20,
                is_erc721,
                is_erc1155,
                is_proxy_eip1967,
                is_proxy_minimal,
                uses_push0,
                decoded_at
            FROM (
                SELECT
                    code_hash,
                    language,
                    compiler_version,
                    has_source_hash,
                    is_erc20,
                    is_erc721,
                    is_erc1155,
                    is_proxy_eip1967,
                    is_proxy_minimal,
                    uses_push0,
                    {decoded_at},
                    row_number() OVER (
                        PARTITION BY code_hash
                        ORDER BY {decoded_order}
                    ) AS rn
                FROM bytecode_metadata_by_hash
            )
            WHERE rn = 1;
            "#
        )
    } else {
        r#"
        CREATE OR REPLACE TEMP VIEW decoded_bytecodes AS
        SELECT
            CAST(NULL AS BLOB) AS code_hash,
            CAST(NULL AS VARCHAR) AS code_hash_hex,
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
        WHERE FALSE;
        "#
        .to_string()
    };
    conn.execute_batch(&decoded_sql)
        .context("create decoded bytecodes query view")?;

    let metadata_join = if has_zellic {
        r#"
            LEFT JOIN decoded_bytecodes m ON c.code_hash = m.code_hash
        "#
    } else {
        r#"
            LEFT JOIN bytecode_metadata_current m ON c.contract_address = m.contract_address
        "#
    };
    let is_verified_expr = if table_exists(conn, "verification_registry_imports")? {
        "COALESCE(e.is_verified, false) AS is_verified"
    } else {
        "e.is_verified"
    };
    let contract_metadata_sql = format!(
        r#"
        CREATE OR REPLACE TEMP VIEW contract_metadata_all AS
        SELECT
            c.chain_id,
            c.block_number,
            c.create_index,
            c.contract_address,
            lower('0x' || hex(c.contract_address)) AS address,
            c.transaction_hash,
            lower('0x' || hex(c.transaction_hash)) AS tx_hash,
            c.block_hash,
            lower('0x' || hex(c.block_hash)) AS block_hash_hex,
            c.deployer,
            lower('0x' || hex(c.deployer)) AS deployer_address,
            c.factory,
            lower('0x' || hex(c.factory)) AS factory_address,
            c.code_hash,
            lower('0x' || hex(c.code_hash)) AS code_hash_hex,
            c.n_code_bytes,
            m.language,
            m.compiler_version,
            COALESCE(m.has_source_hash, false) AS has_source_hash,
            COALESCE(m.is_erc20, false) AS is_erc20,
            COALESCE(m.is_erc721, false) AS is_erc721,
            COALESCE(m.is_erc1155, false) AS is_erc1155,
            COALESCE(m.is_proxy_eip1967, false) AS is_proxy_eip1967,
            COALESCE(m.is_proxy_minimal, false) AS is_proxy_minimal,
            COALESCE(m.uses_push0, false) AS uses_push0,
            m.decoded_at,
            {is_verified_expr},
            e.contract_name,
            e.verification_source,
            e.match_type,
            e.checked_at AS verification_checked_at
        FROM contracts c
        {metadata_join}
        LEFT JOIN enrichment_current e
          ON c.contract_address = e.contract_address
         AND c.chain_id = e.chain_id;

        CREATE OR REPLACE TEMP VIEW contract_metadata AS
        SELECT * FROM contract_metadata_all;
        "#
    );
    conn.execute_batch(&contract_metadata_sql)
        .context("create contract metadata query view")?;

    Ok(())
}

fn ensure_zellic_summary_tables(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "zellic_contracts")? {
        return Ok(());
    }

    if !table_exists(conn, "zellic_bytecode_counts")? {
        conn.execute_batch(
            r#"
            CREATE TABLE zellic_bytecode_counts AS
            SELECT
                bytecode_hash AS code_hash,
                COUNT(*)::UBIGINT AS contract_count
            FROM zellic_contracts
            WHERE bytecode_hash IS NOT NULL
            GROUP BY bytecode_hash;
            "#,
        )
        .context("create missing Zellic bytecode counts")?;
    }

    if !table_exists(conn, "zellic_block_counts")? {
        conn.execute_batch(
            r#"
            CREATE TABLE zellic_block_counts AS
            SELECT
                block_number,
                COUNT(*)::UBIGINT AS contract_count
            FROM zellic_contracts
            WHERE block_number IS NOT NULL
            GROUP BY block_number
            ORDER BY block_number;
            "#,
        )
        .context("create missing Zellic block counts")?;
    }

    Ok(())
}

fn backfill_enrichment_blocks(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "enrichment")? || !table_exists(conn, "zellic_contracts")? {
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
            block_number = z.block_number,
            create_index = z.create_index
        FROM zellic_contracts AS z
        WHERE e.contract_address = z.contract_address
          AND (e.block_number IS NULL OR e.create_index IS NULL);
        "#,
    )
    .context("backfill enrichment block positions")?;
    Ok(())
}

impl Db {
    pub fn open_with_mode(data_dir: &Path, contracts_glob: &str, read_only: bool) -> Result<Self> {
        if !read_only {
            std::fs::create_dir_all(data_dir)
                .with_context(|| format!("create data dir {}", data_dir.display()))?;
        }
        let db_path = data_dir.join("blink.duckdb");

        let conn = if read_only {
            // Read-only connection coexists with an active writer since DuckDB
            // only takes an exclusive lock on writers.
            let config = Config::default()
                .access_mode(AccessMode::ReadOnly)
                .context("set read-only access mode")?;
            Connection::open_with_flags(&db_path, config)
                .with_context(|| format!("open duckdb (read-only) {}", db_path.display()))?
        } else {
            Connection::open(&db_path)
                .with_context(|| format!("open duckdb {}", db_path.display()))?
        };

        if read_only {
            // Schema is owned by the writer. If the tables aren't there yet
            // (decode hasn't run), the queries will fail gracefully.
            rebuild_contracts_view_for_conn(&conn, data_dir, contracts_glob)?;
            return Ok(Self {
                inner: Arc::new(Mutex::new(conn)),
                data_dir: data_dir.to_path_buf(),
                contracts_glob: contracts_glob.to_string(),
            });
        }

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS enrichment (
                contract_address BLOB,
                chain_id UBIGINT DEFAULT 1,
                is_verified BOOLEAN NOT NULL,
                contract_name VARCHAR,
                checked_at TIMESTAMP NOT NULL
            );
            -- Track where each verification came from (verifier_alliance).
            -- Added in a later migration; the IF NOT EXISTS guard keeps older
            -- databases working without an explicit migration step.
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS chain_id UBIGINT DEFAULT 1;
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS verification_source VARCHAR;
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS match_type VARCHAR;
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS block_number UINTEGER;
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS create_index UINTEGER;
            UPDATE enrichment SET chain_id = 1 WHERE chain_id IS NULL;
            CREATE INDEX IF NOT EXISTS enrichment_chain_addr_idx ON enrichment(chain_id, contract_address);
            CREATE INDEX IF NOT EXISTS enrichment_verified_idx ON enrichment(is_verified);
            CREATE INDEX IF NOT EXISTS enrichment_source_idx    ON enrichment(verification_source);

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
            ALTER TABLE bytecode_metadata_by_hash
                ADD COLUMN IF NOT EXISTS decoded_at TIMESTAMP;
            -- EIP-1167 minimal proxy detection added later; backfill the column
            -- with `false` on existing rows. DuckDB cannot add constrained
            -- columns to an existing table.
            ALTER TABLE bytecode_metadata_v2
                ADD COLUMN IF NOT EXISTS is_proxy_minimal BOOLEAN;
            ALTER TABLE bytecode_metadata_by_hash
                ADD COLUMN IF NOT EXISTS is_proxy_minimal BOOLEAN;
            UPDATE bytecode_metadata_v2
                SET is_proxy_minimal = false
                WHERE is_proxy_minimal IS NULL;
            UPDATE bytecode_metadata_by_hash
                SET is_proxy_minimal = false
                WHERE is_proxy_minimal IS NULL;
            "#,
        )
        .context("create blink schema")?;
        ensure_zellic_summary_tables(&conn)?;
        backfill_enrichment_blocks(&conn)?;
        let files = list_contract_parquet_files(data_dir, contracts_glob)?;
        ensure_parquet_block_counts(&conn, &files)?;
        rebuild_contracts_view_for_conn(&conn, data_dir, contracts_glob)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
            data_dir: data_dir.to_path_buf(),
            contracts_glob: contracts_glob.to_string(),
        })
    }

    fn rebuild_contracts_view_blocking(&self) -> Result<()> {
        let conn = self.inner.blocking_lock();
        let files = list_contract_parquet_files(&self.data_dir, &self.contracts_glob)?;
        ensure_parquet_block_counts(&conn, &files)?;
        rebuild_contracts_view_for_conn(&conn, &self.data_dir, &self.contracts_glob)
    }

    pub async fn refresh_contracts_view(&self) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.rebuild_contracts_view_blocking())
            .await
            .map_err(|e| anyhow!("join error: {}", e))?
    }
}

fn rebuild_contracts_view_for_conn(
    conn: &Connection,
    data_dir: &Path,
    contracts_glob: &str,
) -> Result<()> {
    let files = list_contract_parquet_files(data_dir, contracts_glob)?;

    // Use TEMP VIEWs so read-only mode (where the main database is locked
    // for writes) can still set this up — temp views live in a session-
    // scoped schema and don't require write access to the on-disk DB.
    let has_zellic =
        table_exists(conn, "zellic_contracts")? && table_exists(conn, "zellic_bytecodes")?;

    let empty_select = r#"
            SELECT
                CAST(NULL AS UINTEGER) AS block_number,
                CAST(NULL AS BLOB) AS block_hash,
                CAST(NULL AS UINTEGER) AS create_index,
                CAST(NULL AS BLOB) AS transaction_hash,
                CAST(NULL AS BLOB) AS contract_address,
                CAST(NULL AS BLOB) AS deployer,
                CAST(NULL AS BLOB) AS factory,
                CAST(NULL AS BLOB) AS init_code,
                CAST(NULL AS BLOB) AS code,
                CAST(NULL AS BLOB) AS init_code_hash,
                CAST(NULL AS UINTEGER) AS n_init_code_bytes,
                CAST(NULL AS UINTEGER) AS n_code_bytes,
                CAST(NULL AS BLOB) AS code_hash,
                CAST(NULL AS UBIGINT) AS chain_id
            WHERE FALSE
        "#;

    let parquet_select = if files.is_empty() {
        None
    } else {
        let list = files
            .iter()
            .map(|p| format!("'{}'", p.display().to_string().replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        Some(format!(
            r#"
                SELECT
                    block_number, block_hash, create_index, transaction_hash,
                    contract_address, deployer, factory, init_code, code,
                    init_code_hash, n_init_code_bytes, n_code_bytes,
                    code_hash, chain_id
                FROM read_parquet([{}], union_by_name = true)
                "#,
            list
        ))
    };

    let zellic_select = if has_zellic {
        Some(
            r#"
                SELECT
                    z.block_number,
                    CAST(NULL AS BLOB) AS block_hash,
                    z.create_index,
                    CAST(NULL AS BLOB) AS transaction_hash,
                    z.contract_address,
                    CAST(NULL AS BLOB) AS deployer,
                    CAST(NULL AS BLOB) AS factory,
                    CAST(NULL AS BLOB) AS init_code,
                    b.code,
                    CAST(NULL AS BLOB) AS init_code_hash,
                    CAST(NULL AS UINTEGER) AS n_init_code_bytes,
                    b.n_code_bytes,
                    z.bytecode_hash AS code_hash,
                    z.chain_id
                FROM zellic_contracts z
                LEFT JOIN zellic_bytecodes b ON z.bytecode_hash = b.code_hash
                "#
            .to_string(),
        )
    } else {
        None
    };

    let parquet_body = parquet_select
        .clone()
        .unwrap_or_else(|| empty_select.to_string());
    let parquet_sql = format!(
        "CREATE OR REPLACE TEMP VIEW parquet_contracts AS\n{};",
        parquet_body
    );
    conn.execute_batch(&parquet_sql)
        .with_context(|| format!("create parquet contracts view ({} files)", files.len()))?;

    let selects = [
        if files.is_empty() {
            None
        } else {
            Some("SELECT * FROM parquet_contracts".to_string())
        },
        zellic_select,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let body = if selects.is_empty() {
        empty_select.to_string()
    } else {
        selects.join("\nUNION ALL\n")
    };
    let sql = format!("CREATE OR REPLACE TEMP VIEW contracts AS\n{};", body);
    conn.execute_batch(&sql)
        .with_context(|| format!("create contracts view ({} files)", files.len()))?;
    create_metadata_current_view(conn)?;
    create_enrichment_current_view(conn)?;
    create_standard_query_views(conn, has_zellic)?;
    Ok(())
}

impl Db {
    pub async fn query_sql(
        &self,
        sql: String,
        limit: u32,
        chain_id: Option<u64>,
    ) -> Result<SqlQueryResult> {
        let inner = self.inner.clone();
        let normalized = normalize_read_only_sql(&sql)?;
        let limit = limit.clamp(1, 1_000);
        tokio::task::spawn_blocking(move || -> Result<SqlQueryResult> {
            let started = Instant::now();
            let wrapped = wrap_dashboard_query(&normalized, limit, chain_id);
            let conn = inner.blocking_lock();
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
                    values.push(value_ref_to_json(row.get_ref(idx)?));
                }
                out.push(values);
            }
            Ok(SqlQueryResult {
                columns,
                row_count: out.len(),
                rows: out,
                limit,
                elapsed_ms: started.elapsed().as_millis(),
            })
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn stats(&self, chain_id: u64) -> Result<Stats> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<Stats> {
            let conn = inner.blocking_lock();
            let (zellic_total, zellic_first, zellic_last): (i64, Option<u32>, Option<u32>) =
                if chain_id == ETHEREUM_CHAIN_ID && table_exists(&conn, "zellic_block_counts")? {
                    conn.query_row(
                        r#"
                        SELECT
                            COALESCE(SUM(contract_count), 0)::BIGINT,
                            MIN(block_number),
                            MAX(block_number)
                        FROM zellic_block_counts
                        "#,
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap_or((0, None, None))
                } else if chain_id == ETHEREUM_CHAIN_ID && table_exists(&conn, "zellic_contracts")?
                {
                    conn.query_row(
                        "SELECT COUNT(*), MIN(block_number), MAX(block_number) FROM zellic_contracts",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap_or((0, None, None))
                } else {
                    (0, None, None)
                };
            let (parquet_total, parquet_first, parquet_last): (i64, Option<u32>, Option<u32>) =
                conn.query_row(
                    "SELECT COUNT(*), MIN(block_number), MAX(block_number) FROM parquet_contracts WHERE chain_id = ?",
                    params![chain_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap_or((0, None, None));

            let total = (zellic_total.max(0) + parquet_total.max(0)) as u64;
            let first_block = [zellic_first, parquet_first]
                .into_iter()
                .flatten()
                .min()
                .unwrap_or(0) as u64;
            let last_block = [zellic_last, parquet_last]
                .into_iter()
                .flatten()
                .max()
                .unwrap_or(0) as u64;

            let registry_loaded = verification_registry_loaded(&conn, chain_id)?;
            let (enriched, verified): (i64, i64) = if registry_loaded {
                let verified_zellic: i64 =
                    if chain_id == ETHEREUM_CHAIN_ID && table_exists(&conn, "zellic_contracts")? {
                    conn.query_row(
                        r#"
                    SELECT COUNT(*)
                    FROM zellic_contracts c
                    JOIN enrichment_current e
                      ON c.contract_address = e.contract_address
                     AND e.chain_id = ?
                    WHERE e.is_verified
                    "#,
                        params![chain_id],
                        |row| row.get(0),
                    )
                    .unwrap_or(0)
                } else {
                    0
                };
                let verified_parquet: i64 = conn
                    .query_row(
                        r#"
                        SELECT COUNT(*)
                        FROM parquet_contracts c
                        JOIN enrichment_current e
                          ON c.contract_address = e.contract_address
                         AND c.chain_id = e.chain_id
                        WHERE e.is_verified
                          AND c.chain_id = ?
                        "#,
                        params![chain_id],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                let verified = verified_zellic + verified_parquet;
                (total as i64, verified)
            } else {
                conn.query_row(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_verified) FROM enrichment_current WHERE chain_id = ?",
                    params![chain_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap_or((0, 0))
            };

            let verified_count = verified.max(0) as u64;
            let enriched_count = enriched.max(0) as u64;
            let unverified_count = enriched_count.saturating_sub(verified_count);
            let verified_pct = if enriched_count == 0 {
                0.0
            } else {
                100.0 * verified_count as f64 / enriched_count as f64
            };
            let enrichment_coverage_pct = if total == 0 {
                0.0
            } else {
                100.0 * enriched_count as f64 / total as f64
            };

            Ok(Stats {
                total_contracts: total,
                verified_count,
                unverified_count,
                verified_pct,
                last_block,
                first_block,
                enrichment_coverage_pct,
                last_updated: Utc::now(),
            })
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn deploys_over_time(
        &self,
        chain_id: u64,
        bucket_blocks: u64,
        block_range: Option<(u64, u64)>,
    ) -> Result<Vec<DeployBucket>> {
        let inner = self.inner.clone();
        let data_dir = self.data_dir.clone();
        let contracts_glob = self.contracts_glob.clone();
        let bucket_blocks = bucket_blocks.max(1);
        tokio::task::spawn_blocking(move || -> Result<Vec<DeployBucket>> {
            let conn = inner.blocking_lock();
            let use_zellic_counts =
                chain_id == ETHEREUM_CHAIN_ID && table_exists(&conn, "zellic_block_counts")?;
            let use_zellic_contracts =
                chain_id == ETHEREUM_CHAIN_ID && table_exists(&conn, "zellic_contracts")?;
            let range_filter = block_range
                .map(|(start, end)| format!(" AND block_number BETWEEN {start} AND {end}"))
                .unwrap_or_default();
            let parquet_select = if table_exists(&conn, "parquet_block_counts")? {
                format!(
                    r#"
                    SELECT
                        (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                        SUM(contract_count)::UBIGINT AS cnt
                    FROM parquet_block_counts
                    WHERE block_number IS NOT NULL
                      AND chain_id = {chain_id}
                      {range_filter}
                    GROUP BY bucket_id
                    "#
                )
            } else {
                let parquet_source = if block_range.is_some() {
                    let files = list_contract_parquet_files(&data_dir, &contracts_glob)?;
                    let files =
                        contract_parquet_files_for_block_range(&files, chain_id, block_range);
                    create_contract_parquet_view(
                        &conn,
                        "dashboard_range_parquet_contracts",
                        &files,
                        chain_id,
                    )?;
                    "dashboard_range_parquet_contracts"
                } else {
                    "parquet_contracts"
                };
                format!(
                    r#"
                    SELECT
                        (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                        COUNT(*)::UBIGINT AS cnt
                    FROM {parquet_source}
                    WHERE block_number IS NOT NULL
                      AND chain_id = {chain_id}
                      {range_filter}
                    GROUP BY bucket_id
                "#
                )
            };
            let zellic_select = if use_zellic_counts {
                Some(format!(
                    r#"
                    SELECT
                        (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                        SUM(contract_count)::UBIGINT AS cnt
                    FROM zellic_block_counts
                    WHERE block_number IS NOT NULL
                      {range_filter}
                    GROUP BY bucket_id
                    "#
                ))
            } else if use_zellic_contracts {
                Some(format!(
                    r#"
                    SELECT
                        (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                        COUNT(*)::UBIGINT AS cnt
                    FROM zellic_contracts
                    WHERE block_number IS NOT NULL
                      {range_filter}
                    GROUP BY bucket_id
                    "#
                ))
            } else {
                None
            };
            let sources = [zellic_select, Some(parquet_select)]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join("\nUNION ALL\n");
            let sql = format!(
                r#"
                WITH bucket_counts AS (
                    {sources}
                )
                SELECT bucket_id, SUM(cnt)::UBIGINT AS cnt
                FROM bucket_counts
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
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn verified_ratio_over_time(
        &self,
        chain_id: u64,
        bucket_blocks: u64,
        block_range: Option<(u64, u64)>,
    ) -> Result<Vec<VerifiedRatioBucket>> {
        let inner = self.inner.clone();
        let data_dir = self.data_dir.clone();
        let contracts_glob = self.contracts_glob.clone();
        let bucket_blocks = bucket_blocks.max(1);
        tokio::task::spawn_blocking(move || -> Result<Vec<VerifiedRatioBucket>> {
            let conn = inner.blocking_lock();
            let mut out = Vec::new();
            let registry_loaded = verification_registry_loaded(&conn, chain_id)?;
            if let Some((start, end)) = block_range {
                let parquet_total_select = if table_exists(&conn, "parquet_block_counts")? {
                    format!(
                        r#"
                        SELECT
                            (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                            SUM(contract_count)::UBIGINT AS total
                        FROM parquet_block_counts
                        WHERE block_number IS NOT NULL
                          AND chain_id = {chain_id}
                          AND block_number BETWEEN {start} AND {end}
                        GROUP BY bucket_id
                        "#
                    )
                } else {
                    let files = list_contract_parquet_files(&data_dir, &contracts_glob)?;
                    let files =
                        contract_parquet_files_for_block_range(&files, chain_id, block_range);
                    create_contract_parquet_view(
                        &conn,
                        "dashboard_range_parquet_contracts",
                        &files,
                        chain_id,
                    )?;
                    format!(
                        r#"
                        SELECT
                            (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                            COUNT(*)::UBIGINT AS total
                        FROM dashboard_range_parquet_contracts
                        WHERE block_number IS NOT NULL
                          AND chain_id = {chain_id}
                          AND block_number BETWEEN {start} AND {end}
                        GROUP BY bucket_id
                        "#
                    )
                };
                let zellic_total_select = if chain_id == ETHEREUM_CHAIN_ID
                    && table_exists(&conn, "zellic_block_counts")?
                {
                    Some(format!(
                        r#"
                        SELECT
                            (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                            SUM(contract_count)::UBIGINT AS total
                        FROM zellic_block_counts
                        WHERE block_number IS NOT NULL
                          AND block_number BETWEEN {start} AND {end}
                        GROUP BY bucket_id
                        "#
                    ))
                } else if chain_id == ETHEREUM_CHAIN_ID && table_exists(&conn, "zellic_contracts")?
                {
                    Some(format!(
                        r#"
                        SELECT
                            (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                            COUNT(*)::UBIGINT AS total
                        FROM zellic_contracts
                        WHERE block_number IS NOT NULL
                          AND chain_id = {chain_id}
                          AND block_number BETWEEN {start} AND {end}
                        GROUP BY bucket_id
                        "#
                    ))
                } else {
                    None
                };
                let total_sources = [zellic_total_select, Some(parquet_total_select)]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join("\nUNION ALL\n");
                let verified_value = "LEAST(COALESCE(checked.verified, 0), totals.total)";
                let (unverified_expr, unknown_expr) = if registry_loaded {
                    (
                        format!("GREATEST(totals.total - {verified_value}, 0)::UBIGINT"),
                        "0::UBIGINT".to_string(),
                    )
                } else {
                    (
                        "COALESCE(checked.unverified, 0)::UBIGINT".to_string(),
                        format!(
                            "GREATEST(totals.total - {verified_value} - COALESCE(checked.unverified, 0), 0)::UBIGINT"
                        ),
                    )
                };
                let sql = format!(
                    r#"
                    WITH total_rows AS (
                        {total_sources}
                    ),
                    totals AS (
                        SELECT bucket_id, SUM(total)::UBIGINT AS total
                        FROM total_rows
                        GROUP BY bucket_id
                    ),
                    checked AS (
                        SELECT
                            (block_number / {bucket_blocks})::UBIGINT AS bucket_id,
                            COUNT(*) FILTER (WHERE is_verified)::UBIGINT AS verified,
                            COUNT(*) FILTER (WHERE is_verified IS FALSE)::UBIGINT AS unverified
                        FROM enrichment_current
                        WHERE block_number IS NOT NULL
                          AND chain_id = {chain_id}
                          AND block_number BETWEEN {start} AND {end}
                        GROUP BY bucket_id
                    )
                    SELECT
                        totals.bucket_id,
                        {verified_value}::UBIGINT AS verified,
                        {unverified_expr} AS unverified,
                        {unknown_expr} AS unknown
                    FROM totals
                    LEFT JOIN checked ON totals.bucket_id = checked.bucket_id
                    ORDER BY totals.bucket_id
                    "#
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map([], read_verified_bucket_row)?;
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
                return Ok(out);
            }
            if chain_id == ETHEREUM_CHAIN_ID && table_exists(&conn, "zellic_contracts")? {
                if !table_exists(&conn, "zellic_block_counts")? {
                    return Ok(out);
                }

                let sql = match (registry_loaded, block_range.is_some()) {
                    (true, false) => r#"
                    WITH totals AS (
                        SELECT
                            (block_number / ?)::UBIGINT AS bucket_id,
                            SUM(contract_count)::UBIGINT AS total
                        FROM zellic_block_counts
                        WHERE block_number IS NOT NULL
                        GROUP BY bucket_id
                    ),
                    checked AS (
                        SELECT
                            (z.block_number / ?)::UBIGINT AS bucket_id,
                            COUNT(*)::UBIGINT AS verified
                        FROM zellic_contracts z
                        JOIN enrichment_current e
                          ON z.contract_address = e.contract_address
                         AND e.chain_id = ?
                        WHERE z.block_number IS NOT NULL
                          AND e.is_verified
                        GROUP BY bucket_id
                    )
                    SELECT
                        totals.bucket_id,
                        COALESCE(checked.verified, 0)::UBIGINT AS verified,
                        GREATEST(totals.total - COALESCE(checked.verified, 0), 0)::UBIGINT AS unverified,
                        0::UBIGINT AS unknown
                    FROM totals
                    LEFT JOIN checked ON totals.bucket_id = checked.bucket_id
                    ORDER BY totals.bucket_id
                    "#,
                    (true, true) => r#"
                    WITH totals AS (
                        SELECT
                            (block_number / ?)::UBIGINT AS bucket_id,
                            SUM(contract_count)::UBIGINT AS total
                        FROM zellic_block_counts
                        WHERE block_number IS NOT NULL
                          AND block_number BETWEEN ? AND ?
                        GROUP BY bucket_id
                    ),
                    checked AS (
                        SELECT
                            (z.block_number / ?)::UBIGINT AS bucket_id,
                            COUNT(*)::UBIGINT AS verified
                        FROM zellic_contracts z
                        JOIN enrichment_current e
                          ON z.contract_address = e.contract_address
                         AND e.chain_id = ?
                        WHERE z.block_number IS NOT NULL
                          AND z.block_number BETWEEN ? AND ?
                          AND e.is_verified
                        GROUP BY bucket_id
                    )
                    SELECT
                        totals.bucket_id,
                        COALESCE(checked.verified, 0)::UBIGINT AS verified,
                        GREATEST(totals.total - COALESCE(checked.verified, 0), 0)::UBIGINT AS unverified,
                        0::UBIGINT AS unknown
                    FROM totals
                    LEFT JOIN checked ON totals.bucket_id = checked.bucket_id
                    ORDER BY totals.bucket_id
                    "#,
                    (false, false) => r#"
                    WITH totals AS (
                        SELECT
                            (block_number / ?)::UBIGINT AS bucket_id,
                            SUM(contract_count)::UBIGINT AS total
                        FROM zellic_block_counts
                        WHERE block_number IS NOT NULL
                        GROUP BY bucket_id
                    ),
                    checked AS (
                        SELECT
                            (block_number / ?)::UBIGINT AS bucket_id,
                            COUNT(*) FILTER (WHERE is_verified)::UBIGINT AS verified,
                            COUNT(*) FILTER (WHERE is_verified IS FALSE)::UBIGINT AS unverified
                        FROM enrichment_current
                        WHERE block_number IS NOT NULL
                          AND chain_id = ?
                        GROUP BY bucket_id
                    )
                    SELECT
                        totals.bucket_id,
                        COALESCE(checked.verified, 0)::UBIGINT AS verified,
                        COALESCE(checked.unverified, 0)::UBIGINT AS unverified,
                        GREATEST(
                            totals.total
                              - COALESCE(checked.verified, 0)
                              - COALESCE(checked.unverified, 0),
                            0
                        )::UBIGINT AS unknown
                    FROM totals
                    LEFT JOIN checked ON totals.bucket_id = checked.bucket_id
                    ORDER BY totals.bucket_id
                    "#,
                    (false, true) => r#"
                    WITH totals AS (
                        SELECT
                            (block_number / ?)::UBIGINT AS bucket_id,
                            SUM(contract_count)::UBIGINT AS total
                        FROM zellic_block_counts
                        WHERE block_number IS NOT NULL
                          AND block_number BETWEEN ? AND ?
                        GROUP BY bucket_id
                    ),
                    checked AS (
                        SELECT
                            (block_number / ?)::UBIGINT AS bucket_id,
                            COUNT(*) FILTER (WHERE is_verified)::UBIGINT AS verified,
                            COUNT(*) FILTER (WHERE is_verified IS FALSE)::UBIGINT AS unverified
                        FROM enrichment_current
                        WHERE block_number IS NOT NULL
                          AND chain_id = ?
                          AND block_number BETWEEN ? AND ?
                        GROUP BY bucket_id
                    )
                    SELECT
                        totals.bucket_id,
                        COALESCE(checked.verified, 0)::UBIGINT AS verified,
                        COALESCE(checked.unverified, 0)::UBIGINT AS unverified,
                        GREATEST(
                            totals.total
                              - COALESCE(checked.verified, 0)
                              - COALESCE(checked.unverified, 0),
                            0
                        )::UBIGINT AS unknown
                    FROM totals
                    LEFT JOIN checked ON totals.bucket_id = checked.bucket_id
                    ORDER BY totals.bucket_id
                    "#,
                };
                let mut stmt = conn.prepare(sql)?;
                let rows = match block_range {
                    None => stmt.query_map(
                        params![bucket_blocks, bucket_blocks, chain_id],
                        read_verified_bucket_row,
                    )?,
                    Some((start, end)) => stmt.query_map(
                        params![bucket_blocks, start, end, bucket_blocks, chain_id, start, end],
                        read_verified_bucket_row,
                    )?,
                };
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
                return Ok(out);
            }

            let sql = match (registry_loaded, block_range.is_some()) {
                (true, false) => r#"
                SELECT
                    (c.block_number / ?)::UBIGINT AS bucket_id,
                    COUNT(*) FILTER (WHERE e.is_verified)::UBIGINT AS verified,
                    COUNT(*) FILTER (WHERE e.is_verified IS NULL OR e.is_verified IS FALSE)::UBIGINT AS unverified,
                    0::UBIGINT AS unknown
                FROM contracts c
                LEFT JOIN enrichment_current e
                  ON c.contract_address = e.contract_address
                 AND c.chain_id = e.chain_id
                WHERE c.block_number IS NOT NULL
                  AND c.chain_id = ?
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#,
                (true, true) => r#"
                SELECT
                    (c.block_number / ?)::UBIGINT AS bucket_id,
                    COUNT(*) FILTER (WHERE e.is_verified)::UBIGINT AS verified,
                    COUNT(*) FILTER (WHERE e.is_verified IS NULL OR e.is_verified IS FALSE)::UBIGINT AS unverified,
                    0::UBIGINT AS unknown
                FROM contracts c
                LEFT JOIN enrichment_current e
                  ON c.contract_address = e.contract_address
                 AND c.chain_id = e.chain_id
                WHERE c.block_number IS NOT NULL
                  AND c.chain_id = ?
                  AND c.block_number BETWEEN ? AND ?
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#,
                (false, false) => r#"
                SELECT
                    (c.block_number / ?)::UBIGINT AS bucket_id,
                    COUNT(*) FILTER (WHERE e.is_verified)::UBIGINT AS verified,
                    COUNT(*) FILTER (WHERE e.is_verified IS FALSE)::UBIGINT AS unverified,
                    COUNT(*) FILTER (WHERE e.is_verified IS NULL)::UBIGINT AS unknown
                FROM contracts c
                LEFT JOIN enrichment_current e
                  ON c.contract_address = e.contract_address
                 AND c.chain_id = e.chain_id
                WHERE c.block_number IS NOT NULL
                  AND c.chain_id = ?
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#,
                (false, true) => r#"
                SELECT
                    (c.block_number / ?)::UBIGINT AS bucket_id,
                    COUNT(*) FILTER (WHERE e.is_verified)::UBIGINT AS verified,
                    COUNT(*) FILTER (WHERE e.is_verified IS FALSE)::UBIGINT AS unverified,
                    COUNT(*) FILTER (WHERE e.is_verified IS NULL)::UBIGINT AS unknown
                FROM contracts c
                LEFT JOIN enrichment_current e
                  ON c.contract_address = e.contract_address
                 AND c.chain_id = e.chain_id
                WHERE c.block_number IS NOT NULL
                  AND c.chain_id = ?
                  AND c.block_number BETWEEN ? AND ?
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#,
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = match block_range {
                None => stmt.query_map(
                    params![bucket_blocks, chain_id],
                    read_verified_bucket_row,
                )?,
                Some((start, end)) => stmt.query_map(
                    params![bucket_blocks, chain_id, start, end],
                    read_verified_bucket_row,
                )?,
            };
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
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn bytecode_size_distribution(&self, chain_id: u64) -> Result<Vec<SizeBin>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<SizeBin>> {
            let conn = inner.blocking_lock();

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
            let use_zellic = chain_id == ETHEREUM_CHAIN_ID
                && table_exists(&conn, "zellic_bytecodes")?
                && table_exists(&conn, "zellic_bytecode_counts")?;
            let sql = if use_zellic {
                r#"
                SELECT
                    CASE
                        WHEN b.n_code_bytes = 0 THEN 0
                        WHEN b.n_code_bytes <= 32 THEN 1
                        WHEN b.n_code_bytes <= 44 THEN 2
                        WHEN b.n_code_bytes = 45 AND COALESCE(m.is_proxy_minimal, false) THEN 3
                        WHEN b.n_code_bytes = 45 THEN 4
                        WHEN b.n_code_bytes <= 64 THEN 5
                        WHEN b.n_code_bytes <= 256 THEN 6
                        WHEN b.n_code_bytes <= 1024 THEN 7
                        WHEN b.n_code_bytes <= 4096 THEN 8
                        WHEN b.n_code_bytes <= 8192 THEN 9
                        WHEN b.n_code_bytes <= 16384 THEN 10
                        ELSE 11
                    END AS bin_id,
                    SUM(c.contract_count)::UBIGINT AS cnt
                FROM zellic_bytecodes b
                JOIN zellic_bytecode_counts c ON b.code_hash = c.code_hash
                LEFT JOIN bytecode_metadata_by_hash m ON b.code_hash = m.code_hash
                WHERE b.n_code_bytes IS NOT NULL
                  AND b.n_code_bytes <= 24576
                GROUP BY bin_id
                ORDER BY bin_id
                "#
                .to_string()
            } else {
                r#"
                SELECT
                    CASE
                        WHEN c.n_code_bytes = 0 THEN 0
                        WHEN c.n_code_bytes <= 32 THEN 1
                        WHEN c.n_code_bytes <= 44 THEN 2
                        WHEN c.n_code_bytes = 45 AND COALESCE(m.is_proxy_minimal, false) THEN 3
                        WHEN c.n_code_bytes = 45 THEN 4
                        WHEN c.n_code_bytes <= 64 THEN 5
                        WHEN c.n_code_bytes <= 256 THEN 6
                        WHEN c.n_code_bytes <= 1024 THEN 7
                        WHEN c.n_code_bytes <= 4096 THEN 8
                        WHEN c.n_code_bytes <= 8192 THEN 9
                        WHEN c.n_code_bytes <= 16384 THEN 10
                        ELSE 11
                    END AS bin_id,
                    COUNT(*)::UBIGINT AS cnt
                FROM contracts c
                LEFT JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                WHERE c.n_code_bytes IS NOT NULL
                  AND c.n_code_bytes <= 24576
                  AND c.chain_id = ?
                GROUP BY bin_id
                ORDER BY bin_id
                "#
                .to_string()
            };
            let mut stmt = conn.prepare(&sql)?;
            let rows = if use_zellic {
                stmt.query_map([], read_u64_pair)?
            } else {
                stmt.query_map(params![chain_id], read_u64_pair)?
            };
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
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn top_compilers(&self, chain_id: u64, limit: u32) -> Result<Vec<CompilerCount>> {
        let inner = self.inner.clone();
        let limit = limit.clamp(1, 50);
        tokio::task::spawn_blocking(move || -> Result<Vec<CompilerCount>> {
            let conn = inner.blocking_lock();
            // Compiler distribution is bytecode-derived. Verification sources
            // can confirm source publication, but local decode remains the
            // source of truth for compiler metadata in this dashboard.
            let use_zellic = chain_id == ETHEREUM_CHAIN_ID
                && table_exists(&conn, "zellic_bytecode_counts")?
                && table_exists(&conn, "bytecode_metadata_by_hash")?;
            let sql = if use_zellic {
                r#"
                SELECT m.compiler_version, SUM(c.contract_count)::UBIGINT AS cnt
                FROM bytecode_metadata_by_hash m
                JOIN zellic_bytecode_counts c ON m.code_hash = c.code_hash
                WHERE m.compiler_version IS NOT NULL
                GROUP BY m.compiler_version
                ORDER BY cnt DESC
                LIMIT ?
                "#
            } else {
                r#"
                WITH counts AS (
                    SELECT code_hash, COUNT(*)::UBIGINT AS contract_count
                    FROM contracts
                    WHERE chain_id = ?
                      AND code_hash IS NOT NULL
                    GROUP BY code_hash
                )
                SELECT m.compiler_version, SUM(c.contract_count)::UBIGINT AS cnt
                FROM counts c
                JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                WHERE m.compiler_version IS NOT NULL
                GROUP BY m.compiler_version
                ORDER BY cnt DESC
                LIMIT ?
                "#
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = if use_zellic {
                stmt.query_map(params![limit as i64], read_string_u64_pair)?
            } else {
                stmt.query_map(params![chain_id, limit as i64], read_string_u64_pair)?
            };
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
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn compiler_version_total(&self, chain_id: u64) -> Result<u64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<u64> {
            let conn = inner.blocking_lock();
            if chain_id == ETHEREUM_CHAIN_ID
                && table_exists(&conn, "zellic_bytecode_counts")?
                && table_exists(&conn, "bytecode_metadata_by_hash")?
            {
                let count: i64 = conn
                    .query_row(
                        r#"
                        SELECT COALESCE(SUM(c.contract_count), 0)::BIGINT
                        FROM bytecode_metadata_by_hash m
                        JOIN zellic_bytecode_counts c ON m.code_hash = c.code_hash
                        WHERE m.compiler_version IS NOT NULL
                        "#,
                        [],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                return Ok(count.max(0) as u64);
            }
            let count: i64 = conn
                .query_row(
                    r#"
                    WITH counts AS (
                        SELECT code_hash, COUNT(*)::UBIGINT AS contract_count
                        FROM contracts
                        WHERE chain_id = ?
                          AND code_hash IS NOT NULL
                        GROUP BY code_hash
                    )
                    SELECT COALESCE(SUM(c.contract_count), 0)::BIGINT
                    FROM counts c
                    JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                    WHERE m.compiler_version IS NOT NULL
                    "#,
                    params![chain_id],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            Ok(count.max(0) as u64)
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn language_distribution(&self, chain_id: u64) -> Result<Vec<LanguageCount>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<LanguageCount>> {
            let conn = inner.blocking_lock();
            let use_zellic = chain_id == ETHEREUM_CHAIN_ID
                && table_exists(&conn, "zellic_bytecode_counts")?
                && table_exists(&conn, "bytecode_metadata_by_hash")?;
            let sql = if use_zellic {
                r#"
                SELECT COALESCE(m.language, 'unknown') AS lang,
                       SUM(c.contract_count)::UBIGINT AS cnt
                FROM zellic_bytecode_counts c
                LEFT JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                GROUP BY lang
                ORDER BY cnt DESC
                "#
            } else {
                r#"
                WITH counts AS (
                    SELECT code_hash, COUNT(*)::UBIGINT AS contract_count
                    FROM contracts
                    WHERE chain_id = ?
                      AND code_hash IS NOT NULL
                    GROUP BY code_hash
                )
                SELECT COALESCE(m.language, 'unknown') AS lang,
                       SUM(c.contract_count)::UBIGINT AS cnt
                FROM counts c
                LEFT JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                GROUP BY lang
                ORDER BY cnt DESC
                "#
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = if use_zellic {
                stmt.query_map([], read_string_u64_pair)?
            } else {
                stmt.query_map(params![chain_id], read_string_u64_pair)?
            };
            let mut out = Vec::new();
            for r in rows {
                let (language, count) = r?;
                out.push(LanguageCount { language, count });
            }
            Ok(out)
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn standards_breakdown(&self, chain_id: u64) -> Result<StandardsBreakdown> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<StandardsBreakdown> {
            let conn = inner.blocking_lock();
            if chain_id == ETHEREUM_CHAIN_ID
                && table_exists(&conn, "zellic_bytecode_counts")?
                && table_exists(&conn, "bytecode_metadata_by_hash")?
            {
                return conn
                    .query_row(
                        r#"
                        SELECT
                            COALESCE(SUM(CASE WHEN m.is_erc20 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                            COALESCE(SUM(CASE WHEN m.is_erc721 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                            COALESCE(SUM(CASE WHEN m.is_erc1155 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                            COALESCE(SUM(CASE WHEN m.is_proxy_eip1967 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                            COALESCE(SUM(CASE WHEN m.is_proxy_minimal THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                            COALESCE(SUM(CASE WHEN m.uses_push0 THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                            COALESCE(SUM(CASE WHEN m.has_source_hash THEN c.contract_count ELSE 0 END), 0)::UBIGINT,
                            COALESCE(SUM(CASE WHEN m.code_hash IS NOT NULL THEN c.contract_count ELSE 0 END), 0)::UBIGINT
                        FROM zellic_bytecode_counts c
                        LEFT JOIN bytecode_metadata_by_hash m ON c.code_hash = m.code_hash
                        "#,
                        [],
                        |row| {
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
                        },
                    )
                    .or_else(|_| {
                        Ok(StandardsBreakdown {
                            erc20: 0,
                            erc721: 0,
                            erc1155: 0,
                            proxy_eip1967: 0,
                            proxy_minimal: 0,
                            uses_push0: 0,
                            has_source_hash: 0,
                            total_decoded: 0,
                        })
                    });
            }
            conn.query_row(
                r#"
                WITH counts AS (
                    SELECT code_hash, COUNT(*)::UBIGINT AS contract_count
                    FROM contracts
                    WHERE chain_id = ?
                      AND code_hash IS NOT NULL
                    GROUP BY code_hash
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
                "#,
                params![chain_id],
                |row| {
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
                },
            )
            .or_else(|_| {
                Ok(StandardsBreakdown {
                    erc20: 0,
                    erc721: 0,
                    erc1155: 0,
                    proxy_eip1967: 0,
                    proxy_minimal: 0,
                    uses_push0: 0,
                    has_source_hash: 0,
                    total_decoded: 0,
                })
            })
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn recent_contracts(
        &self,
        chain_id: u64,
        limit: u32,
        cursor: Option<RecentCursor>,
    ) -> Result<RecentPage> {
        let inner = self.inner.clone();
        let data_dir = self.data_dir.clone();
        let contracts_glob = self.contracts_glob.clone();
        let limit = limit.clamp(1, 200);
        let (mut contracts, may_have_more_external) =
            tokio::task::spawn_blocking(move || -> Result<(Vec<RecentContract>, bool)> {
                let conn = inner.blocking_lock();
                let page_limit = limit as i64 + 1;
                let has_zellic = chain_id == ETHEREUM_CHAIN_ID
                    && table_exists(&conn, "zellic_contracts")?
                    && table_exists(&conn, "zellic_bytecodes")?;
                let has_hash_metadata = table_exists(&conn, "bytecode_metadata_by_hash")?;
                let parquet_files = list_contract_parquet_files(&data_dir, &contracts_glob)?;
                let max_contracts_block =
                    max_contract_file_block_for_chain(&parquet_files, chain_id);
                let max_zellic_block: Option<u64> = if chain_id == ETHEREUM_CHAIN_ID && has_zellic {
                    let block: Option<u32> = conn
                        .query_row(
                            "SELECT MAX(block_number) FROM zellic_contracts",
                            [],
                            |row| row.get(0),
                        )
                        .unwrap_or(None);
                    block.map(u64::from)
                } else {
                    None
                };
                let has_external_contracts = max_contracts_block > max_zellic_block;
                let use_external_recent = has_external_contracts
                    && cursor
                        .map(|cursor| cursor.block_number > max_zellic_block.unwrap_or(0))
                        .unwrap_or(true);
                if use_external_recent {
                    let recent_files =
                        recent_contract_parquet_files(&parquet_files, chain_id, cursor);
                    create_recent_parquet_contracts_view(&conn, &recent_files, chain_id)?;
                }
                conn.execute_batch(&format!(
                    r#"
                CREATE OR REPLACE TEMP VIEW recent_enrichment AS
                SELECT *
                FROM enrichment_current
                WHERE chain_id = {};
                "#,
                    chain_id
                ))
                .context("create recent enrichment view")?;
                conn.execute_batch(&format!(
                    r#"
                CREATE OR REPLACE TEMP VIEW recent_contracts_all AS
                SELECT *
                FROM contracts
                WHERE chain_id = {};
                "#,
                    chain_id
                ))
                .context("create recent contracts view")?;
                let registry_loaded = verification_registry_loaded(&conn, chain_id)?;

                // For Zellic imports, compiler metadata is keyed by bytecode hash.
                // Select the tiny recent page first, then join hash metadata; going
                // through bytecode_metadata_current would expand hash metadata back
                // to tens of millions of contract-address rows for every page.
                let sql = if use_external_recent && has_hash_metadata && cursor.is_some() {
                    r#"
                WITH page AS (
                    SELECT
                        c.contract_address,
                        c.block_number,
                        c.create_index,
                        c.deployer,
                        c.n_code_bytes,
                        c.code_hash
                    FROM recent_parquet_contracts c
                    WHERE c.block_number IS NOT NULL
                      AND (
                          c.block_number < ?
                          OR (c.block_number = ? AND c.create_index < ?)
                      )
                    ORDER BY c.block_number DESC, c.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    page.deployer,
                    page.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    h.compiler_version
                FROM page
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                LEFT JOIN bytecode_metadata_by_hash h ON page.code_hash = h.code_hash
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else if use_external_recent && has_hash_metadata {
                    r#"
                WITH page AS (
                    SELECT
                        c.contract_address,
                        c.block_number,
                        c.create_index,
                        c.deployer,
                        c.n_code_bytes,
                        c.code_hash
                    FROM recent_parquet_contracts c
                    WHERE c.block_number IS NOT NULL
                    ORDER BY c.block_number DESC, c.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    page.deployer,
                    page.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    h.compiler_version
                FROM page
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                LEFT JOIN bytecode_metadata_by_hash h ON page.code_hash = h.code_hash
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else if use_external_recent && cursor.is_some() {
                    r#"
                WITH page AS (
                    SELECT
                        c.contract_address,
                        c.block_number,
                        c.create_index,
                        c.deployer,
                        c.n_code_bytes
                    FROM recent_parquet_contracts c
                    WHERE c.block_number IS NOT NULL
                      AND (
                          c.block_number < ?
                          OR (c.block_number = ? AND c.create_index < ?)
                      )
                    ORDER BY c.block_number DESC, c.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    page.deployer,
                    page.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    CAST(NULL AS VARCHAR) AS compiler_version
                FROM page
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else if use_external_recent {
                    r#"
                WITH page AS (
                    SELECT
                        c.contract_address,
                        c.block_number,
                        c.create_index,
                        c.deployer,
                        c.n_code_bytes
                    FROM recent_parquet_contracts c
                    WHERE c.block_number IS NOT NULL
                    ORDER BY c.block_number DESC, c.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    page.deployer,
                    page.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    CAST(NULL AS VARCHAR) AS compiler_version
                FROM page
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else if has_zellic && has_hash_metadata && cursor.is_some() {
                    r#"
                WITH page AS (
                    SELECT
                        z.contract_address,
                        z.block_number,
                        z.create_index,
                        z.bytecode_hash
                    FROM zellic_contracts z
                    WHERE z.block_number IS NOT NULL
                      AND (
                          z.block_number < ?
                          OR (z.block_number = ? AND z.create_index < ?)
                      )
                    ORDER BY z.block_number DESC, z.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    CAST(NULL AS BLOB) AS deployer,
                    b.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    h.compiler_version
                FROM page
                LEFT JOIN zellic_bytecodes b ON page.bytecode_hash = b.code_hash
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                LEFT JOIN bytecode_metadata_by_hash h ON page.bytecode_hash = h.code_hash
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else if has_zellic && has_hash_metadata {
                    r#"
                WITH page AS (
                    SELECT
                        z.contract_address,
                        z.block_number,
                        z.create_index,
                        z.bytecode_hash
                    FROM zellic_contracts z
                    WHERE z.block_number IS NOT NULL
                    ORDER BY z.block_number DESC, z.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    CAST(NULL AS BLOB) AS deployer,
                    b.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    h.compiler_version
                FROM page
                LEFT JOIN zellic_bytecodes b ON page.bytecode_hash = b.code_hash
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                LEFT JOIN bytecode_metadata_by_hash h ON page.bytecode_hash = h.code_hash
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else if has_zellic && cursor.is_some() {
                    r#"
                WITH page AS (
                    SELECT
                        z.contract_address,
                        z.block_number,
                        z.create_index,
                        z.bytecode_hash
                    FROM zellic_contracts z
                    WHERE z.block_number IS NOT NULL
                      AND (
                          z.block_number < ?
                          OR (z.block_number = ? AND z.create_index < ?)
                      )
                    ORDER BY z.block_number DESC, z.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    CAST(NULL AS BLOB) AS deployer,
                    b.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    CAST(NULL AS VARCHAR) AS compiler_version
                FROM page
                LEFT JOIN zellic_bytecodes b ON page.bytecode_hash = b.code_hash
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else if has_zellic {
                    r#"
                WITH page AS (
                    SELECT
                        z.contract_address,
                        z.block_number,
                        z.create_index,
                        z.bytecode_hash
                    FROM zellic_contracts z
                    WHERE z.block_number IS NOT NULL
                    ORDER BY z.block_number DESC, z.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    CAST(NULL AS BLOB) AS deployer,
                    b.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    CAST(NULL AS VARCHAR) AS compiler_version
                FROM page
                LEFT JOIN zellic_bytecodes b ON page.bytecode_hash = b.code_hash
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else if cursor.is_some() {
                    r#"
                WITH page AS (
                    SELECT
                        c.contract_address,
                        c.block_number,
                        c.create_index,
                        c.deployer,
                        c.n_code_bytes
                    FROM recent_contracts_all c
                    WHERE c.block_number IS NOT NULL
                      AND (
                          c.block_number < ?
                          OR (c.block_number = ? AND c.create_index < ?)
                      )
                    ORDER BY c.block_number DESC, c.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    page.deployer,
                    page.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    b.compiler_version
                FROM page
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                LEFT JOIN bytecode_metadata_current b ON page.contract_address = b.contract_address
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                } else {
                    r#"
                WITH page AS (
                    SELECT
                        c.contract_address,
                        c.block_number,
                        c.create_index,
                        c.deployer,
                        c.n_code_bytes
                    FROM recent_contracts_all c
                    WHERE c.block_number IS NOT NULL
                    ORDER BY c.block_number DESC, c.create_index DESC
                    LIMIT ?
                )
                SELECT
                    page.contract_address,
                    page.block_number,
                    page.create_index,
                    page.deployer,
                    page.n_code_bytes,
                    e.is_verified,
                    e.contract_name,
                    b.compiler_version
                FROM page
                LEFT JOIN recent_enrichment e ON page.contract_address = e.contract_address
                LEFT JOIN bytecode_metadata_current b ON page.contract_address = b.contract_address
                ORDER BY page.block_number DESC, page.create_index DESC
                "#
                };
                let mut stmt = conn.prepare(sql)?;
                let rows = if let Some(cursor) = cursor {
                    stmt.query_map(
                        params![
                            cursor.block_number as i64,
                            cursor.block_number as i64,
                            cursor.create_index as i64,
                            page_limit
                        ],
                        read_recent_row,
                    )?
                } else {
                    stmt.query_map(params![page_limit], read_recent_row)?
                };
                let mut out = Vec::new();
                for r in rows {
                    let (addr, block, create_index, deployer, n_code, verified, name, compiler) =
                        r?;
                    let block_u64 = block as u64;
                    let is_verified = if registry_loaded {
                        Some(verified.unwrap_or(false))
                    } else {
                        verified
                    };
                    out.push(RecentContract {
                        address: format!("0x{}", hex::encode(&addr)),
                        block_number: block_u64,
                        create_index: create_index as u64,
                        timestamp: crate::blocks::block_timestamp(chain_id, block_u64),
                        deployer: format!("0x{}", hex::encode(deployer.unwrap_or_default())),
                        n_code_bytes: n_code.unwrap_or(0) as u64,
                        is_verified,
                        contract_name: name,
                        compiler_version: compiler,
                    });
                }
                let last_block = out.last().map(|contract| contract.block_number);
                let has_older_external = use_external_recent
                    && last_block
                        .map(|block| {
                            parquet_files
                                .iter()
                                .cloned()
                                .map(contract_file_with_range)
                                .filter(|file| {
                                    file.chain_id == Some(chain_id)
                                        || (file.chain_id.is_none()
                                            && chain_id == ETHEREUM_CHAIN_ID)
                                })
                                .filter_map(|file| file.end_block)
                                .any(|end_block| end_block < block)
                        })
                        .unwrap_or(false);
                let has_older_zellic = use_external_recent
                    && last_block
                        .zip(max_zellic_block)
                        .map(|(block, zellic_block)| zellic_block < block)
                        .unwrap_or(false);
                Ok((out, has_older_external || has_older_zellic))
            })
            .await
            .map_err(|e| anyhow!("join error: {}", e))??;
        let has_more =
            contracts.len() > limit as usize || (may_have_more_external && !contracts.is_empty());
        if has_more {
            contracts.truncate(limit as usize);
        }
        Ok(RecentPage {
            contracts,
            has_more,
        })
    }

    pub async fn highest_block(&self, chain_id: u64) -> Result<Option<u64>> {
        let inner = self.inner.clone();
        let data_dir = self.data_dir.clone();
        let contracts_glob = self.contracts_glob.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<u64>> {
            let parquet_files = list_contract_parquet_files(&data_dir, &contracts_glob)?;
            let parquet_filename_max = max_contract_file_block_for_chain(&parquet_files, chain_id);
            let conn = inner.blocking_lock();
            let zellic_max =
                if chain_id == ETHEREUM_CHAIN_ID && table_exists(&conn, "zellic_contracts")? {
                    let block: Option<u32> = conn
                        .query_row(
                            "SELECT MAX(block_number) FROM zellic_contracts",
                            [],
                            |row| row.get(0),
                        )
                        .unwrap_or(None);
                    block.map(u64::from)
                } else {
                    None
                };
            let parquet_view_max: Option<u32> = conn
                .query_row(
                    "SELECT MAX(block_number) FROM parquet_contracts WHERE chain_id = ?",
                    params![chain_id],
                    |row| row.get(0),
                )
                .unwrap_or(None);

            let contracts_view_max: Option<u32> = conn
                .query_row(
                    "SELECT MAX(block_number) FROM contracts WHERE chain_id = ?",
                    params![chain_id],
                    |row| row.get(0),
                )
                .unwrap_or(None);
            Ok([
                parquet_filename_max,
                parquet_view_max.map(u64::from),
                contracts_view_max.map(u64::from),
                zellic_max,
            ]
            .into_iter()
            .flatten()
            .max())
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;

    use crate::chains::ETHEREUM_CHAIN_ID;

    use super::{
        max_contract_file_block_for_chain, recent_contract_parquet_files, Db, RecentCursor,
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
                "blink_db_test_{}_{}_{}",
                std::process::id(),
                name,
                unique
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn make_bytes(value: u8, len: usize) -> Vec<u8> {
        vec![value; len]
    }

    fn insert_zellic_snapshot(data_dir: &Path) {
        let conn = Connection::open(data_dir.join("blink.duckdb")).unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE zellic_bytecodes (
                code_hash BLOB,
                code BLOB,
                n_code_bytes UINTEGER
            );
            CREATE TABLE zellic_contracts (
                contract_address BLOB,
                bytecode_hash BLOB,
                block_number UINTEGER,
                create_index UINTEGER,
                chain_id UBIGINT
            );
            "#,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO zellic_bytecodes VALUES (?, ?, ?)",
            duckdb::params![make_bytes(1, 32), make_bytes(0x60, 4), 4u32],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO zellic_contracts VALUES (?, ?, ?, ?, ?)",
            duckdb::params![make_bytes(2, 20), make_bytes(1, 32), 100u32, 0u32, 1u64],
        )
        .unwrap();
    }

    fn write_backfill_parquet(data_dir: &Path) {
        let ethereum_path = data_dir.join("contracts__0000000200__0000000200.parquet");
        let ethereum_path_sql = ethereum_path.display().to_string().replace('\'', "''");
        let gnosis_path =
            data_dir.join("contracts__chain_0000000100__0000000300__0000000300.parquet");
        let gnosis_path_sql = gnosis_path.display().to_string().replace('\'', "''");
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            r#"
            COPY (
                SELECT
                    200::UINTEGER AS block_number,
                    unhex(repeat('03', 32)) AS block_hash,
                    0::UINTEGER AS create_index,
                    unhex(repeat('04', 32)) AS transaction_hash,
                    unhex(repeat('05', 20)) AS contract_address,
                    unhex(repeat('06', 20)) AS deployer,
                    unhex(repeat('07', 20)) AS factory,
                    unhex('6000') AS init_code,
                    unhex('6001') AS code,
                    unhex(repeat('08', 32)) AS init_code_hash,
                    2::UINTEGER AS n_init_code_bytes,
                    2::UINTEGER AS n_code_bytes,
                    unhex(repeat('09', 32)) AS code_hash,
                    1::UBIGINT AS chain_id
            ) TO '{ethereum_path_sql}' (FORMAT PARQUET);

            COPY (
                SELECT
                    300::UINTEGER AS block_number,
                    unhex(repeat('13', 32)) AS block_hash,
                    0::UINTEGER AS create_index,
                    unhex(repeat('14', 32)) AS transaction_hash,
                    unhex(repeat('15', 20)) AS contract_address,
                    unhex(repeat('16', 20)) AS deployer,
                    unhex(repeat('17', 20)) AS factory,
                    unhex('6000') AS init_code,
                    unhex('6001') AS code,
                    unhex(repeat('18', 32)) AS init_code_hash,
                    2::UINTEGER AS n_init_code_bytes,
                    2::UINTEGER AS n_code_bytes,
                    unhex(repeat('19', 32)) AS code_hash,
                    100::UBIGINT AS chain_id
            ) TO '{gnosis_path_sql}' (FORMAT PARQUET);
            "#
        ))
        .unwrap();
    }

    #[test]
    fn recent_parquet_selection_uses_filename_block_ranges() {
        let files = vec![
            PathBuf::from("/tmp/contracts__0021850001__0021950000.parquet"),
            PathBuf::from("/tmp/tail__chain_0000000001__0025330032__0025330040.parquet"),
            PathBuf::from("/tmp/tail__chain_0000000001__0025330041__0025330048.parquet"),
            PathBuf::from("/tmp/tail__chain_0000000100__0040000001__0040000048.parquet"),
        ];

        assert_eq!(
            max_contract_file_block_for_chain(&files, ETHEREUM_CHAIN_ID),
            Some(25_330_048)
        );
        assert_eq!(
            max_contract_file_block_for_chain(&files, 100),
            Some(40_000_048)
        );

        let latest = recent_contract_parquet_files(&files, ETHEREUM_CHAIN_ID, None);
        assert_eq!(
            latest[0].file_name().and_then(|name| name.to_str()),
            Some("tail__chain_0000000001__0025330041__0025330048.parquet")
        );

        let gnosis_latest = recent_contract_parquet_files(&files, 100, None);
        assert_eq!(
            gnosis_latest[0].file_name().and_then(|name| name.to_str()),
            Some("tail__chain_0000000100__0040000001__0040000048.parquet")
        );

        let cursor_page = recent_contract_parquet_files(
            &files,
            ETHEREUM_CHAIN_ID,
            Some(RecentCursor {
                block_number: 25_330_040,
                create_index: 0,
            }),
        );
        assert_eq!(
            cursor_page[0].file_name().and_then(|name| name.to_str()),
            Some("tail__chain_0000000001__0025330032__0025330040.parquet")
        );
    }

    #[tokio::test]
    async fn stats_and_recent_include_parquet_rows_newer_than_zellic() {
        let dir = TestDir::new("parquet_newer_than_zellic");
        insert_zellic_snapshot(&dir.path);
        write_backfill_parquet(&dir.path);

        let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

        let stats = db.stats(ETHEREUM_CHAIN_ID).await.unwrap();
        assert_eq!(stats.total_contracts, 2);
        assert_eq!(stats.first_block, 100);
        assert_eq!(stats.last_block, 200);

        let deploys = db
            .deploys_over_time(ETHEREUM_CHAIN_ID, 100, Some((0, 250)))
            .await
            .unwrap();
        assert_eq!(
            deploys
                .iter()
                .map(|bucket| (bucket.block_start, bucket.count))
                .collect::<Vec<_>>(),
            vec![(100, 1), (200, 1)]
        );

        let recent = db
            .recent_contracts(ETHEREUM_CHAIN_ID, 5, None)
            .await
            .unwrap();
        assert_eq!(recent.contracts.len(), 1);
        assert_eq!(recent.contracts[0].block_number, 200);
        assert_eq!(
            recent.contracts[0].address,
            format!("0x{}", hex::encode(make_bytes(5, 20)))
        );
    }

    #[tokio::test]
    async fn stats_and_recent_filter_gnosis_chain() {
        let dir = TestDir::new("gnosis_chain_filter");
        insert_zellic_snapshot(&dir.path);
        write_backfill_parquet(&dir.path);

        let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

        let stats = db.stats(100).await.unwrap();
        assert_eq!(stats.total_contracts, 1);
        assert_eq!(stats.first_block, 300);
        assert_eq!(stats.last_block, 300);

        let recent = db.recent_contracts(100, 5, None).await.unwrap();
        assert_eq!(recent.contracts.len(), 1);
        assert_eq!(recent.contracts[0].block_number, 300);
        assert_eq!(
            recent.contracts[0].address,
            format!("0x{}", hex::encode(make_bytes(0x15, 20)))
        );
    }

    #[tokio::test]
    async fn recent_does_not_fall_back_to_ethereum_for_other_chains() {
        let dir = TestDir::new("no_cross_chain_recent_fallback");
        insert_zellic_snapshot(&dir.path);

        let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

        let recent = db.recent_contracts(100, 5, None).await.unwrap();
        assert!(recent.contracts.is_empty());
        assert!(!recent.has_more);
    }

    #[tokio::test]
    async fn dashboard_sql_scopes_contract_metadata_by_chain() {
        let dir = TestDir::new("query_chain_scope");
        insert_zellic_snapshot(&dir.path);
        write_backfill_parquet(&dir.path);

        let db = Db::open_with_mode(&dir.path, "*.parquet", false).unwrap();

        let result = db
            .query_sql(
                "SELECT chain_id, block_number FROM contract_metadata ORDER BY block_number"
                    .to_string(),
                10,
                Some(100),
            )
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0][0], serde_json::json!(100));
        assert_eq!(result.rows[0][1], serde_json::json!(300));
    }
}

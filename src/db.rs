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
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use duckdb::{params, types::ValueRef, AccessMode, Config, Connection, Row};
use serde_json::{Number, Value};
use tokio::sync::Mutex;

use crate::util::match_simple_glob;

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

fn verification_registry_loaded(conn: &Connection) -> Result<bool> {
    if !table_exists(conn, "verification_registry_imports")? {
        return Ok(false);
    }
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM verification_registry_imports WHERE source = 'verifier_alliance'",
            [],
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
    let is_verified_expr = if verification_registry_loaded(conn)? {
        "COALESCE(e.is_verified, false) AS is_verified"
    } else {
        "e.is_verified"
    };
    let contract_metadata_sql = format!(
        r#"
        CREATE OR REPLACE TEMP VIEW contract_metadata AS
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
        LEFT JOIN enrichment_current e ON c.contract_address = e.contract_address;
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
            let db = Self {
                inner: Arc::new(Mutex::new(conn)),
                data_dir: data_dir.to_path_buf(),
                contracts_glob: contracts_glob.to_string(),
            };
            db.rebuild_contracts_view_blocking()?;
            return Ok(db);
        }

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS enrichment (
                contract_address BLOB,
                is_verified BOOLEAN NOT NULL,
                contract_name VARCHAR,
                checked_at TIMESTAMP NOT NULL
            );
            -- Track where each verification came from (verifier_alliance).
            -- Added in a later migration; the IF NOT EXISTS guard keeps older
            -- databases working without an explicit migration step.
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS verification_source VARCHAR;
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS match_type VARCHAR;
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS block_number UINTEGER;
            ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS create_index UINTEGER;
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

        let db = Self {
            inner: Arc::new(Mutex::new(conn)),
            data_dir: data_dir.to_path_buf(),
            contracts_glob: contracts_glob.to_string(),
        };
        db.rebuild_contracts_view_blocking()?;
        Ok(db)
    }

    fn rebuild_contracts_view_blocking(&self) -> Result<()> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(&self.data_dir)
            .with_context(|| format!("read data dir {}", self.data_dir.display()))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension().and_then(|s| s.to_str()) == Some("parquet")
                    && match_simple_glob(
                        &self.contracts_glob,
                        p.file_name().and_then(|s| s.to_str()).unwrap_or_default(),
                    )
                    && p.file_name()
                        .and_then(|s| s.to_str())
                        .map(|n| n != "enrichment.parquet")
                        .unwrap_or(true)
            })
            .collect();
        files.sort();

        let conn = self
            .inner
            .try_lock()
            .map_err(|_| anyhow!("db lock contended at startup"))?;

        // Use TEMP VIEWs so read-only mode (where the main database is locked
        // for writes) can still set this up — temp views live in a session-
        // scoped schema and don't require write access to the on-disk DB.
        let has_zellic =
            table_exists(&conn, "zellic_contracts")? && table_exists(&conn, "zellic_bytecodes")?;

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

        let selects = [parquet_select, zellic_select]
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
        create_metadata_current_view(&conn)?;
        create_enrichment_current_view(&conn)?;
        create_standard_query_views(&conn, has_zellic)?;
        Ok(())
    }

    pub async fn refresh_contracts_view(&self) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.rebuild_contracts_view_blocking())
            .await
            .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn query_sql(&self, sql: String, limit: u32) -> Result<SqlQueryResult> {
        let inner = self.inner.clone();
        let normalized = normalize_read_only_sql(&sql)?;
        let limit = limit.clamp(1, 1_000);
        tokio::task::spawn_blocking(move || -> Result<SqlQueryResult> {
            let started = Instant::now();
            let wrapped = format!(
                "SELECT * FROM ({}) AS _blink_dashboard_query LIMIT {}",
                normalized, limit
            );
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

    pub async fn stats(&self) -> Result<Stats> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<Stats> {
            let conn = inner.blocking_lock();
            let source_table = if table_exists(&conn, "zellic_contracts")? {
                "zellic_contracts"
            } else {
                "contracts"
            };
            let source_sql = format!(
                "SELECT COUNT(*), MIN(block_number), MAX(block_number) FROM {}",
                source_table
            );
            let (total, first_block, last_block) = conn
                .query_row::<(Option<i64>, Option<u32>, Option<u32>), _, _>(
                    &source_sql,
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .context("contracts agg")?;

            let total = total.unwrap_or(0).max(0) as u64;
            let first_block = first_block.unwrap_or(0) as u64;
            let last_block = last_block.unwrap_or(0) as u64;

            let registry_loaded = verification_registry_loaded(&conn)?;
            let (enriched, verified): (i64, i64) = if registry_loaded {
                let verified_sql = format!(
                    r#"
                    SELECT COUNT(*)
                    FROM {source_table} c
                    JOIN enrichment e ON c.contract_address = e.contract_address
                    WHERE e.is_verified
                    "#
                );
                let verified = conn
                    .query_row(&verified_sql, [], |row| row.get(0))
                    .unwrap_or(0);
                (total as i64, verified)
            } else {
                conn.query_row(
                    "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_verified) FROM enrichment",
                    [],
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

    pub async fn deploys_over_time(&self, bucket_blocks: u64) -> Result<Vec<DeployBucket>> {
        let inner = self.inner.clone();
        let bucket_blocks = bucket_blocks.max(1);
        tokio::task::spawn_blocking(move || -> Result<Vec<DeployBucket>> {
            let conn = inner.blocking_lock();
            let sql = if table_exists(&conn, "zellic_block_counts")? {
                r#"
                SELECT
                    (block_number / ?)::UBIGINT AS bucket_id,
                    SUM(contract_count)::UBIGINT AS cnt
                FROM zellic_block_counts
                WHERE block_number IS NOT NULL
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#
            } else if table_exists(&conn, "zellic_contracts")? {
                r#"
                SELECT
                    (block_number / ?)::UBIGINT AS bucket_id,
                    COUNT(*)::UBIGINT AS cnt
                FROM zellic_contracts
                WHERE block_number IS NOT NULL
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#
            } else {
                r#"
                SELECT
                    (block_number / ?)::UBIGINT AS bucket_id,
                    COUNT(*)::UBIGINT AS cnt
                FROM contracts
                WHERE block_number IS NOT NULL
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map(params![bucket_blocks], |row| {
                let bucket_id: u64 = row.get(0)?;
                let count: u64 = row.get(1)?;
                Ok((bucket_id, count))
            })?;
            let mut out = Vec::new();
            for r in rows {
                let (bucket_id, count) = r?;
                let block_start = bucket_id * bucket_blocks;
                let block_end = block_start + bucket_blocks - 1;
                let mid = block_start + bucket_blocks / 2;
                out.push(DeployBucket {
                    block_start,
                    block_end,
                    timestamp: crate::blocks::block_timestamp(mid),
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
        bucket_blocks: u64,
    ) -> Result<Vec<VerifiedRatioBucket>> {
        let inner = self.inner.clone();
        let bucket_blocks = bucket_blocks.max(1);
        tokio::task::spawn_blocking(move || -> Result<Vec<VerifiedRatioBucket>> {
            let conn = inner.blocking_lock();
            let mut out = Vec::new();
            let registry_loaded = verification_registry_loaded(&conn)?;
            if table_exists(&conn, "zellic_contracts")? {
                if !table_exists(&conn, "zellic_block_counts")? {
                    return Ok(out);
                }

                let sql = if registry_loaded {
                    r#"
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
                        JOIN enrichment_current e ON z.contract_address = e.contract_address
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
                    "#
                } else {
                    r#"
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
                    "#
                };
                let mut stmt = conn.prepare(sql)?;
                let rows = stmt.query_map(params![bucket_blocks, bucket_blocks], |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, u64>(2)?,
                        row.get::<_, u64>(3)?,
                    ))
                })?;
                for r in rows {
                    let (bucket_id, verified, unverified, unknown) = r?;
                    let block_start = bucket_id * bucket_blocks;
                    let block_end = block_start + bucket_blocks - 1;
                    let mid = block_start + bucket_blocks / 2;
                    out.push(VerifiedRatioBucket {
                        block_start,
                        block_end,
                        timestamp: crate::blocks::block_timestamp(mid),
                        verified,
                        unverified,
                        unknown,
                    });
                }
                return Ok(out);
            }

            let sql = if registry_loaded {
                r#"
                SELECT
                    (c.block_number / ?)::UBIGINT AS bucket_id,
                    COUNT(*) FILTER (WHERE e.is_verified)::UBIGINT AS verified,
                    COUNT(*) FILTER (WHERE e.is_verified IS NULL OR e.is_verified IS FALSE)::UBIGINT AS unverified,
                    0::UBIGINT AS unknown
                FROM contracts c
                LEFT JOIN enrichment e ON c.contract_address = e.contract_address
                WHERE c.block_number IS NOT NULL
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#
            } else {
                r#"
                SELECT
                    (c.block_number / ?)::UBIGINT AS bucket_id,
                    COUNT(*) FILTER (WHERE e.is_verified)::UBIGINT AS verified,
                    COUNT(*) FILTER (WHERE e.is_verified IS FALSE)::UBIGINT AS unverified,
                    COUNT(*) FILTER (WHERE e.is_verified IS NULL)::UBIGINT AS unknown
                FROM contracts c
                LEFT JOIN enrichment e ON c.contract_address = e.contract_address
                WHERE c.block_number IS NOT NULL
                GROUP BY bucket_id
                ORDER BY bucket_id
                "#
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map(params![bucket_blocks], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, u64>(3)?,
                ))
            })?;
            for r in rows {
                let (bucket_id, verified, unverified, unknown) = r?;
                let block_start = bucket_id * bucket_blocks;
                let block_end = block_start + bucket_blocks - 1;
                let mid = block_start + bucket_blocks / 2;
                out.push(VerifiedRatioBucket {
                    block_start,
                    block_end,
                    timestamp: crate::blocks::block_timestamp(mid),
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

    pub async fn bytecode_size_distribution(&self) -> Result<Vec<SizeBin>> {
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
            let sql = if table_exists(&conn, "zellic_bytecodes")?
                && table_exists(&conn, "zellic_bytecode_counts")?
            {
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
                LEFT JOIN bytecode_metadata_current m ON c.contract_address = m.contract_address
                WHERE c.n_code_bytes IS NOT NULL
                  AND c.n_code_bytes <= 24576
                GROUP BY bin_id
                ORDER BY bin_id
                "#
                .to_string()
            };
            let mut stmt = conn.prepare(&sql)?;
            let rows =
                stmt.query_map([], |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)))?;
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

    pub async fn top_compilers(&self, limit: u32) -> Result<Vec<CompilerCount>> {
        let inner = self.inner.clone();
        let limit = limit.clamp(1, 50);
        tokio::task::spawn_blocking(move || -> Result<Vec<CompilerCount>> {
            let conn = inner.blocking_lock();
            // Compiler distribution is bytecode-derived. Verification sources
            // can confirm source publication, but local decode remains the
            // source of truth for compiler metadata in this dashboard.
            let sql = if table_exists(&conn, "zellic_bytecode_counts")?
                && table_exists(&conn, "bytecode_metadata_by_hash")?
            {
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
                SELECT compiler_version, COUNT(*)::UBIGINT AS cnt
                FROM bytecode_metadata_current
                WHERE compiler_version IS NOT NULL
                GROUP BY compiler_version
                ORDER BY cnt DESC
                LIMIT ?
                "#
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map(params![limit as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
            })?;
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

    pub async fn compiler_version_total(&self) -> Result<u64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<u64> {
            let conn = inner.blocking_lock();
            if table_exists(&conn, "zellic_bytecode_counts")?
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
                    "SELECT COUNT(*) FROM bytecode_metadata_current WHERE compiler_version IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            Ok(count.max(0) as u64)
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn language_distribution(&self) -> Result<Vec<LanguageCount>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<LanguageCount>> {
            let conn = inner.blocking_lock();
            let sql = if table_exists(&conn, "zellic_bytecode_counts")?
                && table_exists(&conn, "bytecode_metadata_by_hash")?
            {
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
                SELECT COALESCE(language, 'unknown') AS lang, COUNT(*)::UBIGINT AS cnt
                FROM bytecode_metadata_current
                GROUP BY lang
                ORDER BY cnt DESC
                "#
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
            })?;
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

    pub async fn standards_breakdown(&self) -> Result<StandardsBreakdown> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<StandardsBreakdown> {
            let conn = inner.blocking_lock();
            if table_exists(&conn, "zellic_bytecode_counts")?
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
                SELECT
                    COUNT(*) FILTER (WHERE is_erc20)::UBIGINT,
                    COUNT(*) FILTER (WHERE is_erc721)::UBIGINT,
                    COUNT(*) FILTER (WHERE is_erc1155)::UBIGINT,
                    COUNT(*) FILTER (WHERE is_proxy_eip1967)::UBIGINT,
                    COUNT(*) FILTER (WHERE is_proxy_minimal)::UBIGINT,
                    COUNT(*) FILTER (WHERE uses_push0)::UBIGINT,
                    COUNT(*) FILTER (WHERE has_source_hash)::UBIGINT,
                    COUNT(*)::UBIGINT
                FROM bytecode_metadata_current
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
            })
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn recent_contracts(
        &self,
        limit: u32,
        cursor: Option<RecentCursor>,
    ) -> Result<RecentPage> {
        let inner = self.inner.clone();
        let limit = limit.clamp(1, 200);
        let mut contracts = tokio::task::spawn_blocking(move || -> Result<Vec<RecentContract>> {
            let conn = inner.blocking_lock();
            let page_limit = limit as i64 + 1;
            let has_zellic = table_exists(&conn, "zellic_contracts")?
                && table_exists(&conn, "zellic_bytecodes")?;
            let has_hash_metadata = table_exists(&conn, "bytecode_metadata_by_hash")?;
            let registry_loaded = verification_registry_loaded(&conn)?;

            // For Zellic imports, compiler metadata is keyed by bytecode hash.
            // Select the tiny recent page first, then join hash metadata; going
            // through bytecode_metadata_current would expand hash metadata back
            // to tens of millions of contract-address rows for every page.
            let sql = if has_zellic && has_hash_metadata && cursor.is_some() {
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
                LEFT JOIN enrichment e ON page.contract_address = e.contract_address
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
                LEFT JOIN enrichment e ON page.contract_address = e.contract_address
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
                LEFT JOIN enrichment e ON page.contract_address = e.contract_address
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
                LEFT JOIN enrichment e ON page.contract_address = e.contract_address
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
                    FROM contracts c
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
                LEFT JOIN enrichment e        ON page.contract_address = e.contract_address
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
                    FROM contracts c
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
                LEFT JOIN enrichment e        ON page.contract_address = e.contract_address
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
                let (addr, block, create_index, deployer, n_code, verified, name, compiler) = r?;
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
                    timestamp: crate::blocks::block_timestamp(block_u64),
                    deployer: format!("0x{}", hex::encode(deployer.unwrap_or_default())),
                    n_code_bytes: n_code.unwrap_or(0) as u64,
                    is_verified,
                    contract_name: name,
                    compiler_version: compiler,
                });
            }
            Ok(out)
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))??;
        let has_more = contracts.len() > limit as usize;
        if has_more {
            contracts.truncate(limit as usize);
        }
        Ok(RecentPage {
            contracts,
            has_more,
        })
    }

    pub async fn highest_block(&self) -> Result<Option<u64>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<Option<u64>> {
            let conn = inner.blocking_lock();
            let block: Option<u32> = conn
                .query_row("SELECT MAX(block_number) FROM contracts", [], |row| {
                    row.get(0)
                })
                .unwrap_or(None);
            Ok(block.map(|b| b as u64))
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }
}

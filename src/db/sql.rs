//! Read-only SQL guard rails and JSON conversion for `POST /api/query`.

use anyhow::{anyhow, Result};
use duckdb::types::ValueRef;
use serde_json::{Number, Value};

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct SqlQueryResult {
    pub columns: Vec<String>,
    #[schema(value_type = Vec<Vec<Object>>)]
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
    pub limit: u32,
    pub elapsed_ms: u128,
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

pub(crate) fn normalize_read_only_sql(sql: &str) -> Result<String> {
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

pub(crate) fn wrap_dashboard_query(sql: &str, limit: u32, chain_id: Option<u64>) -> String {
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

pub(crate) fn value_ref_to_json(value: ValueRef<'_>) -> Value {
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

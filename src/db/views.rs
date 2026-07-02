//! Persistent schema (enrichment + decode metadata tables) and the temp views
//! backing `POST /api/query`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use duckdb::Connection;

use super::{column_exists, table_exists};

/// Create/migrate the persistent tables owned by the serve writer.
pub(crate) fn ensure_schema(conn: &Connection) -> Result<()> {
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
        CREATE INDEX IF NOT EXISTS enrichment_chain_block_idx ON enrichment(chain_id, block_number);
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
    .context("create blink schema")
}

/// Rebuild every temp view on this connection: the parquet-backed raw views
/// plus the metadata/enrichment compatibility views.
pub(crate) fn rebuild_query_views(conn: &Connection, files: &[PathBuf]) -> Result<()> {
    rebuild_parquet_views(conn, files)?;
    create_metadata_current_view(conn)?;
    create_enrichment_current_view(conn)?;
    create_standard_query_views(conn)?;
    Ok(())
}

/// Rebuild only the views whose definition embeds the parquet file list.
/// Cheap; called on every connection when the tail loop lands a new file.
pub(crate) fn rebuild_parquet_views(conn: &Connection, files: &[PathBuf]) -> Result<()> {
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

    let parquet_body = if files.is_empty() {
        empty_select.to_string()
    } else {
        let list = files
            .iter()
            .map(|p| format!("'{}'", p.display().to_string().replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            r#"
                SELECT
                    block_number, block_hash, create_index, transaction_hash,
                    contract_address, deployer, factory, init_code, code,
                    init_code_hash, n_init_code_bytes, n_code_bytes,
                    code_hash, chain_id
                FROM read_parquet([{list}], union_by_name = true)
                "#
        )
    };
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP VIEW parquet_contracts AS\n{parquet_body};"
    ))
    .with_context(|| format!("create parquet contracts view ({} files)", files.len()))?;

    let has_zellic =
        table_exists(conn, "zellic_contracts")? && table_exists(conn, "zellic_bytecodes")?;
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
    conn.execute_batch(&format!(
        "CREATE OR REPLACE TEMP VIEW contracts AS\n{body};"
    ))
    .with_context(|| format!("create contracts view ({} files)", files.len()))
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
        (false, false) => unreachable!("guarded above"),
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

    conn.execute_batch(&format!(
        r#"
        CREATE OR REPLACE TEMP VIEW bytecode_metadata_current AS
        {address_meta};
        "#
    ))
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

/// Views consumed by `POST /api/query` users: `bytecodes`,
/// `decoded_bytecodes`, `contract_metadata_all`.
fn create_standard_query_views(conn: &Connection) -> Result<()> {
    let has_rollups = table_exists(conn, "rollup_code_counts")?;
    let has_zellic_bytecodes = table_exists(conn, "zellic_bytecodes")?;
    let has_hash = table_exists(conn, "bytecode_metadata_by_hash")?;
    let hash_has_decoded_at =
        has_hash && column_exists(conn, "bytecode_metadata_by_hash", "decoded_at")?;

    // Distinct bytecodes with usage counts, from the native rollup instead of
    // a parquet scan. `code` comes from the Zellic snapshot when available.
    let bytecodes_sql = if has_rollups {
        let (code_expr, code_join) = if has_zellic_bytecodes {
            (
                "b.code",
                "LEFT JOIN zellic_bytecodes b ON c.code_hash = b.code_hash",
            )
        } else {
            ("CAST(NULL AS BLOB)", "")
        };
        format!(
            r#"
            CREATE OR REPLACE TEMP VIEW bytecodes AS
            SELECT
                c.code_hash,
                lower('0x' || hex(c.code_hash)) AS code_hash_hex,
                any_value(c.n_code_bytes)::UINTEGER AS n_code_bytes,
                any_value({code_expr}) AS code,
                SUM(c.contract_count)::UBIGINT AS contract_count
            FROM rollup_code_counts c
            {code_join}
            GROUP BY c.code_hash;
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

    let metadata_join = if has_hash {
        "LEFT JOIN decoded_bytecodes m ON c.code_hash = m.code_hash"
    } else {
        "LEFT JOIN bytecode_metadata_current m ON c.contract_address = m.contract_address"
    };
    let is_verified_expr = if table_exists(conn, "verification_registry_imports")? {
        "COALESCE(e.is_verified, false) AS is_verified"
    } else {
        "e.is_verified"
    };
    conn.execute_batch(&format!(
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
    ))
    .context("create contract metadata query view")
}

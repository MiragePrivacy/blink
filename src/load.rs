//! Loader for local contract datasets.

use std::{
    path::{Path, PathBuf},
    time::{Instant, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use duckdb::{params, Connection};

use crate::{
    cli::LoadArgs,
    util::{format_count, match_simple_glob, print_header, print_kv, print_kv_accent},
};

pub async fn run_load(args: LoadArgs) -> Result<()> {
    tokio::task::spawn_blocking(move || run_load_blocking(args))
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
}

fn run_load_blocking(args: LoadArgs) -> Result<()> {
    if !args.contracts_dir.is_dir() {
        return Err(anyhow!(
            "--contracts-dir {} does not exist or is not a directory",
            args.contracts_dir.display()
        ));
    }
    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("create data dir {}", args.data_dir.display()))?;

    let inputs = detect_inputs(&args.contracts_dir, &args.contracts_glob)?;
    let verifier_alliance = detect_verifier_alliance_inputs(args.verifier_alliance_dir.as_deref())?;

    if !inputs.has_normalized_csv && inputs.parquet_files.is_empty() && verifier_alliance.is_none()
    {
        return Err(anyhow!(
            "no loadable data in {} — expected either a normalized CSV pair \
             (contracts.csv + bytecodes.csv), Parquet files matching `{}`, \
             or --va pointing to a Verifier Alliance export",
            args.contracts_dir.display(),
            args.contracts_glob
        ));
    }

    print_header("blink load");
    print_kv("source", &args.contracts_dir.display().to_string());
    print_kv("data dir", &args.data_dir.display().to_string());
    if inputs.has_normalized_csv {
        print_kv_accent(
            "detected",
            "normalized CSV dataset (contracts.csv + bytecodes.csv)",
        );
    }
    if !inputs.parquet_files.is_empty() {
        print_kv_accent(
            "detected",
            &format!(
                "{} Parquet file(s) matching `{}`",
                inputs.parquet_files.len(),
                args.contracts_glob
            ),
        );
    }
    if let Some(va) = &verifier_alliance {
        print_kv_accent(
            "detected",
            &format!(
                "Verifier Alliance registry ({} deployment files · {} verification files)",
                va.contract_deployments.len(),
                va.verified_contracts.len()
            ),
        );
    }
    print_kv("chain id", &args.chain_id.to_string());
    if args.overwrite {
        print_kv_accent("mode", "overwrite");
    }
    println!();

    if !inputs.parquet_files.is_empty() {
        load_parquet_links(
            &args.contracts_dir,
            &args.data_dir,
            &inputs.parquet_files,
            args.overwrite,
        )?;
    }
    if inputs.has_normalized_csv {
        load_normalized_csvs(
            &args.data_dir,
            &inputs.csv_contracts,
            &inputs.csv_bytecodes,
            &args.memory_limit,
            args.threads,
            args.chain_id,
            args.overwrite,
        )?;
    }
    if let Some(va) = verifier_alliance {
        load_verifier_alliance_registry(
            &args.data_dir,
            &va,
            &args.memory_limit,
            args.threads,
            args.chain_id,
            args.rebuild_va,
        )?;
    }

    Ok(())
}

#[derive(Debug)]
struct LoadInputs {
    csv_contracts: PathBuf,
    csv_bytecodes: PathBuf,
    has_normalized_csv: bool,
    parquet_files: Vec<PathBuf>,
}

#[derive(Debug)]
struct VerifierAllianceInputs {
    contract_deployments: Vec<PathBuf>,
    verified_contracts: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
struct VaFileEntry {
    table_name: &'static str,
    path: PathBuf,
    path_key: String,
    size_bytes: i64,
    modified_unix_ns: i64,
}

fn detect_inputs(contracts_dir: &Path, contracts_glob: &str) -> Result<LoadInputs> {
    let csv_contracts = contracts_dir.join("contracts.csv");
    let csv_bytecodes = contracts_dir.join("bytecodes.csv");
    let has_normalized_csv = csv_contracts.is_file() && csv_bytecodes.is_file();
    let parquet_files = list_parquet_files(contracts_dir, contracts_glob)?;

    Ok(LoadInputs {
        csv_contracts,
        csv_bytecodes,
        has_normalized_csv,
        parquet_files,
    })
}

fn detect_verifier_alliance_inputs(root: Option<&Path>) -> Result<Option<VerifierAllianceInputs>> {
    let Some(root) = root else {
        return Ok(None);
    };
    if !root.is_dir() {
        return Err(anyhow!("--va {} is not a directory", root.display()));
    }

    let deployments_dir = root.join("contract_deployments");
    let verifications_dir = root.join("verified_contracts");
    if !deployments_dir.is_dir() || !verifications_dir.is_dir() {
        return Err(anyhow!(
            "--va needs both {} and {}",
            deployments_dir.display(),
            verifications_dir.display()
        ));
    }

    let contract_deployments = list_parquet_files(&deployments_dir, "*.parquet")?;
    let verified_contracts = list_parquet_files(&verifications_dir, "*.parquet")?;
    if contract_deployments.is_empty() || verified_contracts.is_empty() {
        return Err(anyhow!(
            "--va {} has the expected folders but no Parquet files",
            root.display()
        ));
    }

    Ok(Some(VerifierAllianceInputs {
        contract_deployments,
        verified_contracts,
    }))
}

fn load_parquet_links(
    contracts_dir: &Path,
    data_dir: &Path,
    files: &[PathBuf],
    overwrite: bool,
) -> Result<()> {
    let started = Instant::now();
    print_kv("step", "link Parquet files into data_dir");

    // If the contracts directory IS the data directory, the files are already in place
    let same_dir = same_canonical(contracts_dir, data_dir);
    if same_dir {
        print_kv_accent(
            "parquet",
            &format!(
                "{} file(s) · already in data_dir · {:.1}s",
                files.len(),
                started.elapsed().as_secs_f64()
            ),
        );
        return Ok(());
    }

    let mut linked = 0usize;
    let mut skipped = 0usize;
    let mut replaced = 0usize;

    for src in files {
        let file_name = src
            .file_name()
            .ok_or_else(|| anyhow!("path has no file name: {}", src.display()))?;
        let dest = data_dir.join(file_name);
        let abs_src = std::fs::canonicalize(src)
            .with_context(|| format!("canonicalize {}", src.display()))?;

        match std::fs::symlink_metadata(&dest) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    let current = std::fs::read_link(&dest)
                        .with_context(|| format!("read symlink {}", dest.display()))?;
                    let current_abs =
                        std::fs::canonicalize(&current).unwrap_or_else(|_| current.clone());
                    if current_abs == abs_src && !overwrite {
                        skipped += 1;
                        continue;
                    }
                    std::fs::remove_file(&dest)
                        .with_context(|| format!("remove stale symlink {}", dest.display()))?;
                    std::os::unix::fs::symlink(&abs_src, &dest).with_context(|| {
                        format!("relink {} -> {}", dest.display(), abs_src.display())
                    })?;
                    replaced += 1;
                } else if overwrite {
                    return Err(anyhow!(
                        "refusing to overwrite real file {} (would destroy data)",
                        dest.display()
                    ));
                } else {
                    skipped += 1;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                std::os::unix::fs::symlink(&abs_src, &dest).with_context(|| {
                    format!("symlink {} -> {}", dest.display(), abs_src.display())
                })?;
                linked += 1;
            }
            Err(err) => return Err(err).with_context(|| format!("stat {}", dest.display())),
        }
    }

    print_kv_accent(
        "parquet",
        &format!(
            "{} new · {} relinked · {} unchanged · {:.1}s",
            linked,
            replaced,
            skipped,
            started.elapsed().as_secs_f64()
        ),
    );
    Ok(())
}

fn same_canonical(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

fn list_parquet_files(dir: &Path, glob: &str) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("read dir {}", dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let is_parquet = p.extension().and_then(|e| e.to_str()) == Some("parquet");
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            is_parquet && !name.starts_with('.') && match_simple_glob(glob, name)
        })
        .collect();
    out.sort();
    Ok(out)
}

fn load_normalized_csvs(
    data_dir: &Path,
    contracts_csv: &Path,
    bytecodes_csv: &Path,
    memory_limit: &str,
    threads: Option<usize>,
    chain_id: u64,
    overwrite: bool,
) -> Result<()> {
    let db_path = data_dir.join("blink.duckdb");
    let conn =
        Connection::open(&db_path).with_context(|| format!("open duckdb {}", db_path.display()))?;
    configure_duckdb(&conn, memory_limit, threads)?;

    let has_contracts = table_exists(&conn, "zellic_contracts")?;
    let has_bytecodes = table_exists(&conn, "zellic_bytecodes")?;
    if (has_contracts || has_bytecodes) && !overwrite {
        print_kv(
            "step",
            "normalized CSV import — already present (pass --overwrite to rebuild)",
        );
        print_existing_counts(&conn)?;
        return Ok(());
    }

    if overwrite {
        conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS zellic_bytecode_counts;
            DROP TABLE IF EXISTS zellic_block_counts;
            DROP TABLE IF EXISTS zellic_contracts;
            DROP TABLE IF EXISTS zellic_bytecodes;
            DROP TABLE IF EXISTS bytecode_metadata_by_hash;
            "#,
        )
        .context("drop existing normalized CSV tables")?;
    }

    let started = Instant::now();
    import_bytecodes(&conn, bytecodes_csv)?;
    import_contracts(&conn, contracts_csv, chain_id)?;
    import_counts(&conn)?;

    let bytecodes = count_table(&conn, "zellic_bytecodes")?;
    let contracts = count_table(&conn, "zellic_contracts")?;
    let counted_hashes = count_table(&conn, "zellic_bytecode_counts")?;
    let counted_blocks = count_table(&conn, "zellic_block_counts")?;
    let (min_block, max_block): (Option<u32>, Option<u32>) = conn
        .query_row(
            "SELECT MIN(block_number), MAX(block_number) FROM zellic_contracts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap_or((None, None));

    println!();
    print_kv_accent("contracts", &format_count(contracts));
    print_kv_accent("bytecodes", &format_count(bytecodes));
    print_kv("hash counts", &format_count(counted_hashes));
    print_kv("block counts", &format_count(counted_blocks));
    print_kv(
        "block range",
        &format!("{} - {}", min_block.unwrap_or(0), max_block.unwrap_or(0)),
    );
    print_kv_accent(
        "elapsed",
        &format!("{:.1}s", started.elapsed().as_secs_f64()),
    );

    Ok(())
}

fn configure_duckdb(conn: &Connection, memory_limit: &str, threads: Option<usize>) -> Result<()> {
    let memory_limit = memory_limit.replace('\'', "''");
    conn.execute_batch(&format!(
        "SET memory_limit='{}'; SET preserve_insertion_order=false;",
        memory_limit
    ))
    .context("configure DuckDB memory")?;
    if let Some(threads) = threads {
        conn.execute_batch(&format!("PRAGMA threads={};", threads.max(1)))
            .context("configure DuckDB threads")?;
    }
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

fn count_table(conn: &Connection, table: &str) -> Result<u64> {
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
    Ok(count.max(0) as u64)
}

fn print_existing_counts(conn: &Connection) -> Result<()> {
    if table_exists(conn, "zellic_contracts")? {
        print_kv(
            "contracts",
            &format_count(count_table(conn, "zellic_contracts")?),
        );
    }
    if table_exists(conn, "zellic_bytecodes")? {
        print_kv(
            "bytecodes",
            &format_count(count_table(conn, "zellic_bytecodes")?),
        );
    }
    if table_exists(conn, "zellic_bytecode_counts")? {
        print_kv(
            "hash counts",
            &format_count(count_table(conn, "zellic_bytecode_counts")?),
        );
    }
    if table_exists(conn, "zellic_block_counts")? {
        print_kv(
            "block counts",
            &format_count(count_table(conn, "zellic_block_counts")?),
        );
    }
    Ok(())
}

fn ensure_verification_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS enrichment (
            contract_address BLOB,
            chain_id UBIGINT DEFAULT 1,
            is_verified BOOLEAN NOT NULL,
            contract_name VARCHAR,
            checked_at TIMESTAMP NOT NULL
        );
        ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS chain_id UBIGINT DEFAULT 1;
        ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS verification_source VARCHAR;
        ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS match_type VARCHAR;
        ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS block_number UINTEGER;
        ALTER TABLE enrichment ADD COLUMN IF NOT EXISTS create_index UINTEGER;
        UPDATE enrichment SET chain_id = 1 WHERE chain_id IS NULL;
        CREATE INDEX IF NOT EXISTS enrichment_chain_addr_idx ON enrichment(chain_id, contract_address);
        CREATE INDEX IF NOT EXISTS enrichment_verified_idx ON enrichment(is_verified);
        CREATE INDEX IF NOT EXISTS enrichment_source_idx ON enrichment(verification_source);

        CREATE TABLE IF NOT EXISTS verification_registry_imports (
            source VARCHAR NOT NULL,
            chain_id UBIGINT NOT NULL,
            imported_at TIMESTAMP NOT NULL,
            verified_count UBIGINT NOT NULL,
            PRIMARY KEY (source, chain_id)
        );

        CREATE TABLE IF NOT EXISTS verification_registry_files (
            source VARCHAR NOT NULL,
            chain_id UBIGINT NOT NULL,
            table_name VARCHAR NOT NULL,
            path VARCHAR NOT NULL,
            size_bytes BIGINT NOT NULL,
            modified_unix_ns BIGINT NOT NULL,
            imported_at TIMESTAMP NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS verification_registry_files_idx
            ON verification_registry_files(source, chain_id, table_name, path);

        CREATE TABLE IF NOT EXISTS verification_registry_file_addresses (
            source VARCHAR NOT NULL,
            chain_id UBIGINT NOT NULL,
            table_name VARCHAR NOT NULL,
            path VARCHAR NOT NULL,
            contract_address BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS verification_registry_file_addresses_idx
            ON verification_registry_file_addresses(source, chain_id, table_name, path);
        CREATE INDEX IF NOT EXISTS verification_registry_file_addresses_addr_idx
            ON verification_registry_file_addresses(chain_id, contract_address);
        "#,
    )
    .context("create verification registry schema")
}

fn load_verifier_alliance_registry(
    data_dir: &Path,
    inputs: &VerifierAllianceInputs,
    memory_limit: &str,
    threads: Option<usize>,
    chain_id: u64,
    rebuild_va: bool,
) -> Result<()> {
    let started = Instant::now();
    print_kv(
        "step",
        if rebuild_va {
            "rebuild Verifier Alliance registry"
        } else {
            "import Verifier Alliance registry incrementally"
        },
    );

    let db_path = data_dir.join("blink.duckdb");
    let conn =
        Connection::open(&db_path).with_context(|| format!("open duckdb {}", db_path.display()))?;
    configure_duckdb(&conn, memory_limit, threads)?;
    ensure_verification_schema(&conn)?;

    let entries = verifier_alliance_file_entries(inputs)?;
    if rebuild_va {
        rebuild_verifier_alliance_registry(&conn, inputs, &entries, chain_id, started)
    } else {
        import_verifier_alliance_registry_incremental(&conn, inputs, &entries, chain_id, started)
    }
}

fn rebuild_verifier_alliance_registry(
    conn: &Connection,
    inputs: &VerifierAllianceInputs,
    entries: &[VaFileEntry],
    chain_id: u64,
    started: Instant,
) -> Result<()> {
    let deployments_list = sql_path_list(&inputs.contract_deployments)?;
    let verifications_list = sql_path_list(&inputs.verified_contracts)?;
    conn.execute_batch("BEGIN TRANSACTION;")
        .context("begin Verifier Alliance import")?;

    let result = (|| -> Result<()> {
        conn.execute_batch(&format!(
            r#"
            DELETE FROM verification_registry_imports
            WHERE source = 'verifier_alliance'
              AND chain_id = {chain_id};
            DELETE FROM enrichment
            WHERE verification_source = 'verifier_alliance'
              AND chain_id = {chain_id};
            "#,
        ))
        .context("prepare verification registry import")?;

        let sql = format!(
            r#"
            CREATE OR REPLACE TEMP TABLE va_verified_contracts AS
            SELECT
                cd.address AS contract_address,
                (max(cd.block_number) FILTER (WHERE cd.block_number >= 0))::UINTEGER
                    AS block_number,
                (min(cd.transaction_index) FILTER (WHERE cd.transaction_index >= 0))::UINTEGER
                    AS create_index,
                max(vc.created_at)::TIMESTAMP AS checked_at,
                bool_or(COALESCE(vc.runtime_match, false)) AS runtime_match,
                bool_or(COALESCE(vc.creation_match, false)) AS creation_match,
                bool_or(COALESCE(vc.runtime_metadata_match, false)) AS runtime_metadata_match,
                bool_or(COALESCE(vc.creation_metadata_match, false)) AS creation_metadata_match
            FROM read_parquet({verifications_list}) vc
            JOIN read_parquet({deployments_list}) cd
              ON cd.id = vc.deployment_id
            WHERE cd.chain_id = {chain_id}
              AND cd.address IS NOT NULL
            GROUP BY cd.address;

            CREATE OR REPLACE TEMP TABLE enrichment_next AS
            SELECT
                contract_address,
                {chain_id}::UBIGINT AS chain_id,
                true AS is_verified,
                CAST(NULL AS VARCHAR) AS contract_name,
                COALESCE(checked_at, CURRENT_TIMESTAMP) AS checked_at,
                'verifier_alliance' AS verification_source,
                CASE
                    WHEN runtime_match AND creation_match THEN 'runtime+creation'
                    WHEN runtime_match THEN 'runtime'
                    WHEN creation_match THEN 'creation'
                    WHEN runtime_metadata_match OR creation_metadata_match THEN 'metadata'
                    ELSE 'verified'
                END AS match_type,
                block_number,
                create_index
            FROM va_verified_contracts;

            INSERT INTO enrichment (
                contract_address,
                chain_id,
                is_verified,
                contract_name,
                checked_at,
                verification_source,
                match_type,
                block_number,
                create_index
            )
            SELECT
                contract_address,
                chain_id,
                is_verified,
                contract_name,
                checked_at,
                verification_source,
                match_type,
                block_number,
                create_index
            FROM enrichment_next;

            INSERT INTO verification_registry_imports (
                source,
                chain_id,
                imported_at,
                verified_count
            )
            SELECT
                'verifier_alliance',
                {chain_id},
                CURRENT_TIMESTAMP,
                COUNT(*)::UBIGINT
            FROM va_verified_contracts;
            "#
        );
        conn.execute_batch(&sql)
            .context("import Verifier Alliance verified addresses")?;
        rebuild_va_file_addresses(conn, inputs, chain_id)?;
        replace_va_file_manifest(conn, chain_id, entries)?;
        Ok(())
    })();

    if let Err(err) = result {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(err);
    }
    conn.execute_batch("COMMIT;")
        .context("commit Verifier Alliance import")?;

    let verified = va_verified_count(conn, chain_id)?;
    print_kv_accent(
        "verified",
        &format!(
            "{} from Verifier Alliance · {:.1}s",
            format_count(verified),
            started.elapsed().as_secs_f64()
        ),
    );
    Ok(())
}

fn import_verifier_alliance_registry_incremental(
    conn: &Connection,
    inputs: &VerifierAllianceInputs,
    entries: &[VaFileEntry],
    chain_id: u64,
    started: Instant,
) -> Result<()> {
    let changed_entries = changed_va_file_entries(conn, chain_id, entries)?;
    let import_exists = verification_registry_import_exists(conn, chain_id)?;
    let deployment_changed = changed_entries
        .iter()
        .any(|entry| entry.table_name == "contract_deployments");

    let mut changed_verified = if deployment_changed || !import_exists {
        entries
            .iter()
            .filter(|entry| entry.table_name == "verified_contracts")
            .cloned()
            .collect::<Vec<_>>()
    } else {
        changed_entries
            .iter()
            .filter(|entry| entry.table_name == "verified_contracts")
            .cloned()
            .collect::<Vec<_>>()
    };
    changed_verified.sort_by_key(|entry| entry.path_key.clone());
    changed_verified.dedup_by(|a, b| a.path_key == b.path_key);

    if changed_entries.is_empty() && import_exists {
        let verified = va_verified_count(conn, chain_id)?;
        print_kv_accent(
            "verified",
            &format!(
                "{} from Verifier Alliance · already current · {:.1}s",
                format_count(verified),
                started.elapsed().as_secs_f64()
            ),
        );
        return Ok(());
    }

    if changed_verified.is_empty() {
        upsert_va_file_manifest_entries(conn, chain_id, &changed_entries)?;
        let verified = va_verified_count(conn, chain_id)?;
        print_kv_accent(
            "verified",
            &format!(
                "{} from Verifier Alliance · metadata refreshed · {:.1}s",
                format_count(verified),
                started.elapsed().as_secs_f64()
            ),
        );
        return Ok(());
    }

    let deployments_list = sql_path_list(&inputs.contract_deployments)?;
    let verifications_list = sql_path_list(&inputs.verified_contracts)?;
    let changed_verifications_list =
        sql_path_list(changed_verified.iter().map(|entry| &entry.path))?;
    let changed_file_values =
        sql_string_values(changed_verified.iter().map(|entry| entry.path_key.as_str()));

    conn.execute_batch("BEGIN TRANSACTION;")
        .context("begin incremental Verifier Alliance import")?;

    let result = (|| -> Result<()> {
        let sql = format!(
            r#"
            CREATE OR REPLACE TEMP TABLE va_changed_files AS
            SELECT col0 AS path
            FROM (VALUES {changed_file_values});

            CREATE OR REPLACE TEMP TABLE va_previous_affected_addresses AS
            SELECT DISTINCT contract_address
            FROM verification_registry_file_addresses
            WHERE source = 'verifier_alliance'
              AND chain_id = {chain_id}
              AND table_name = 'verified_contracts'
              AND path IN (SELECT path FROM va_changed_files);

            CREATE OR REPLACE TEMP TABLE va_changed_file_addresses AS
            SELECT DISTINCT
                vc.filename AS path,
                cd.address AS contract_address
            FROM read_parquet({changed_verifications_list}, filename=true) vc
            JOIN read_parquet({deployments_list}) cd
              ON cd.id = vc.deployment_id
            WHERE cd.chain_id = {chain_id}
              AND cd.address IS NOT NULL;

            CREATE OR REPLACE TEMP TABLE va_affected_addresses AS
            SELECT contract_address FROM va_previous_affected_addresses
            UNION
            SELECT contract_address FROM va_changed_file_addresses;

            DELETE FROM enrichment
            WHERE verification_source = 'verifier_alliance'
              AND chain_id = {chain_id}
              AND contract_address IN (
                  SELECT contract_address FROM va_affected_addresses
              );

            DELETE FROM verification_registry_file_addresses
            WHERE source = 'verifier_alliance'
              AND chain_id = {chain_id}
              AND table_name = 'verified_contracts'
              AND path IN (SELECT path FROM va_changed_files);

            INSERT INTO verification_registry_file_addresses (
                source,
                chain_id,
                table_name,
                path,
                contract_address
            )
            SELECT
                'verifier_alliance',
                {chain_id},
                'verified_contracts',
                path,
                contract_address
            FROM va_changed_file_addresses;

            CREATE OR REPLACE TEMP TABLE va_verified_contracts AS
            SELECT
                cd.address AS contract_address,
                (max(cd.block_number) FILTER (WHERE cd.block_number >= 0))::UINTEGER
                    AS block_number,
                (min(cd.transaction_index) FILTER (WHERE cd.transaction_index >= 0))::UINTEGER
                    AS create_index,
                max(vc.created_at)::TIMESTAMP AS checked_at,
                bool_or(COALESCE(vc.runtime_match, false)) AS runtime_match,
                bool_or(COALESCE(vc.creation_match, false)) AS creation_match,
                bool_or(COALESCE(vc.runtime_metadata_match, false)) AS runtime_metadata_match,
                bool_or(COALESCE(vc.creation_metadata_match, false)) AS creation_metadata_match
            FROM read_parquet({verifications_list}) vc
            JOIN read_parquet({deployments_list}) cd
              ON cd.id = vc.deployment_id
            JOIN va_affected_addresses affected
              ON affected.contract_address = cd.address
            WHERE cd.chain_id = {chain_id}
              AND cd.address IS NOT NULL
            GROUP BY cd.address;

            INSERT INTO enrichment (
                contract_address,
                chain_id,
                is_verified,
                contract_name,
                checked_at,
                verification_source,
                match_type,
                block_number,
                create_index
            )
            SELECT
                contract_address,
                {chain_id}::UBIGINT AS chain_id,
                true AS is_verified,
                CAST(NULL AS VARCHAR) AS contract_name,
                COALESCE(checked_at, CURRENT_TIMESTAMP) AS checked_at,
                'verifier_alliance' AS verification_source,
                CASE
                    WHEN runtime_match AND creation_match THEN 'runtime+creation'
                    WHEN runtime_match THEN 'runtime'
                    WHEN creation_match THEN 'creation'
                    WHEN runtime_metadata_match OR creation_metadata_match THEN 'metadata'
                    ELSE 'verified'
                END AS match_type,
                block_number,
                create_index
            FROM va_verified_contracts;

            DELETE FROM verification_registry_imports
            WHERE source = 'verifier_alliance'
              AND chain_id = {chain_id};

            INSERT INTO verification_registry_imports (
                source,
                chain_id,
                imported_at,
                verified_count
            )
            SELECT
                'verifier_alliance',
                {chain_id},
                CURRENT_TIMESTAMP,
                COUNT(*)::UBIGINT
            FROM enrichment
            WHERE verification_source = 'verifier_alliance'
              AND chain_id = {chain_id};
            "#
        );
        conn.execute_batch(&sql)
            .context("import changed Verifier Alliance verified addresses")?;
        upsert_va_file_manifest_entries(conn, chain_id, &changed_entries)?;
        Ok(())
    })();

    if let Err(err) = result {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(err);
    }
    conn.execute_batch("COMMIT;")
        .context("commit incremental Verifier Alliance import")?;

    let verified = va_verified_count(conn, chain_id)?;
    print_kv_accent(
        "verified",
        &format!(
            "{} from Verifier Alliance · {} changed file(s) · {:.1}s",
            format_count(verified),
            changed_entries.len(),
            started.elapsed().as_secs_f64()
        ),
    );
    Ok(())
}

fn rebuild_va_file_addresses(
    conn: &Connection,
    inputs: &VerifierAllianceInputs,
    chain_id: u64,
) -> Result<()> {
    let deployments_list = sql_path_list(&inputs.contract_deployments)?;
    let verifications_list = sql_path_list(&inputs.verified_contracts)?;
    let sql = format!(
        r#"
        DELETE FROM verification_registry_file_addresses
        WHERE source = 'verifier_alliance'
          AND chain_id = {chain_id};

        INSERT INTO verification_registry_file_addresses (
            source,
            chain_id,
            table_name,
            path,
            contract_address
        )
        SELECT DISTINCT
            'verifier_alliance',
            {chain_id},
            'verified_contracts',
            vc.filename,
            cd.address
        FROM read_parquet({verifications_list}, filename=true) vc
        JOIN read_parquet({deployments_list}) cd
          ON cd.id = vc.deployment_id
        WHERE cd.chain_id = {chain_id}
          AND cd.address IS NOT NULL;
        "#
    );
    conn.execute_batch(&sql)
        .context("rebuild Verifier Alliance file address map")
}

fn verification_registry_import_exists(conn: &Connection, chain_id: u64) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM verification_registry_imports WHERE source = 'verifier_alliance' AND chain_id = ?",
        params![chain_id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn va_verified_count(conn: &Connection, chain_id: u64) -> Result<u64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM enrichment WHERE verification_source = 'verifier_alliance' AND chain_id = ?",
        params![chain_id],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(count.max(0) as u64)
}

fn verifier_alliance_file_entries(inputs: &VerifierAllianceInputs) -> Result<Vec<VaFileEntry>> {
    let mut entries = Vec::new();
    for path in &inputs.contract_deployments {
        entries.push(va_file_entry("contract_deployments", path)?);
    }
    for path in &inputs.verified_contracts {
        entries.push(va_file_entry("verified_contracts", path)?);
    }
    entries.sort_by_key(|entry| (entry.table_name, entry.path_key.clone()));
    Ok(entries)
}

fn va_file_entry(table_name: &'static str, path: &Path) -> Result<VaFileEntry> {
    let canonical =
        std::fs::canonicalize(path).with_context(|| format!("canonicalize {}", path.display()))?;
    let metadata =
        std::fs::metadata(&canonical).with_context(|| format!("stat {}", canonical.display()))?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| {
            duration.as_secs() as i64 * 1_000_000_000 + i64::from(duration.subsec_nanos())
        })
        .unwrap_or(0);
    Ok(VaFileEntry {
        table_name,
        path_key: canonical.display().to_string(),
        path: canonical,
        size_bytes: metadata.len().min(i64::MAX as u64) as i64,
        modified_unix_ns: modified,
    })
}

fn changed_va_file_entries(
    conn: &Connection,
    chain_id: u64,
    entries: &[VaFileEntry],
) -> Result<Vec<VaFileEntry>> {
    let mut changed = Vec::new();
    for entry in entries {
        let count: i64 = conn.query_row(
            r#"
                SELECT COUNT(*)
                FROM verification_registry_files
                WHERE source = 'verifier_alliance'
                  AND chain_id = ?
                  AND table_name = ?
                  AND path = ?
                  AND size_bytes = ?
                  AND modified_unix_ns = ?
                "#,
            params![
                chain_id,
                entry.table_name,
                entry.path_key,
                entry.size_bytes,
                entry.modified_unix_ns
            ],
            |row| row.get(0),
        )?;
        if count == 0 {
            changed.push(entry.clone());
        }
    }
    Ok(changed)
}

fn replace_va_file_manifest(
    conn: &Connection,
    chain_id: u64,
    entries: &[VaFileEntry],
) -> Result<()> {
    conn.execute(
        "DELETE FROM verification_registry_files WHERE source = 'verifier_alliance' AND chain_id = ?",
        params![chain_id],
    )
    .context("clear Verifier Alliance file manifest")?;
    upsert_va_file_manifest_entries(conn, chain_id, entries)
}

fn upsert_va_file_manifest_entries(
    conn: &Connection,
    chain_id: u64,
    entries: &[VaFileEntry],
) -> Result<()> {
    for entry in entries {
        conn.execute(
            r#"
            DELETE FROM verification_registry_files
            WHERE source = 'verifier_alliance'
              AND chain_id = ?
              AND table_name = ?
              AND path = ?
            "#,
            params![chain_id, entry.table_name, entry.path_key],
        )
        .context("delete Verifier Alliance file manifest row")?;
        conn.execute(
            r#"
            INSERT INTO verification_registry_files (
                source,
                chain_id,
                table_name,
                path,
                size_bytes,
                modified_unix_ns,
                imported_at
            )
            VALUES ('verifier_alliance', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            "#,
            params![
                chain_id,
                entry.table_name,
                entry.path_key,
                entry.size_bytes,
                entry.modified_unix_ns
            ],
        )
        .context("insert Verifier Alliance file manifest row")?;
    }
    Ok(())
}

fn import_bytecodes(conn: &Connection, bytecodes_csv: &Path) -> Result<()> {
    let started = Instant::now();
    print_kv("step", "import unique bytecodes");

    let path = sql_path(bytecodes_csv);
    let sql = format!(
        r#"
        CREATE TABLE zellic_bytecodes AS
        WITH raw AS (
            SELECT
                bytecode_hash,
                CASE
                    WHEN bytecode IS NULL THEN ''
                    WHEN starts_with(bytecode, chr(92) || chr(92) || 'x') THEN substr(bytecode, 4)
                    WHEN starts_with(bytecode, chr(92) || 'x') THEN substr(bytecode, 3)
                    WHEN starts_with(bytecode, '0x') THEN substr(bytecode, 3)
                    ELSE bytecode
                END AS code_hex
            FROM read_csv_auto('{path}')
            WHERE bytecode_hash IS NOT NULL
              AND length(bytecode_hash) = 66
        )
        SELECT
            unhex(substr(bytecode_hash, 3)) AS code_hash,
            unhex(code_hex) AS code,
            CAST(length(code_hex) / 2 AS UINTEGER) AS n_code_bytes
        FROM raw;
        "#
    );
    conn.execute_batch(&sql)
        .context("import normalized bytecodes")?;
    print_kv_accent(
        "bytecodes",
        &format!(
            "{} · {:.1}s",
            format_count(count_table(conn, "zellic_bytecodes")?),
            started.elapsed().as_secs_f64()
        ),
    );
    Ok(())
}

fn import_contracts(conn: &Connection, contracts_csv: &Path, chain_id: u64) -> Result<()> {
    let started = Instant::now();
    print_kv("step", "import contract deployments");

    let path = sql_path(contracts_csv);
    let sql = format!(
        r#"
        CREATE TABLE zellic_contracts AS
        SELECT *
        FROM (
            SELECT
                unhex(substr(address, 3)) AS contract_address,
                CASE
                    WHEN bytecode_hash IS NULL THEN NULL
                    WHEN length(bytecode_hash) = 66 THEN unhex(substr(bytecode_hash, 3))
                    ELSE NULL
                END AS bytecode_hash,
                CAST(blocknum AS UINTEGER) AS block_number,
                CAST(
                    row_number() OVER (
                        PARTITION BY blocknum
                        ORDER BY address
                    ) - 1
                    AS UINTEGER
                ) AS create_index,
                CAST({chain_id} AS UBIGINT) AS chain_id
            FROM read_csv_auto('{path}')
            WHERE address IS NOT NULL
              AND length(address) = 42
              AND blocknum IS NOT NULL
        )
        ORDER BY block_number DESC, create_index DESC;
        "#
    );
    conn.execute_batch(&sql)
        .context("import normalized contracts")?;
    print_kv_accent(
        "contracts",
        &format!(
            "{} · {:.1}s",
            format_count(count_table(conn, "zellic_contracts")?),
            started.elapsed().as_secs_f64()
        ),
    );
    Ok(())
}

fn import_counts(conn: &Connection) -> Result<()> {
    let started = Instant::now();
    print_kv("step", "count contracts per bytecode and block");
    conn.execute_batch(
        r#"
        CREATE TABLE zellic_bytecode_counts AS
        SELECT
            bytecode_hash AS code_hash,
            COUNT(*)::UBIGINT AS contract_count
        FROM zellic_contracts
        WHERE bytecode_hash IS NOT NULL
        GROUP BY bytecode_hash;

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
    .context("create normalized summary counts")?;
    print_kv_accent(
        "counts",
        &format!(
            "{} hashes · {} blocks · {:.1}s",
            format_count(count_table(conn, "zellic_bytecode_counts")?),
            format_count(count_table(conn, "zellic_block_counts")?),
            started.elapsed().as_secs_f64()
        ),
    );
    Ok(())
}

fn sql_path(path: &Path) -> String {
    path.display().to_string().replace('\'', "''")
}

fn sql_path_list<I, P>(paths: I) -> Result<String>
where
    I: IntoIterator<Item = P>,
    P: AsRef<Path>,
{
    let mut out = Vec::new();
    for path in paths {
        let path = path.as_ref();
        let canonical = std::fs::canonicalize(path)
            .with_context(|| format!("canonicalize {}", path.display()))?;
        out.push(format!("'{}'", sql_path(&canonical)));
    }
    if out.is_empty() {
        return Err(anyhow!("empty parquet path list"));
    }
    Ok(format!("[{}]", out.join(", ")))
}

fn sql_string_values<I, S>(values: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    values
        .into_iter()
        .map(|value| format!("('{}')", value.as_ref().replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
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
                "blink_load_test_{}_{}_{}",
                std::process::id(),
                name,
                unique
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn touch(&self, name: &str) -> PathBuf {
            let path = self.path.join(name);
            fs::write(&path, []).unwrap();
            path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn names(paths: &[PathBuf]) -> Vec<String> {
        paths
            .iter()
            .map(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn detect_inputs_requires_both_csv_files_for_csv_load() {
        let dir = TestDir::new("csv_pair");
        dir.touch("contracts.csv");

        let inputs = detect_inputs(&dir.path, "*.parquet").unwrap();

        assert!(!inputs.has_normalized_csv);
        assert!(inputs.parquet_files.is_empty());

        dir.touch("bytecodes.csv");
        let inputs = detect_inputs(&dir.path, "*.parquet").unwrap();

        assert!(inputs.has_normalized_csv);
        assert_eq!(inputs.csv_contracts, dir.path.join("contracts.csv"));
        assert_eq!(inputs.csv_bytecodes, dir.path.join("bytecodes.csv"));
    }

    #[test]
    fn list_parquet_files_filters_hidden_and_non_matching_files() {
        let dir = TestDir::new("parquet_filter");
        dir.touch("b.parquet");
        dir.touch("a.parquet");
        dir.touch(".hidden.parquet");
        dir.touch("contracts.csv");
        dir.touch("notes.txt");

        let files = list_parquet_files(&dir.path, "*.parquet").unwrap();

        assert_eq!(names(&files), vec!["a.parquet", "b.parquet"]);
    }

    #[test]
    fn detect_inputs_applies_parquet_glob() {
        let dir = TestDir::new("parquet_glob");
        dir.touch("ethereum__contracts__1_to_2.parquet");
        dir.touch("other__contracts__1_to_2.parquet");

        let inputs = detect_inputs(&dir.path, "ethereum__*.parquet").unwrap();

        assert_eq!(
            names(&inputs.parquet_files),
            vec!["ethereum__contracts__1_to_2.parquet"]
        );
    }

    #[test]
    fn detect_verifier_alliance_inputs_requires_both_tables() {
        let dir = TestDir::new("va_missing_table");
        fs::create_dir_all(dir.path.join("contract_deployments")).unwrap();

        let err = detect_verifier_alliance_inputs(Some(&dir.path)).unwrap_err();

        assert!(err.to_string().contains("--va needs both"));
    }

    #[test]
    fn detect_verifier_alliance_inputs_finds_required_parquet_files() {
        let dir = TestDir::new("va_tables");
        let deployments = dir.path.join("contract_deployments");
        let verifications = dir.path.join("verified_contracts");
        fs::create_dir_all(&deployments).unwrap();
        fs::create_dir_all(&verifications).unwrap();
        fs::write(deployments.join("contract_deployments_0_1.parquet"), []).unwrap();
        fs::write(verifications.join("verified_contracts_0_1.parquet"), []).unwrap();

        let inputs = detect_verifier_alliance_inputs(Some(&dir.path))
            .unwrap()
            .unwrap();

        assert_eq!(inputs.contract_deployments.len(), 1);
        assert_eq!(inputs.verified_contracts.len(), 1);
    }

    #[test]
    fn load_parquet_links_creates_symlinks_without_copying() {
        let src = TestDir::new("link_src");
        let dst = TestDir::new("link_dst");
        let parquet = src.touch("contracts__0000000001__0000000002.parquet");

        load_parquet_links(&src.path, &dst.path, std::slice::from_ref(&parquet), false).unwrap();

        let linked = dst.path.join("contracts__0000000001__0000000002.parquet");
        let metadata = fs::symlink_metadata(&linked).unwrap();
        assert!(metadata.file_type().is_symlink());
        assert_eq!(
            fs::canonicalize(linked).unwrap(),
            fs::canonicalize(parquet).unwrap()
        );
    }
}

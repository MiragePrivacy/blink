//! DuckDB-backed query layer for the dashboard.
//!
//! Architecture, in the order requests hit it:
//! - **Ingest** (`rollups`): every contract parquet file is immutable once
//!   written, so it is rolled up exactly once into native DuckDB tables —
//!   `contract_deployments_native` (one deduplicated, blob-free row per
//!   deployment) plus `rollup_block_counts` / `rollup_code_counts` summaries
//!   derived from the deduped delta. Overlapping files therefore cannot
//!   double-count.
//! - **Queries** (`queries`): every dashboard endpoint reads only those
//!   native tables; no request ever scans parquet. `POST /api/query` keeps a
//!   parquet-backed `contracts` view (`views`) for raw SQL access to the full
//!   bytecode columns.
//! - **Concurrency**: one writer connection owns ingest; a small pool of
//!   reader connections (clones of the same DuckDB instance, so they share
//!   the buffer cache) serves queries in parallel instead of serializing on a
//!   single mutex.

use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, Context, Result};
use duckdb::{params, AccessMode, Config, Connection};
use rayon::prelude::*;
use tokio::sync::Mutex;

mod explorer;
mod queries;
mod rollups;
mod sql;
mod views;

pub use queries::{
    CompilerCount, DeployBucket, LanguageCount, RecentContract, RecentCursor, RecentPage, SizeBin,
    StandardsBreakdown, Stats, VerifiedRatioBucket,
};
pub use rollups::invalidate_zellic_rollups;
pub use sql::SqlQueryResult;

/// Tuning knobs for [`Db::open`]. `Default` keeps DuckDB's own defaults and
/// sizes the reader pool from the host's core count.
#[derive(Debug, Clone, Default)]
pub struct DbOptions {
    pub read_only: bool,
    /// DuckDB instance-wide memory limit, e.g. `"2GB"`. Worth setting on
    /// small hosts; DuckDB otherwise assumes 80% of physical RAM.
    pub memory_limit: Option<String>,
    /// DuckDB instance-wide thread count.
    pub threads: Option<u64>,
    /// Reader connections for query traffic. `0` = auto.
    pub readers: usize,
}

#[derive(Clone)]
pub struct Db {
    writer: Arc<Mutex<Connection>>,
    readers: Arc<Vec<Arc<Mutex<Connection>>>>,
    next_reader: Arc<AtomicUsize>,
    data_dir: PathBuf,
    contracts_glob: String,
    read_only: bool,
}

impl Db {
    pub fn open_with_mode(data_dir: &Path, contracts_glob: &str, read_only: bool) -> Result<Self> {
        Self::open(
            data_dir,
            contracts_glob,
            DbOptions {
                read_only,
                ..DbOptions::default()
            },
        )
    }

    pub fn open(data_dir: &Path, contracts_glob: &str, options: DbOptions) -> Result<Self> {
        let started = Instant::now();
        if !options.read_only {
            std::fs::create_dir_all(data_dir)
                .with_context(|| format!("create data dir {}", data_dir.display()))?;
        }
        let db_path = data_dir.join("blink.duckdb");
        tracing::info!("opening dashboard database {}", db_path.display());

        let open_started = Instant::now();
        let writer = if options.read_only {
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
        tracing::info!(
            "dashboard database file opened in {:.1}s",
            open_started.elapsed().as_secs_f64()
        );
        configure_connection(&writer, data_dir, &options)?;

        if options.read_only {
            // Schema and rollups are owned by the writer process. If they are
            // not there yet, queries fail gracefully per-request.
            if !rollups::rollups_ready(&writer)? {
                tracing::warn!(
                    "read-only mode: rollup tables missing — run a writable `blink serve` once \
                     to build them; dashboard queries will fail until then"
                );
            }
        } else {
            explorer::cleanup_stale_build(&writer)?;
            tracing::info!("checking deployment rollups");
            views::ensure_schema(&writer)?;
            rollups::ensure_rollup_schema(&writer)?;
            rollups::sync_rollups(&writer, data_dir, contracts_glob)?;
        }

        let checkpoint_count = crate::checkpoints::load_runtime(&writer)?;
        if checkpoint_count == 0 {
            tracing::warn!(
                "no block-time checkpoints loaded; run `blink checkpoints` for accurate chart dates"
            );
        }

        let files = rollups::list_contract_parquet_files(data_dir, contracts_glob)?;
        tracing::info!(
            "preparing dashboard query views ({} parquet files)",
            files.len()
        );
        views::rebuild_query_views(&writer, &files)?;

        let reader_count = if options.readers == 0 {
            default_reader_count()
        } else {
            options.readers
        };
        let mut readers = Vec::with_capacity(reader_count);
        for _ in 0..reader_count {
            let conn = writer
                .try_clone()
                .context("clone duckdb reader connection")?;
            // Temp views live in a per-connection schema, so each reader
            // needs its own copy.
            views::rebuild_query_views(&conn, &files)?;
            readers.push(Arc::new(Mutex::new(conn)));
        }

        tracing::info!(
            "dashboard database ready in {:.1}s ({} readers)",
            started.elapsed().as_secs_f64(),
            reader_count
        );

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers: Arc::new(readers),
            next_reader: Arc::new(AtomicUsize::new(0)),
            data_dir: data_dir.to_path_buf(),
            contracts_glob: contracts_glob.to_string(),
            read_only: options.read_only,
        })
    }

    /// Pick up newly written parquet files: ingest them into the rollups and
    /// point the raw-SQL `contracts` view at the new file list. Called by the
    /// background tail loop after each extraction.
    pub async fn refresh(&self) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.refresh_blocking())
            .await
            .map_err(|e| anyhow!("join error: {}", e))?
    }

    fn refresh_blocking(&self) -> Result<()> {
        if !self.read_only {
            let writer = self.writer.blocking_lock();
            rollups::sync_rollups(&writer, &self.data_dir, &self.contracts_glob)?;
        }
        let files = rollups::list_contract_parquet_files(&self.data_dir, &self.contracts_glob)?;
        {
            let writer = self.writer.blocking_lock();
            views::rebuild_parquet_views(&writer, &files)?;
        }
        for reader in self.readers.iter() {
            let conn = reader.blocking_lock();
            views::rebuild_parquet_views(&conn, &files)?;
        }
        Ok(())
    }

    /// Bring the materialized SQL-explorer table (`contract_metadata_native`)
    /// up to date if its inputs changed. Safe to run while serving: queries
    /// use the previous copy (or the live-join fallback) until the swap, and
    /// the writer lock is released between build slices. Returns whether a
    /// rebuild ran.
    pub async fn refresh_explorer(&self) -> Result<bool> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.refresh_explorer_blocking())
            .await
            .map_err(|e| anyhow!("join error: {}", e))?
    }

    /// Run raw SQL on the writer connection. Intended for tests and admin
    /// tooling, not request paths.
    pub async fn execute_batch(&self, sql: String) -> Result<()> {
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            writer
                .blocking_lock()
                .execute_batch(&sql)
                .context("execute batch")
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn record_block_checkpoint(
        &self,
        chain_id: u64,
        block_number: u64,
        timestamp: i64,
    ) -> Result<()> {
        if self.read_only {
            return Err(anyhow!("cannot record block checkpoint in read-only mode"));
        }
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = writer.blocking_lock();
            crate::checkpoints::ensure_schema(&conn)?;
            crate::checkpoints::upsert(&conn, chain_id, block_number, timestamp)?;
            crate::blocks::upsert_runtime_checkpoint(chain_id, block_number, timestamp);
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub async fn latest_checkpoint_block(&self, chain_id: u64) -> Result<Option<u64>> {
        self.run_read(move |conn| {
            if !table_exists(conn, "chain_block_checkpoints")? {
                return Ok(None);
            }
            let block = conn
                .query_row(
                    "SELECT MAX(block_number) FROM chain_block_checkpoints WHERE chain_id = ?",
                    params![chain_id],
                    |row| row.get::<_, Option<u64>>(0),
                )
                .unwrap_or(None);
            Ok(block)
        })
        .await
    }

    pub async fn import_verifier_alliance(
        &self,
        verifier_alliance_dir: PathBuf,
        chain_id: u64,
    ) -> Result<bool> {
        if self.read_only {
            return Err(anyhow!(
                "cannot import Verifier Alliance data in read-only mode"
            ));
        }
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || -> Result<bool> {
            let conn = writer.blocking_lock();
            crate::load::import_verifier_alliance_from_dir(&conn, &verifier_alliance_dir, chain_id)
        })
        .await
        .map_err(|error| anyhow!("join error: {error}"))?
    }

    /// Analyze bytecodes captured by the live tail and persist metadata by
    /// runtime-code hash. Tail batches are small, so this keeps today's
    /// compiler and standards widgets current without a separate decoder
    /// process competing for the DuckDB write lock.
    pub async fn decode_live_bytecodes(&self, bytecodes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<u64> {
        if self.read_only || bytecodes.is_empty() {
            return Ok(0);
        }
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || -> Result<u64> {
            let analyzed = bytecodes
                .into_par_iter()
                .filter(|(code_hash, code)| {
                    code_hash.len() == 32 && !code.is_empty() && code.len() <= 65_536
                })
                .map(|(code_hash, code)| {
                    let metadata = crate::decode::bytecode_meta::analyze(&code);
                    (code_hash, metadata)
                })
                .collect::<Vec<_>>();
            let mut conn = writer.blocking_lock();
            crate::decode::flush_hash_batch(&mut conn, &analyzed)
        })
        .await
        .map_err(|error| anyhow!("join error: {error}"))?
    }

    fn reader(&self) -> Arc<Mutex<Connection>> {
        if self.readers.is_empty() {
            return self.writer.clone();
        }
        let idx = self.next_reader.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        self.readers[idx].clone()
    }

    /// Run a closure against a pooled reader connection on the blocking
    /// thread pool. All dashboard queries go through this.
    async fn run_read<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = self.reader();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            f(&conn)
        })
        .await
        .map_err(|e| anyhow!("join error: {}", e))?
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub(crate) fn contracts_glob(&self) -> &str {
        &self.contracts_glob
    }
}

fn default_reader_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() / 2)
        .unwrap_or(2)
        .clamp(2, 4)
}

fn configure_connection(conn: &Connection, data_dir: &Path, options: &DbOptions) -> Result<()> {
    conn.execute_batch("SET preserve_insertion_order=false;")
        .context("configure DuckDB insertion-order preservation")?;
    if let Some(memory_limit) = options.memory_limit.as_deref() {
        conn.execute_batch(&format!(
            "SET memory_limit='{}';",
            memory_limit.replace('\'', "''")
        ))
        .context("configure DuckDB memory limit")?;
    }
    if let Some(threads) = options.threads {
        conn.execute_batch(&format!("SET threads={};", threads.max(1)))
            .context("configure DuckDB threads")?;
    }
    let temp_dir = data_dir.join(".blink").join("duckdb-tmp");
    match std::fs::create_dir_all(&temp_dir) {
        Ok(()) => conn
            .execute_batch(&format!(
                "SET temp_directory='{}';",
                sql_string_literal(&temp_dir)
            ))
            .context("configure DuckDB temp directory")?,
        Err(err) => tracing::warn!(
            "could not create DuckDB temp dir {}: {}; using DuckDB default temp directory",
            temp_dir.display(),
            err
        ),
    }
    Ok(())
}

fn sql_string_literal(value: &Path) -> String {
    value.display().to_string().replace('\'', "''")
}

pub(crate) fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            params![table],
            |row| row.get(0),
        )
        .unwrap_or(0);
    Ok(count > 0)
}

pub(crate) fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
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

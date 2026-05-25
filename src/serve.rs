//! Public dashboard HTTP server.
//!
//! Hosts the JSON API consumed by the static frontend and serves the
//! frontend itself out of `--static-dir`. Optional background tasks:
//! - `--metadata-sync` runs [`crate::metadata::metadata_sync_loop`] continuously.
//! - `--tail-rpc URL` polls the chain head and extracts newly produced
//!   blocks into a separate `tail__*.parquet` file (see [`crate::extract::tail`]).
//!
//! Endpoints (all return JSON):
//! - `GET /api/stats` — totals, verified pct, last block, metadata sync coverage.
//! - `GET /api/runtime` — serve-mode flags and background loop health.
//! - `GET /api/deploys-over-time?bucket=hour|day|week|month` — time-series.
//! - `GET /api/verified-ratio?bucket=...` — verified vs unverified vs unknown.
//! - `GET /api/bytecode-sizes` — semantic histogram of `n_code_bytes`.
//! - `GET /api/compilers?limit=N` — top known compiler versions.
//! - `GET /api/recent?limit=N` — newest contracts with verification join.
//! - `POST /api/query` — read-only SQL over dashboard query views.

use std::{
    collections::HashMap,
    future::Future,
    hash::Hash,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::{cors::CorsLayer, services::ServeDir, trace::TraceLayer};

use crate::{
    blocks::blocks_per_day,
    cli::ServeArgs,
    db::{Db, RecentCursor},
    metadata::{
        metadata_sync_loop, resolve_api_key, EtherscanClient, MetadataSyncOptions, SourcifyClient,
        VerificationSources,
    },
};

#[derive(Clone)]
struct AppState {
    db: Db,
    runtime: Arc<RuntimeState>,
    cache: Arc<ApiCache>,
}

const API_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Default)]
struct ApiCache {
    stats: CacheCell<crate::db::Stats>,
    deploys: CacheMap<u64, Vec<crate::db::DeployBucket>>,
    verified: CacheMap<u64, Vec<crate::db::VerifiedRatioBucket>>,
    bytecode_sizes: CacheCell<Vec<crate::db::SizeBin>>,
    compilers: CacheMap<u32, (Vec<crate::db::CompilerCount>, u64)>,
    languages: CacheCell<Vec<crate::db::LanguageCount>>,
    standards: CacheCell<crate::db::StandardsBreakdown>,
}

struct CacheCell<T> {
    value: Mutex<Option<(Instant, T)>>,
}

impl<T> Default for CacheCell<T> {
    fn default() -> Self {
        Self {
            value: Mutex::new(None),
        }
    }
}

impl<T: Clone> CacheCell<T> {
    async fn get_or_try_update<F>(&self, ttl: Duration, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        {
            let guard = self.value.lock().await;
            if let Some((stored_at, value)) = guard.as_ref() {
                if stored_at.elapsed() <= ttl {
                    return Ok(value.clone());
                }
            }
        }

        let value = future.await?;
        *self.value.lock().await = Some((Instant::now(), value.clone()));
        Ok(value)
    }
}

struct CacheMap<K, V> {
    values: Mutex<HashMap<K, (Instant, V)>>,
}

impl<K, V> Default for CacheMap<K, V> {
    fn default() -> Self {
        Self {
            values: Mutex::new(HashMap::new()),
        }
    }
}

impl<K, V> CacheMap<K, V>
where
    K: Copy + Eq + Hash,
    V: Clone,
{
    async fn get_or_try_update<F>(&self, key: K, ttl: Duration, future: F) -> Result<V>
    where
        F: Future<Output = Result<V>>,
    {
        {
            let guard = self.values.lock().await;
            if let Some((stored_at, value)) = guard.get(&key) {
                if stored_at.elapsed() <= ttl {
                    return Ok(value.clone());
                }
            }
        }

        let value = future.await?;
        self.values
            .lock()
            .await
            .insert(key, (Instant::now(), value.clone()));
        Ok(value)
    }
}

#[derive(Debug)]
struct RuntimeState {
    read_only: bool,
    metadata_sync_enabled: bool,
    tail_enabled: bool,
    tail_interval_secs: u64,
    snapshot: Mutex<RuntimeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize)]
struct RuntimeSnapshot {
    metadata_sync_running: bool,
    metadata_last_ok_at: Option<DateTime<Utc>>,
    metadata_last_processed: Option<u64>,
    metadata_last_error_at: Option<DateTime<Utc>>,
    metadata_last_error: Option<String>,
    tail_running: bool,
    tail_last_ok_at: Option<DateTime<Utc>>,
    tail_last_block: Option<u64>,
    tail_last_rows: Option<u64>,
    tail_last_error_at: Option<DateTime<Utc>>,
    tail_last_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct RuntimeResponse {
    read_only: bool,
    metadata_sync_enabled: bool,
    tail_enabled: bool,
    tail_interval_secs: u64,
    #[serde(flatten)]
    snapshot: RuntimeSnapshot,
}

struct TailLoopConfig {
    rpc: String,
    interval: Duration,
    confirmations: u64,
    batch_size: usize,
    max_concurrent: usize,
    data_dir: std::path::PathBuf,
}

impl RuntimeState {
    fn new(
        read_only: bool,
        metadata_sync_enabled: bool,
        tail_enabled: bool,
        tail_interval_secs: u64,
    ) -> Self {
        Self {
            read_only,
            metadata_sync_enabled,
            tail_enabled,
            tail_interval_secs,
            snapshot: Mutex::new(RuntimeSnapshot::default()),
        }
    }

    async fn response(&self) -> RuntimeResponse {
        RuntimeResponse {
            read_only: self.read_only,
            metadata_sync_enabled: self.metadata_sync_enabled,
            tail_enabled: self.tail_enabled,
            tail_interval_secs: self.tail_interval_secs,
            snapshot: self.snapshot.lock().await.clone(),
        }
    }

    async fn mark_metadata_sync_start(&self) {
        self.snapshot.lock().await.metadata_sync_running = true;
    }

    async fn mark_metadata_sync_ok(&self, processed: u64) {
        let mut snapshot = self.snapshot.lock().await;
        snapshot.metadata_sync_running = false;
        snapshot.metadata_last_ok_at = Some(Utc::now());
        snapshot.metadata_last_processed = Some(processed);
        snapshot.metadata_last_error = None;
        snapshot.metadata_last_error_at = None;
    }

    async fn mark_metadata_sync_error(&self, message: String) {
        let mut snapshot = self.snapshot.lock().await;
        snapshot.metadata_sync_running = false;
        snapshot.metadata_last_error_at = Some(Utc::now());
        snapshot.metadata_last_error = Some(message);
    }

    async fn mark_tail_start(&self) {
        self.snapshot.lock().await.tail_running = true;
    }

    async fn mark_tail_ok(&self, end_block: Option<u64>, rows: u64) {
        let mut snapshot = self.snapshot.lock().await;
        snapshot.tail_running = false;
        snapshot.tail_last_ok_at = Some(Utc::now());
        if let Some(block) = end_block {
            snapshot.tail_last_block = Some(block);
        }
        snapshot.tail_last_rows = Some(rows);
        snapshot.tail_last_error = None;
        snapshot.tail_last_error_at = None;
    }

    async fn mark_tail_error(&self, message: String) {
        let mut snapshot = self.snapshot.lock().await;
        snapshot.tail_running = false;
        snapshot.tail_last_error_at = Some(Utc::now());
        snapshot.tail_last_error = Some(message);
    }
}

pub async fn run_serve(args: ServeArgs) -> Result<()> {
    if args.read_only && (args.metadata_sync || args.tail_rpc.is_some()) {
        tracing::warn!(
            "--read-only is set; ignoring --metadata-sync / --tail-rpc (both require a write lock)"
        );
    }
    let db = Db::open_with_mode(&args.data_dir, &args.contracts_glob, args.read_only)?;
    let metadata_sync_enabled = args.metadata_sync && !args.read_only;
    let tail_enabled = args.tail_rpc.is_some() && !args.read_only;
    let runtime = Arc::new(RuntimeState::new(
        args.read_only,
        metadata_sync_enabled,
        tail_enabled,
        args.tail_interval_secs.max(15),
    ));
    let state = AppState {
        db: db.clone(),
        runtime: runtime.clone(),
        cache: Arc::new(ApiCache::default()),
    };

    if metadata_sync_enabled {
        // Etherscan is the primary verifier when a key is present; Sourcify is
        // kept as a free fallback and no-key mode.
        let sourcify = Some(Arc::new(SourcifyClient::new(
            args.sourcify_url.clone(),
            args.chain_id,
        )?));
        let etherscan = match resolve_api_key(args.etherscan_api_key.clone()) {
            Ok(key) => Some(Arc::new(EtherscanClient::new(
                args.etherscan_url.clone(),
                key,
                args.chain_id,
            )?)),
            Err(_) => {
                tracing::info!(
                    "no Etherscan API key — background metadata sync will use Sourcify only"
                );
                None
            }
        };
        let db_bg = db.clone();
        let rps = args.metadata_sync_rate_limit_rps;
        let recheck = args.recheck_unverified_after_secs;
        let runtime_bg = runtime.clone();
        tokio::spawn(async move {
            background_metadata_sync_loop(
                db_bg,
                VerificationSources {
                    etherscan,
                    sourcify,
                },
                rps,
                recheck,
                runtime_bg,
            )
            .await;
        });
    }

    if let Some(rpc) = args.tail_rpc.clone().filter(|_| !args.read_only) {
        let db_bg = db.clone();
        let config = TailLoopConfig {
            rpc,
            interval: Duration::from_secs(args.tail_interval_secs.max(15)),
            confirmations: args.tail_confirmations,
            batch_size: args.tail_batch_size,
            max_concurrent: args.tail_max_concurrent_requests,
            data_dir: args.data_dir.clone(),
        };
        let runtime_bg = runtime.clone();
        tokio::spawn(async move {
            background_tail_loop(db_bg, config, runtime_bg).await;
        });
    }

    let app = Router::new()
        .route("/api/stats", get(stats_handler))
        .route("/api/runtime", get(runtime_handler))
        .route("/api/deploys-over-time", get(deploys_handler))
        .route("/api/verified-ratio", get(verified_handler))
        .route("/api/bytecode-sizes", get(bytecode_sizes_handler))
        .route("/api/compilers", get(compilers_handler))
        .route("/api/languages", get(languages_handler))
        .route("/api/standards", get(standards_handler))
        .route("/api/recent", get(recent_handler))
        .route("/api/query", post(query_handler))
        .fallback_service(ServeDir::new(&args.static_dir).append_index_html_on_directories(true))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr: SocketAddr = args
        .bind
        .parse()
        .with_context(|| format!("invalid bind address {}", args.bind))?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {}", addr))?;
    tracing::info!("serving blink dashboard on http://{}", addr);
    tracing::info!(" data dir: {}", args.data_dir.display());
    tracing::info!(" static dir: {}", args.static_dir.display());
    axum::serve(listener, app)
        .await
        .context("axum server failed")
}

#[derive(Serialize)]
struct ApiError {
    error: String,
}

struct AppError {
    status: StatusCode,
    error: anyhow::Error,
}

impl AppError {
    fn bad_request(error: impl Into<anyhow::Error>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: error.into(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let body = Json(ApiError {
            error: format!("{:#}", self.error),
        });
        (self.status, body).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(err: E) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: err.into(),
        }
    }
}

async fn stats_handler(State(state): State<AppState>) -> Result<Json<crate::db::Stats>, AppError> {
    let stats = state
        .cache
        .stats
        .get_or_try_update(API_CACHE_TTL, state.db.stats())
        .await?;
    Ok(Json(stats))
}

async fn runtime_handler(State(state): State<AppState>) -> Json<RuntimeResponse> {
    Json(state.runtime.response().await)
}

#[derive(Deserialize)]
struct BucketQuery {
    /// `day` (default), `week`, or a raw block count
    #[serde(default)]
    bucket: Option<String>,
}

fn parse_bucket(q: &BucketQuery, anchor_block: u64) -> u64 {
    match q.bucket.as_deref() {
        None | Some("day") => blocks_per_day(anchor_block),
        Some("hour") => blocks_per_day(anchor_block) / 24,
        Some("week") => blocks_per_day(anchor_block) * 7,
        Some("month") => blocks_per_day(anchor_block) * 30,
        Some(other) => other
            .parse::<u64>()
            .unwrap_or_else(|_| blocks_per_day(anchor_block)),
    }
}

async fn deploys_handler(
    State(state): State<AppState>,
    Query(q): Query<BucketQuery>,
) -> Result<Json<DeploysResponse>, AppError> {
    let highest = state.db.highest_block().await?.unwrap_or(0);
    let bucket = parse_bucket(&q, highest);
    let buckets = state
        .cache
        .deploys
        .get_or_try_update(bucket, API_CACHE_TTL, state.db.deploys_over_time(bucket))
        .await?;
    Ok(Json(DeploysResponse {
        bucket_blocks: bucket,
        buckets,
    }))
}

async fn verified_handler(
    State(state): State<AppState>,
    Query(q): Query<BucketQuery>,
) -> Result<Json<VerifiedResponse>, AppError> {
    let highest = state.db.highest_block().await?.unwrap_or(0);
    let bucket = parse_bucket(&q, highest);
    let buckets = state
        .cache
        .verified
        .get_or_try_update(
            bucket,
            API_CACHE_TTL,
            state.db.verified_ratio_over_time(bucket),
        )
        .await?;
    Ok(Json(VerifiedResponse {
        bucket_blocks: bucket,
        buckets,
    }))
}

async fn bytecode_sizes_handler(
    State(state): State<AppState>,
) -> Result<Json<SizeResponse>, AppError> {
    let bins_out = state
        .cache
        .bytecode_sizes
        .get_or_try_update(API_CACHE_TTL, state.db.bytecode_size_distribution())
        .await?;
    Ok(Json(SizeResponse { bins: bins_out }))
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<u32>,
}

#[derive(Deserialize)]
struct PageQuery {
    limit: Option<u32>,
    before_block: Option<u64>,
    before_create_index: Option<u64>,
}

async fn compilers_handler(
    State(state): State<AppState>,
    Query(q): Query<LimitQuery>,
) -> Result<Json<CompilersResponse>, AppError> {
    let limit = q.limit.unwrap_or(15);
    let (compilers, total_known) = state
        .cache
        .compilers
        .get_or_try_update(limit, API_CACHE_TTL, async {
            Ok((
                state.db.top_compilers(limit).await?,
                state.db.compiler_version_total().await?,
            ))
        })
        .await?;
    Ok(Json(CompilersResponse {
        compilers,
        total_known,
    }))
}

async fn recent_handler(
    State(state): State<AppState>,
    Query(q): Query<PageQuery>,
) -> Result<Json<RecentResponse>, AppError> {
    let limit = q.limit.unwrap_or(20);
    let cursor = match (q.before_block, q.before_create_index) {
        (Some(block_number), Some(create_index)) => Some(RecentCursor {
            block_number,
            create_index,
        }),
        _ => None,
    };
    let page = state.db.recent_contracts(limit, cursor).await?;
    Ok(Json(RecentResponse {
        contracts: page.contracts,
        limit,
        has_more: page.has_more,
    }))
}

#[derive(Deserialize)]
struct SqlQueryRequest {
    sql: String,
    limit: Option<u32>,
}

async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<SqlQueryRequest>,
) -> Result<Json<crate::db::SqlQueryResult>, AppError> {
    state
        .db
        .query_sql(req.sql, req.limit.unwrap_or(100))
        .await
        .map(Json)
        .map_err(AppError::bad_request)
}

async fn languages_handler(
    State(state): State<AppState>,
) -> Result<Json<LanguagesResponse>, AppError> {
    let languages = state
        .cache
        .languages
        .get_or_try_update(API_CACHE_TTL, state.db.language_distribution())
        .await?;
    Ok(Json(LanguagesResponse { languages }))
}

async fn standards_handler(
    State(state): State<AppState>,
) -> Result<Json<crate::db::StandardsBreakdown>, AppError> {
    let standards = state
        .cache
        .standards
        .get_or_try_update(API_CACHE_TTL, state.db.standards_breakdown())
        .await?;
    Ok(Json(standards))
}

#[derive(Serialize)]
struct DeploysResponse {
    bucket_blocks: u64,
    buckets: Vec<crate::db::DeployBucket>,
}

#[derive(Serialize)]
struct VerifiedResponse {
    bucket_blocks: u64,
    buckets: Vec<crate::db::VerifiedRatioBucket>,
}

#[derive(Serialize)]
struct SizeResponse {
    bins: Vec<crate::db::SizeBin>,
}

#[derive(Serialize)]
struct CompilersResponse {
    compilers: Vec<crate::db::CompilerCount>,
    total_known: u64,
}

#[derive(Serialize)]
struct RecentResponse {
    contracts: Vec<crate::db::RecentContract>,
    limit: u32,
    has_more: bool,
}

#[derive(Serialize)]
struct LanguagesResponse {
    languages: Vec<crate::db::LanguageCount>,
}

async fn background_metadata_sync_loop(
    db: Db,
    sources: VerificationSources,
    rps: u32,
    recheck_after_secs: i64,
    runtime: Arc<RuntimeState>,
) {
    tracing::info!(
        "background metadata sync loop starting (rps={}, sourcify={}, etherscan={})",
        rps,
        sources.sourcify.is_some(),
        sources.etherscan.is_some()
    );
    loop {
        runtime.mark_metadata_sync_start().await;
        match metadata_sync_loop(
            db.clone(),
            sources.clone(),
            MetadataSyncOptions {
                limit: 0,
                rate_limit_rps: rps,
                recheck_after_secs,
                newest_first: true,
            },
        )
        .await
        {
            Ok(stats) if stats.processed == 0 => {
                runtime.mark_metadata_sync_ok(stats.processed).await;
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            Ok(stats) => {
                runtime.mark_metadata_sync_ok(stats.processed).await;
                tracing::info!(
                    "metadata sync pass done: processed={} verified={} unverified={} failed={}",
                    stats.processed,
                    stats.verified,
                    stats.unverified,
                    stats.failed
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(err) => {
                let msg = format!("{:#}", err);
                runtime.mark_metadata_sync_error(msg.clone()).await;
                if msg.contains("API key") {
                    tracing::error!(
                        "background metadata sync loop disabled: {}. Restart the server with a valid key.",
                        msg
                    );
                    return;
                }
                tracing::warn!("metadata sync pass failed: {}", msg);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
    }
}

async fn background_tail_loop(db: Db, config: TailLoopConfig, runtime: Arc<RuntimeState>) {
    tracing::info!(
        "background tail loop starting (rpc={}, interval={:?}, confirmations={})",
        config.rpc,
        config.interval,
        config.confirmations
    );
    loop {
        runtime.mark_tail_start().await;
        match crate::extract::tail::tail_once(
            &db,
            &config.rpc,
            config.confirmations,
            config.batch_size,
            config.max_concurrent,
            &config.data_dir,
        )
        .await
        {
            Ok(Some(report)) => {
                runtime
                    .mark_tail_ok(Some(report.end_block), report.rows as u64)
                    .await;
                tracing::info!(
                    "tail extracted blocks {}-{} ({} contracts)",
                    report.start_block,
                    report.end_block,
                    report.rows
                );
            }
            Ok(None) => {
                runtime.mark_tail_ok(None, 0).await;
                tracing::debug!("tail: no new blocks");
            }
            Err(err) => {
                let msg = format!("{:#}", err);
                runtime.mark_tail_error(msg.clone()).await;
                tracing::warn!("tail failed: {}", msg);
            }
        }
        tokio::time::sleep(config.interval).await;
    }
}

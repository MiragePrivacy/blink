//! Public dashboard HTTP server.
//!
//! Hosts the JSON API consumed by the separate dashboard frontend.
//! Optional background tasks:
//! - repeated `--rpc URL` flags poll one or more chain heads and extract
//!   newly produced blocks into separate `tail__chain_*` parquet files.
//!
//! Endpoints (all return JSON):
//! - `GET /api/stats` — totals, verified pct, last block, verification coverage.
//! - `GET /api/runtime` — serve-mode flags and background loop health.
//! - `GET /api/deploys-over-time?range=hour|day|week|month|year` — time-series.
//! - `GET /api/verified-ratio?range=...` — verified vs unverified vs unknown.
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
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Json},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_scalar::{Scalar, Servable};

use crate::{
    blocks::blocks_per_day,
    chains::{self, ChainInfo},
    cli::ServeArgs,
    db::{Db, RecentCursor},
};

#[derive(Clone)]
struct AppState {
    db: Db,
    runtime: Arc<RuntimeState>,
    cache: Arc<ApiCache>,
}

const API_CACHE_TTL: Duration = Duration::from_secs(600);
const TAIL_START_DELAY: Duration = Duration::from_secs(15);
const DEFAULT_COMPILER_LIMIT: u32 = 12;
const DEFAULT_RECENT_LIMIT: u32 = 20;
const INITIAL_DEPLOYS_RANGE: &str = "day";
const INITIAL_VERIFIED_RANGE: &str = "week";
const INITIAL_AGGREGATE_RANGE: &str = "month";
const API_TAG: &str = "Dashboard";

#[derive(OpenApi)]
#[openapi(tags(
    (name = API_TAG, description = "Blink dashboard and contract intelligence endpoints")
))]
struct ApiDoc;

#[derive(Default)]
struct ApiCache {
    stats: CacheMap<u64, crate::db::Stats>,
    deploys: CacheMap<BucketCacheKey, Vec<crate::db::DeployBucket>>,
    verified: CacheMap<BucketCacheKey, Vec<crate::db::VerifiedRatioBucket>>,
    bytecode_sizes: CacheMap<RangeCacheKey, Vec<crate::db::SizeBin>>,
    compilers: CacheMap<LimitRangeCacheKey, (Vec<crate::db::CompilerCount>, u64)>,
    recent: CacheMap<RecentCacheKey, crate::db::RecentPage>,
    languages: CacheMap<u64, Vec<crate::db::LanguageCount>>,
    standards: CacheMap<RangeCacheKey, crate::db::StandardsBreakdown>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct BucketCacheKey {
    chain_id: u64,
    bucket: u64,
    start_block: Option<u64>,
    end_block: Option<u64>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct RangeCacheKey {
    chain_id: u64,
    start_block: Option<u64>,
    end_block: Option<u64>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct LimitRangeCacheKey {
    chain_id: u64,
    limit: u32,
    start_block: Option<u64>,
    end_block: Option<u64>,
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct RecentCacheKey {
    chain_id: u64,
    limit: u32,
    before_block: Option<u64>,
    before_create_index: Option<u64>,
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
                if stored_at.elapsed() > ttl {
                    tracing::debug!("serving stale dashboard cache entry");
                }
                return Ok(value.clone());
            }
        }

        let value = future.await?;
        self.insert(key, value.clone()).await;
        Ok(value)
    }

    async fn insert(&self, key: K, value: V) {
        self.values
            .lock()
            .await
            .insert(key, (Instant::now(), value));
    }

    async fn get(&self, key: K) -> Option<V> {
        self.values
            .lock()
            .await
            .get(&key)
            .map(|(_, value)| value.clone())
    }
}

#[derive(Debug)]
struct RuntimeState {
    read_only: bool,
    tail_enabled: bool,
    tail_interval_secs: u64,
    snapshot: Mutex<RuntimeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, ToSchema)]
struct RuntimeSnapshot {
    tail_running: bool,
    tail_running_count: u64,
    tail_last_ok_at: Option<DateTime<Utc>>,
    tail_last_block: Option<u64>,
    tail_last_rows: Option<u64>,
    tail_last_error_at: Option<DateTime<Utc>>,
    tail_last_error: Option<String>,
    tail_chains: Vec<ChainRuntimeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, ToSchema)]
struct ChainRuntimeSnapshot {
    chain_id: u64,
    tail_running: bool,
    tail_running_count: u64,
    tail_last_ok_at: Option<DateTime<Utc>>,
    tail_last_block: Option<u64>,
    tail_last_rows: Option<u64>,
    tail_last_error_at: Option<DateTime<Utc>>,
    tail_last_error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct RuntimeResponse {
    read_only: bool,
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
    fn new(read_only: bool, tail_enabled: bool, tail_interval_secs: u64) -> Self {
        Self {
            read_only,
            tail_enabled,
            tail_interval_secs,
            snapshot: Mutex::new(RuntimeSnapshot::default()),
        }
    }

    async fn response(&self) -> RuntimeResponse {
        RuntimeResponse {
            read_only: self.read_only,
            tail_enabled: self.tail_enabled,
            tail_interval_secs: self.tail_interval_secs,
            snapshot: self.snapshot.lock().await.clone(),
        }
    }

    async fn mark_tail_start(&self, chain_id: Option<u64>) {
        let mut snapshot = self.snapshot.lock().await;
        snapshot.tail_running_count = snapshot.tail_running_count.saturating_add(1);
        snapshot.tail_running = true;
        if let Some(chain_id) = chain_id {
            let chain = chain_runtime_snapshot_mut(&mut snapshot, chain_id);
            chain.tail_running_count = chain.tail_running_count.saturating_add(1);
            chain.tail_running = true;
        }
    }

    async fn mark_tail_ok(&self, chain_id: Option<u64>, end_block: Option<u64>, rows: u64) {
        let mut snapshot = self.snapshot.lock().await;
        snapshot.tail_running_count = snapshot.tail_running_count.saturating_sub(1);
        snapshot.tail_running = snapshot.tail_running_count > 0;
        snapshot.tail_last_ok_at = Some(Utc::now());
        if let Some(block) = end_block {
            snapshot.tail_last_block = Some(block);
        }
        snapshot.tail_last_rows = Some(rows);
        snapshot.tail_last_error = None;
        snapshot.tail_last_error_at = None;
        if let Some(chain_id) = chain_id {
            let last_ok_at = snapshot.tail_last_ok_at;
            let chain = chain_runtime_snapshot_mut(&mut snapshot, chain_id);
            chain.tail_running_count = chain.tail_running_count.saturating_sub(1);
            chain.tail_running = chain.tail_running_count > 0;
            chain.tail_last_ok_at = last_ok_at;
            if let Some(block) = end_block {
                chain.tail_last_block = Some(block);
            }
            chain.tail_last_rows = Some(rows);
            chain.tail_last_error = None;
            chain.tail_last_error_at = None;
        }
    }

    async fn mark_tail_ready(&self, chain_id: u64, block: Option<u64>) {
        let mut snapshot = self.snapshot.lock().await;
        let now = Some(Utc::now());
        snapshot.tail_last_ok_at = now;
        if let Some(block) = block {
            snapshot.tail_last_block = Some(block);
        }
        snapshot.tail_last_rows = Some(0);
        snapshot.tail_last_error = None;
        snapshot.tail_last_error_at = None;

        let chain = chain_runtime_snapshot_mut(&mut snapshot, chain_id);
        chain.tail_last_ok_at = now;
        chain.tail_last_block = block;
        chain.tail_last_rows = Some(0);
        chain.tail_last_error = None;
        chain.tail_last_error_at = None;
    }

    async fn mark_tail_error(&self, chain_id: Option<u64>, message: String) {
        let mut snapshot = self.snapshot.lock().await;
        snapshot.tail_running_count = snapshot.tail_running_count.saturating_sub(1);
        snapshot.tail_running = snapshot.tail_running_count > 0;
        snapshot.tail_last_error_at = Some(Utc::now());
        snapshot.tail_last_error = Some(message);
        if let Some(chain_id) = chain_id {
            let last_error_at = snapshot.tail_last_error_at;
            let last_error = snapshot.tail_last_error.clone();
            let chain = chain_runtime_snapshot_mut(&mut snapshot, chain_id);
            chain.tail_running_count = chain.tail_running_count.saturating_sub(1);
            chain.tail_running = chain.tail_running_count > 0;
            chain.tail_last_error_at = last_error_at;
            chain.tail_last_error = last_error;
        }
    }
}

fn chain_runtime_snapshot_mut(
    snapshot: &mut RuntimeSnapshot,
    chain_id: u64,
) -> &mut ChainRuntimeSnapshot {
    if let Some(index) = snapshot
        .tail_chains
        .iter()
        .position(|chain| chain.chain_id == chain_id)
    {
        return &mut snapshot.tail_chains[index];
    }
    snapshot.tail_chains.push(ChainRuntimeSnapshot {
        chain_id,
        ..ChainRuntimeSnapshot::default()
    });
    let index = snapshot.tail_chains.len() - 1;
    &mut snapshot.tail_chains[index]
}

async fn seed_runtime_snapshot(db: &Db, runtime: &RuntimeState) {
    for chain in chains::supported_chains() {
        match db.highest_contract_block(chain.chain_id).await {
            Ok(block) => runtime.mark_tail_ready(chain.chain_id, block).await,
            Err(err) => tracing::warn!(
                "could not seed runtime state for chain_id={}: {:#}",
                chain.chain_id,
                err
            ),
        }
    }
}

pub async fn run_serve(args: ServeArgs) -> Result<()> {
    if args.read_only && !args.rpc.is_empty() {
        tracing::warn!(
            "--read-only is set; ignoring --rpc values (background extraction requires a write lock)"
        );
    }
    let db = Db::open_with_mode(&args.data_dir, &args.contracts_glob, args.read_only)?;
    let rpcs = args
        .rpc
        .iter()
        .map(|rpc| rpc.trim())
        .filter(|rpc| !rpc.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let tail_enabled = !rpcs.is_empty() && !args.read_only;
    let runtime = Arc::new(RuntimeState::new(
        args.read_only,
        tail_enabled,
        args.tail_interval_secs.max(15),
    ));
    let cache = Arc::new(ApiCache::default());
    prewarm_initial_dashboard_cache(db.clone(), cache.clone()).await;
    seed_runtime_snapshot(&db, &runtime).await;
    let state = AppState {
        db: db.clone(),
        runtime: runtime.clone(),
        cache: cache.clone(),
    };

    let (api_router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(stats_handler))
        .routes(routes!(chains_handler))
        .routes(routes!(runtime_handler))
        .routes(routes!(deploys_handler))
        .routes(routes!(verified_handler))
        .routes(routes!(bytecode_sizes_handler))
        .routes(routes!(compilers_handler))
        .routes(routes!(languages_handler))
        .routes(routes!(standards_handler))
        .routes(routes!(recent_handler))
        .routes(routes!(query_handler))
        .split_for_parts();
    let openapi_json = api
        .to_pretty_json()
        .context("failed to generate openapi json")?;
    let app = api_router
        .route("/openapi.json", get(|| async { openapi_json }))
        .merge(Scalar::with_url("/scalar", api))
        .layer(dashboard_cors_layer())
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
    if !args.read_only {
        spawn_tail_loops(
            db.clone(),
            runtime.clone(),
            rpcs,
            Duration::from_secs(args.tail_interval_secs.max(15)),
            args.tail_confirmations,
            args.tail_batch_size,
            args.tail_max_concurrent_requests,
            args.data_dir.clone(),
        );
    }
    axum::serve(listener, app)
        .await
        .context("axum server failed")
}

fn dashboard_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(HeaderValue::from_static("https://blink.mirageprivacy.com"))
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
}

#[derive(Serialize, ToSchema)]
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

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ChainQuery {
    /// Chain ID to query. Defaults to Ethereum mainnet (1).
    chain_id: Option<u64>,
}

fn selected_chain_id(chain_id: Option<u64>) -> u64 {
    chain_id.unwrap_or_else(chains::default_chain_id)
}

#[derive(Serialize, ToSchema)]
struct ChainsResponse {
    chains: Vec<ChainInfo>,
    default_chain_id: u64,
}

#[utoipa::path(
    get,
    path = "/api/chains",
    tag = API_TAG,
    responses((status = OK, body = ChainsResponse))
)]
async fn chains_handler() -> Json<ChainsResponse> {
    Json(ChainsResponse {
        chains: chains::supported_chains().to_vec(),
        default_chain_id: chains::default_chain_id(),
    })
}

#[utoipa::path(
    get,
    path = "/api/stats",
    tag = API_TAG,
    params(ChainQuery),
    responses(
        (status = OK, body = crate::db::Stats),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn stats_handler(
    State(state): State<AppState>,
    Query(q): Query<ChainQuery>,
) -> Result<Json<crate::db::Stats>, AppError> {
    let chain_id = selected_chain_id(q.chain_id);
    let stats = state
        .cache
        .stats
        .get_or_try_update(chain_id, API_CACHE_TTL, state.db.stats(chain_id))
        .await?;
    Ok(Json(stats))
}

#[utoipa::path(
    get,
    path = "/api/runtime",
    tag = API_TAG,
    responses((status = OK, body = RuntimeResponse))
)]
async fn runtime_handler(State(state): State<AppState>) -> Json<RuntimeResponse> {
    Json(state.runtime.response().await)
}

#[derive(Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct BucketQuery {
    /// Chain ID to query. Defaults to Ethereum mainnet (1).
    chain_id: Option<u64>,
    /// Optional internal aggregation bucket: `hour`, `day`, `week`, `month`, `year`, or raw block count.
    #[serde(default)]
    bucket: Option<String>,
    /// Visible time range: `hour`, `day`, `week`, `month`, or `year`.
    #[serde(default)]
    range: Option<String>,
    /// Optional block number where the visible range should end. Defaults to the latest indexed block.
    #[serde(default)]
    end_block: Option<u64>,
    /// Optional block number where the visible range should start.
    #[serde(default)]
    start_block: Option<u64>,
    /// Optional ISO-8601 timestamp where the visible range should start.
    #[serde(default)]
    start_time: Option<DateTime<Utc>>,
    /// Optional ISO-8601 timestamp where the visible range should end.
    #[serde(default)]
    end_time: Option<DateTime<Utc>>,
    /// Maximum number of compiler versions to return.
    #[serde(default)]
    limit: Option<u32>,
}

fn range_cache_key(chain_id: u64, block_range: Option<(u64, u64)>) -> RangeCacheKey {
    RangeCacheKey {
        chain_id,
        start_block: block_range.map(|(start, _)| start),
        end_block: block_range.map(|(_, end)| end),
    }
}

#[derive(Clone, Copy)]
struct TimeSeriesWindow {
    bucket_blocks: u64,
    block_range: Option<(u64, u64)>,
}

fn parse_bucket_value(bucket: Option<&str>, chain_id: u64, anchor_block: u64) -> u64 {
    match bucket {
        None | Some("day") => blocks_per_day(chain_id, anchor_block),
        Some("hour") => blocks_per_day(chain_id, anchor_block) / 24,
        Some("week") => blocks_per_day(chain_id, anchor_block) * 7,
        Some("month") => blocks_per_day(chain_id, anchor_block) * 30,
        Some("year") => blocks_per_day(chain_id, anchor_block) * 365,
        Some(other) => other
            .parse::<u64>()
            .unwrap_or_else(|_| blocks_per_day(chain_id, anchor_block)),
    }
}

fn parse_time_series_window(q: &BucketQuery, chain_id: u64, anchor_block: u64) -> TimeSeriesWindow {
    let blocks_per_day = blocks_per_day(chain_id, anchor_block).max(1);
    let explicit_end = q.end_block.or_else(|| {
        q.end_time
            .map(|time| crate::blocks::block_number_at_time(chain_id, time))
    });
    let range_end = explicit_end.unwrap_or(anchor_block).min(anchor_block);
    let range_start = q
        .start_block
        .or_else(|| {
            q.start_time
                .map(|time| crate::blocks::block_number_at_time(chain_id, time))
        })
        .map(|start| start.min(range_end));
    if let Some(range_start) = range_start {
        let width = range_end.saturating_sub(range_start).saturating_add(1);
        let default_bucket = (width / 96).max(1);
        return TimeSeriesWindow {
            bucket_blocks: q
                .bucket
                .as_deref()
                .map(|bucket| parse_bucket_value(Some(bucket), chain_id, anchor_block))
                .unwrap_or(default_bucket),
            block_range: Some((range_start, range_end)),
        };
    }

    let range = q.range.as_deref();
    let (window_blocks, default_bucket) = match range {
        Some("hour") => {
            let window = (blocks_per_day / 24).max(1);
            (window, (window / 12).max(1))
        }
        Some("day") => (blocks_per_day, (blocks_per_day / 24).max(1)),
        Some("week") => (blocks_per_day * 7, blocks_per_day),
        Some("month") => (blocks_per_day * 30, blocks_per_day),
        Some("year") => (blocks_per_day * 365, blocks_per_day * 30),
        _ => {
            return TimeSeriesWindow {
                bucket_blocks: parse_bucket_value(q.bucket.as_deref(), chain_id, anchor_block),
                block_range: None,
            };
        }
    };

    TimeSeriesWindow {
        bucket_blocks: q
            .bucket
            .as_deref()
            .map(|bucket| parse_bucket_value(Some(bucket), chain_id, anchor_block))
            .unwrap_or(default_bucket)
            .max(1),
        block_range: Some((
            range_end.saturating_sub(window_blocks.saturating_sub(1)),
            range_end,
        )),
    }
}

fn default_aggregate_window(q: &mut BucketQuery) {
    if q.range.is_none()
        && q.start_block.is_none()
        && q.end_block.is_none()
        && q.start_time.is_none()
        && q.end_time.is_none()
    {
        q.range = Some(INITIAL_AGGREGATE_RANGE.to_string());
    }
}

#[utoipa::path(
    get,
    path = "/api/deploys-over-time",
    tag = API_TAG,
    params(BucketQuery),
    responses(
        (status = OK, body = DeploysResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn deploys_handler(
    State(state): State<AppState>,
    Query(q): Query<BucketQuery>,
) -> Result<Json<DeploysResponse>, AppError> {
    let chain_id = selected_chain_id(q.chain_id);
    let highest = state
        .db
        .highest_contract_block(chain_id)
        .await?
        .unwrap_or(0);
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = BucketCacheKey {
        chain_id,
        bucket: window.bucket_blocks,
        start_block: window.block_range.map(|(start, _)| start),
        end_block: window.block_range.map(|(_, end)| end),
    };
    let buckets = state
        .cache
        .deploys
        .get_or_try_update(
            cache_key,
            API_CACHE_TTL,
            state
                .db
                .deploys_over_time(chain_id, window.bucket_blocks, window.block_range),
        )
        .await?;
    Ok(Json(DeploysResponse {
        bucket_blocks: window.bucket_blocks,
        range_start_block: window.block_range.map(|(start, _)| start),
        range_end_block: window.block_range.map(|(_, end)| end),
        latest_block: highest,
        buckets,
    }))
}

#[utoipa::path(
    get,
    path = "/api/verified-ratio",
    tag = API_TAG,
    params(BucketQuery),
    responses(
        (status = OK, body = VerifiedResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn verified_handler(
    State(state): State<AppState>,
    Query(q): Query<BucketQuery>,
) -> Result<Json<VerifiedResponse>, AppError> {
    let chain_id = selected_chain_id(q.chain_id);
    let highest = state
        .db
        .highest_contract_block(chain_id)
        .await?
        .unwrap_or(0);
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = BucketCacheKey {
        chain_id,
        bucket: window.bucket_blocks,
        start_block: window.block_range.map(|(start, _)| start),
        end_block: window.block_range.map(|(_, end)| end),
    };
    let buckets = state
        .cache
        .verified
        .get_or_try_update(
            cache_key,
            API_CACHE_TTL,
            state
                .db
                .verified_ratio_over_time(chain_id, window.bucket_blocks, window.block_range),
        )
        .await?;
    Ok(Json(VerifiedResponse {
        bucket_blocks: window.bucket_blocks,
        range_start_block: window.block_range.map(|(start, _)| start),
        range_end_block: window.block_range.map(|(_, end)| end),
        latest_block: highest,
        buckets,
    }))
}

#[utoipa::path(
    get,
    path = "/api/bytecode-sizes",
    tag = API_TAG,
    params(BucketQuery),
    responses(
        (status = OK, body = SizeResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn bytecode_sizes_handler(
    State(state): State<AppState>,
    Query(mut q): Query<BucketQuery>,
) -> Result<Json<SizeResponse>, AppError> {
    let chain_id = selected_chain_id(q.chain_id);
    default_aggregate_window(&mut q);
    let highest = state
        .db
        .highest_contract_block(chain_id)
        .await?
        .unwrap_or(0);
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = range_cache_key(chain_id, window.block_range);
    let bins_out = state
        .cache
        .bytecode_sizes
        .get_or_try_update(
            cache_key,
            API_CACHE_TTL,
            state
                .db
                .bytecode_size_distribution(chain_id, window.block_range),
        )
        .await?;
    Ok(Json(SizeResponse { bins: bins_out }))
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct PageQuery {
    /// Chain ID to query. Defaults to Ethereum mainnet (1).
    chain_id: Option<u64>,
    /// Maximum number of contracts to return.
    limit: Option<u32>,
    /// Cursor block number from the previous page.
    before_block: Option<u64>,
    /// Cursor create index from the previous page.
    before_create_index: Option<u64>,
}

#[utoipa::path(
    get,
    path = "/api/compilers",
    tag = API_TAG,
    params(BucketQuery),
    responses(
        (status = OK, body = CompilersResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn compilers_handler(
    State(state): State<AppState>,
    Query(mut q): Query<BucketQuery>,
) -> Result<Json<CompilersResponse>, AppError> {
    let chain_id = selected_chain_id(q.chain_id);
    default_aggregate_window(&mut q);
    let limit = q.limit.unwrap_or(DEFAULT_COMPILER_LIMIT);
    let highest = state
        .db
        .highest_contract_block(chain_id)
        .await?
        .unwrap_or(0);
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = LimitRangeCacheKey {
        chain_id,
        limit,
        start_block: window.block_range.map(|(start, _)| start),
        end_block: window.block_range.map(|(_, end)| end),
    };
    let (compilers, total_known) = state
        .cache
        .compilers
        .get_or_try_update(cache_key, API_CACHE_TTL, async {
            Ok((
                state
                    .db
                    .top_compilers(chain_id, limit, window.block_range)
                    .await?,
                state
                    .db
                    .compiler_version_total(chain_id, window.block_range)
                    .await?,
            ))
        })
        .await?;
    Ok(Json(CompilersResponse {
        compilers,
        total_known,
    }))
}

#[utoipa::path(
    get,
    path = "/api/recent",
    tag = API_TAG,
    params(PageQuery),
    responses(
        (status = OK, body = RecentResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn recent_handler(
    State(state): State<AppState>,
    Query(q): Query<PageQuery>,
) -> Result<Json<RecentResponse>, AppError> {
    let chain_id = selected_chain_id(q.chain_id);
    let limit = q.limit.unwrap_or(20);
    let cache_key = RecentCacheKey {
        chain_id,
        limit,
        before_block: q.before_block,
        before_create_index: q.before_create_index,
    };
    let cursor = match (q.before_block, q.before_create_index) {
        (Some(block_number), Some(create_index)) => Some(RecentCursor {
            block_number,
            create_index,
        }),
        _ => None,
    };
    let page = match state
        .cache
        .recent
        .get_or_try_update(
            cache_key,
            API_CACHE_TTL,
            state.db.recent_contracts(chain_id, limit, cursor),
        )
        .await
    {
        Ok(page) => page,
        Err(err) => {
            tracing::warn!("recent contracts query failed: {:#}", err);
            state
                .cache
                .recent
                .get(cache_key)
                .await
                .unwrap_or(crate::db::RecentPage {
                    contracts: Vec::new(),
                    has_more: false,
                })
        }
    };
    Ok(Json(RecentResponse {
        contracts: page.contracts,
        limit,
        has_more: page.has_more,
    }))
}

#[derive(Deserialize, ToSchema)]
struct SqlQueryRequest {
    /// Read-only SQL query over dashboard views.
    sql: String,
    /// Maximum rows to return.
    limit: Option<u32>,
    /// Chain id used to scope the `contract_metadata` dashboard view.
    chain_id: Option<u64>,
}

#[utoipa::path(
    post,
    path = "/api/query",
    tag = API_TAG,
    request_body = SqlQueryRequest,
    responses(
        (status = OK, body = crate::db::SqlQueryResult),
        (status = BAD_REQUEST, body = ApiError),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<SqlQueryRequest>,
) -> Result<Json<crate::db::SqlQueryResult>, AppError> {
    state
        .db
        .query_sql(
            req.sql,
            req.limit.unwrap_or(100),
            Some(selected_chain_id(req.chain_id)),
        )
        .await
        .map(Json)
        .map_err(AppError::bad_request)
}

#[utoipa::path(
    get,
    path = "/api/languages",
    tag = API_TAG,
    params(ChainQuery),
    responses(
        (status = OK, body = LanguagesResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn languages_handler(
    State(state): State<AppState>,
    Query(q): Query<ChainQuery>,
) -> Result<Json<LanguagesResponse>, AppError> {
    let chain_id = selected_chain_id(q.chain_id);
    let languages = state
        .cache
        .languages
        .get_or_try_update(
            chain_id,
            API_CACHE_TTL,
            state.db.language_distribution(chain_id),
        )
        .await?;
    Ok(Json(LanguagesResponse { languages }))
}

#[utoipa::path(
    get,
    path = "/api/standards",
    tag = API_TAG,
    params(BucketQuery),
    responses(
        (status = OK, body = crate::db::StandardsBreakdown),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn standards_handler(
    State(state): State<AppState>,
    Query(mut q): Query<BucketQuery>,
) -> Result<Json<crate::db::StandardsBreakdown>, AppError> {
    let chain_id = selected_chain_id(q.chain_id);
    default_aggregate_window(&mut q);
    let highest = state
        .db
        .highest_contract_block(chain_id)
        .await?
        .unwrap_or(0);
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = range_cache_key(chain_id, window.block_range);
    let standards = state
        .cache
        .standards
        .get_or_try_update(
            cache_key,
            API_CACHE_TTL,
            state.db.standards_breakdown(chain_id, window.block_range),
        )
        .await?;
    Ok(Json(standards))
}

#[derive(Serialize, ToSchema)]
struct DeploysResponse {
    bucket_blocks: u64,
    range_start_block: Option<u64>,
    range_end_block: Option<u64>,
    latest_block: u64,
    buckets: Vec<crate::db::DeployBucket>,
}

#[derive(Serialize, ToSchema)]
struct VerifiedResponse {
    bucket_blocks: u64,
    range_start_block: Option<u64>,
    range_end_block: Option<u64>,
    latest_block: u64,
    buckets: Vec<crate::db::VerifiedRatioBucket>,
}

#[derive(Serialize, ToSchema)]
struct SizeResponse {
    bins: Vec<crate::db::SizeBin>,
}

#[derive(Serialize, ToSchema)]
struct CompilersResponse {
    compilers: Vec<crate::db::CompilerCount>,
    total_known: u64,
}

#[derive(Serialize, ToSchema)]
struct RecentResponse {
    contracts: Vec<crate::db::RecentContract>,
    limit: u32,
    has_more: bool,
}

#[derive(Serialize, ToSchema)]
struct LanguagesResponse {
    languages: Vec<crate::db::LanguageCount>,
}

async fn prewarm_initial_dashboard_cache(db: Db, cache: Arc<ApiCache>) {
    let started = Instant::now();
    tracing::info!("warming initial dashboard cache");
    for chain in chains::supported_chains() {
        prewarm_chain_dashboard_cache(
            &db,
            &cache,
            chain.chain_id,
            &[INITIAL_DEPLOYS_RANGE, INITIAL_VERIFIED_RANGE],
            false,
        )
        .await;
    }
    tracing::info!(
        "initial dashboard cache warmed in {:.1}s",
        started.elapsed().as_secs_f64()
    );
}

fn spawn_tail_loops(
    db: Db,
    runtime: Arc<RuntimeState>,
    rpcs: Vec<String>,
    interval: Duration,
    confirmations: u64,
    batch_size: usize,
    max_concurrent: usize,
    data_dir: std::path::PathBuf,
) {
    for rpc in rpcs {
        let db_bg = db.clone();
        let runtime_bg = runtime.clone();
        let data_dir = data_dir.clone();
        let config = TailLoopConfig {
            rpc,
            interval,
            confirmations,
            batch_size,
            max_concurrent,
            data_dir,
        };
        tokio::spawn(async move {
            tokio::time::sleep(TAIL_START_DELAY).await;
            background_tail_loop(db_bg, config, runtime_bg).await;
        });
    }
}

async fn prewarm_chain_dashboard_cache(
    db: &Db,
    cache: &ApiCache,
    chain_id: u64,
    chart_ranges: &[&str],
    include_widgets: bool,
) {
    let started = Instant::now();
    let highest = match db.highest_contract_block(chain_id).await {
        Ok(Some(block)) => block,
        Ok(None) => 0,
        Err(err) => {
            log_prewarm_error(chain_id, "highest block", err);
            return;
        }
    };

    for range in chart_ranges {
        prewarm_chart_range(db, cache, chain_id, highest, range).await;
    }

    if include_widgets {
        let aggregate_query = BucketQuery {
            chain_id: Some(chain_id),
            range: Some(INITIAL_AGGREGATE_RANGE.to_string()),
            ..BucketQuery::default()
        };
        let aggregate_window = parse_time_series_window(&aggregate_query, chain_id, highest);
        let aggregate_key = range_cache_key(chain_id, aggregate_window.block_range);

        match db.stats(chain_id).await {
            Ok(stats) => cache.stats.insert(chain_id, stats).await,
            Err(err) => log_prewarm_error(chain_id, "stats", err),
        }

        match db
            .bytecode_size_distribution(chain_id, aggregate_window.block_range)
            .await
        {
            Ok(bins) => cache.bytecode_sizes.insert(aggregate_key, bins).await,
            Err(err) => log_prewarm_error(chain_id, "bytecode sizes", err),
        }

        let compiler_key = LimitRangeCacheKey {
            chain_id,
            limit: DEFAULT_COMPILER_LIMIT,
            start_block: aggregate_window.block_range.map(|(start, _)| start),
            end_block: aggregate_window.block_range.map(|(_, end)| end),
        };
        match async {
            Ok::<_, anyhow::Error>((
                db.top_compilers(
                    chain_id,
                    DEFAULT_COMPILER_LIMIT,
                    aggregate_window.block_range,
                )
                .await?,
                db.compiler_version_total(chain_id, aggregate_window.block_range)
                    .await?,
            ))
        }
        .await
        {
            Ok(compilers) => cache.compilers.insert(compiler_key, compilers).await,
            Err(err) => log_prewarm_error(chain_id, "compilers", err),
        }

        match db.language_distribution(chain_id).await {
            Ok(languages) => cache.languages.insert(chain_id, languages).await,
            Err(err) => log_prewarm_error(chain_id, "languages", err),
        }

        match db
            .standards_breakdown(chain_id, aggregate_window.block_range)
            .await
        {
            Ok(standards) => cache.standards.insert(aggregate_key, standards).await,
            Err(err) => log_prewarm_error(chain_id, "standards", err),
        }

        let recent_key = RecentCacheKey {
            chain_id,
            limit: DEFAULT_RECENT_LIMIT,
            before_block: None,
            before_create_index: None,
        };
        match db
            .recent_contracts(chain_id, DEFAULT_RECENT_LIMIT, None)
            .await
        {
            Ok(recent) => cache.recent.insert(recent_key, recent).await,
            Err(err) => log_prewarm_error(chain_id, "recent deployments", err),
        }
    }

    tracing::debug!(
        "dashboard cache warmed for chain_id={} in {:.1}s",
        chain_id,
        started.elapsed().as_secs_f64()
    );
}

async fn prewarm_chart_range(db: &Db, cache: &ApiCache, chain_id: u64, highest: u64, range: &str) {
    let query = BucketQuery {
        chain_id: Some(chain_id),
        range: Some(range.to_string()),
        ..BucketQuery::default()
    };
    let window = parse_time_series_window(&query, chain_id, highest);
    let cache_key = BucketCacheKey {
        chain_id,
        bucket: window.bucket_blocks,
        start_block: window.block_range.map(|(start, _)| start),
        end_block: window.block_range.map(|(_, end)| end),
    };

    match db
        .deploys_over_time(chain_id, window.bucket_blocks, window.block_range)
        .await
    {
        Ok(buckets) => cache.deploys.insert(cache_key, buckets).await,
        Err(err) => log_prewarm_error(chain_id, &format!("deployments {range}"), err),
    }

    match db
        .verified_ratio_over_time(chain_id, window.bucket_blocks, window.block_range)
        .await
    {
        Ok(buckets) => cache.verified.insert(cache_key, buckets).await,
        Err(err) => log_prewarm_error(chain_id, &format!("verification {range}"), err),
    }
}

fn log_prewarm_error(chain_id: u64, label: &str, err: anyhow::Error) {
    tracing::warn!(
        "dashboard cache prewarm failed (chain_id={}, {}): {:#}",
        chain_id,
        label,
        err
    );
}

async fn background_tail_loop(db: Db, config: TailLoopConfig, runtime: Arc<RuntimeState>) {
    let chain_id = match crate::extract::tail::rpc_chain_id(&config.rpc).await {
        Ok(chain_id) => Some(chain_id),
        Err(err) => {
            tracing::warn!(
                "could not determine tail chain id at startup (rpc={}): {:#}",
                config.rpc,
                err
            );
            None
        }
    };
    tracing::info!(
        "background tail loop starting (rpc={}, chain_id={}, interval={:?}, confirmations={})",
        config.rpc,
        chain_id
            .map(|chain_id| chain_id.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        config.interval,
        config.confirmations
    );
    loop {
        runtime.mark_tail_start(chain_id).await;
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
                    .mark_tail_ok(chain_id, Some(report.end_block), report.rows as u64)
                    .await;
                tracing::info!(
                    "tail extracted blocks {}-{} ({} contracts)",
                    report.start_block,
                    report.end_block,
                    report.rows
                );
            }
            Ok(None) => {
                runtime.mark_tail_ok(chain_id, None, 0).await;
                tracing::debug!("tail: no new blocks");
            }
            Err(err) => {
                let msg = format!("{:#}", err);
                runtime.mark_tail_error(chain_id, msg.clone()).await;
                tracing::warn!("tail failed: {}", msg);
            }
        }
        tokio::time::sleep(config.interval).await;
    }
}

#[cfg(test)]
mod tests {
    use crate::chains::{ETHEREUM_CHAIN_ID, GNOSIS_CHAIN_ID};

    use super::{parse_time_series_window, BucketQuery};

    fn query(range: Option<&str>, bucket: Option<&str>) -> BucketQuery {
        BucketQuery {
            chain_id: None,
            bucket: bucket.map(str::to_string),
            range: range.map(str::to_string),
            end_block: None,
            start_block: None,
            start_time: None,
            end_time: None,
            limit: None,
        }
    }

    #[test]
    fn day_range_limits_chart_to_last_day_with_hourly_buckets() {
        let anchor_block = 20_000_000;
        let window =
            parse_time_series_window(&query(Some("day"), None), ETHEREUM_CHAIN_ID, anchor_block);

        assert_eq!(window.block_range, Some((19_992_801, 20_000_000)));
        assert_eq!(window.bucket_blocks, 300);
    }

    #[test]
    fn week_range_limits_chart_to_last_week_with_daily_buckets() {
        let anchor_block = 20_000_000;
        let window =
            parse_time_series_window(&query(Some("week"), None), ETHEREUM_CHAIN_ID, anchor_block);

        assert_eq!(window.block_range, Some((19_949_601, 20_000_000)));
        assert_eq!(window.bucket_blocks, 7_200);
    }

    #[test]
    fn year_range_limits_chart_to_last_year_with_monthly_buckets() {
        let anchor_block = 20_000_000;
        let window =
            parse_time_series_window(&query(Some("year"), None), ETHEREUM_CHAIN_ID, anchor_block);

        assert_eq!(window.block_range, Some((17_372_001, 20_000_000)));
        assert_eq!(window.bucket_blocks, 216_000);
    }

    #[test]
    fn hour_range_uses_chain_specific_block_time() {
        let anchor_block = 46_000_000;
        let window =
            parse_time_series_window(&query(Some("hour"), None), GNOSIS_CHAIN_ID, anchor_block);

        assert_eq!(window.block_range, Some((45_999_281, 46_000_000)));
        assert_eq!(window.bucket_blocks, 60);
    }

    #[test]
    fn legacy_bucket_query_keeps_full_history_behavior() {
        let anchor_block = 20_000_000;
        let window =
            parse_time_series_window(&query(None, Some("day")), ETHEREUM_CHAIN_ID, anchor_block);

        assert_eq!(window.block_range, None);
        assert_eq!(window.bucket_blocks, 7_200);
    }

    #[test]
    fn range_end_block_moves_visible_window() {
        let anchor_block = 20_000_000;
        let mut q = query(Some("day"), None);
        q.end_block = Some(19_000_000);
        let window = parse_time_series_window(&q, ETHEREUM_CHAIN_ID, anchor_block);

        assert_eq!(window.block_range, Some((18_992_801, 19_000_000)));

        q.end_block = Some(21_000_000);
        let capped = parse_time_series_window(&q, ETHEREUM_CHAIN_ID, anchor_block);
        assert_eq!(capped.block_range, Some((19_992_801, 20_000_000)));
    }

    #[test]
    fn explicit_start_block_creates_custom_window() {
        let anchor_block = 20_000_000;
        let mut q = query(None, None);
        q.start_block = Some(19_900_000);
        q.end_block = Some(19_950_000);
        let window = parse_time_series_window(&q, ETHEREUM_CHAIN_ID, anchor_block);

        assert_eq!(window.block_range, Some((19_900_000, 19_950_000)));
        assert_eq!(window.bucket_blocks, 520);
    }
}

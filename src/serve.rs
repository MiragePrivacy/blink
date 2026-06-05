//! Public dashboard HTTP server.
//!
//! Hosts the JSON API consumed by the separate dashboard frontend.
//! Optional background tasks:
//! - `--tail-rpc URL` polls the chain head and extracts newly produced
//!   blocks into a separate `tail__*.parquet` file (see [`crate::extract::tail`]).
//!
//! Endpoints (all return JSON):
//! - `GET /api/stats` — totals, verified pct, last block, verification coverage.
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
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_scalar::{Scalar, Servable};

use crate::{
    blocks::blocks_per_day,
    cli::ServeArgs,
    db::{Db, RecentCursor},
};

#[derive(Clone)]
struct AppState {
    db: Db,
    runtime: Arc<RuntimeState>,
    cache: Arc<ApiCache>,
}

const API_CACHE_TTL: Duration = Duration::from_secs(30);
const API_TAG: &str = "Dashboard";

#[derive(OpenApi)]
#[openapi(tags(
    (name = API_TAG, description = "Blink dashboard and contract intelligence endpoints")
))]
struct ApiDoc;

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
    tail_enabled: bool,
    tail_interval_secs: u64,
    snapshot: Mutex<RuntimeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, ToSchema)]
struct RuntimeSnapshot {
    tail_running: bool,
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
    if args.read_only && args.tail_rpc.is_some() {
        tracing::warn!("--read-only is set; ignoring --tail-rpc (it requires a write lock)");
    }
    let db = Db::open_with_mode(&args.data_dir, &args.contracts_glob, args.read_only)?;
    let tail_enabled = args.tail_rpc.is_some() && !args.read_only;
    let runtime = Arc::new(RuntimeState::new(
        args.read_only,
        tail_enabled,
        args.tail_interval_secs.max(15),
    ));
    let state = AppState {
        db: db.clone(),
        runtime: runtime.clone(),
        cache: Arc::new(ApiCache::default()),
    };

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

    let (api_router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(stats_handler))
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
    axum::serve(listener, app)
        .await
        .context("axum server failed")
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

#[utoipa::path(
    get,
    path = "/api/stats",
    tag = API_TAG,
    responses(
        (status = OK, body = crate::db::Stats),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
async fn stats_handler(State(state): State<AppState>) -> Result<Json<crate::db::Stats>, AppError> {
    let stats = state
        .cache
        .stats
        .get_or_try_update(API_CACHE_TTL, state.db.stats())
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

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
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

#[utoipa::path(
    get,
    path = "/api/bytecode-sizes",
    tag = API_TAG,
    responses(
        (status = OK, body = SizeResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
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

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct LimitQuery {
    /// Maximum number of compiler versions to return.
    limit: Option<u32>,
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct PageQuery {
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
    params(LimitQuery),
    responses(
        (status = OK, body = CompilersResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
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

#[derive(Deserialize, ToSchema)]
struct SqlQueryRequest {
    /// Read-only SQL query over dashboard views.
    sql: String,
    /// Maximum rows to return.
    limit: Option<u32>,
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
        .query_sql(req.sql, req.limit.unwrap_or(100))
        .await
        .map(Json)
        .map_err(AppError::bad_request)
}

#[utoipa::path(
    get,
    path = "/api/languages",
    tag = API_TAG,
    responses(
        (status = OK, body = LanguagesResponse),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
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

#[utoipa::path(
    get,
    path = "/api/standards",
    tag = API_TAG,
    responses(
        (status = OK, body = crate::db::StandardsBreakdown),
        (status = INTERNAL_SERVER_ERROR, body = ApiError)
    )
)]
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

#[derive(Serialize, ToSchema)]
struct DeploysResponse {
    bucket_blocks: u64,
    buckets: Vec<crate::db::DeployBucket>,
}

#[derive(Serialize, ToSchema)]
struct VerifiedResponse {
    bucket_blocks: u64,
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

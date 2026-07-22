//! Public dashboard HTTP server.
//!
//! Hosts the JSON API consumed by the separate dashboard frontend.
//! Optional background tasks:
//! - repeated `--rpc URL` flags poll one or more chain heads and extract
//!   newly produced blocks into separate `tail__chain_*` parquet files.
//! - `--verifier-alliance-dir` periodically downloads and incrementally
//!   imports Verifier Alliance labels without restarting the server.
//!
//! Serving model: the default dashboard is warmed before the listener binds,
//! then every cacheable endpoint is stale-while-revalidate. A cached entry is
//! returned immediately no matter its age; entries past their TTL trigger a
//! background refresh (deduplicated per key).
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
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    extract::{Query, Request, State},
    http::{HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{watch, Mutex},
};
use tower_http::{
    compression::CompressionLayer,
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
    db::{Db, DbOptions, RecentCursor},
};

#[derive(Clone)]
struct AppState {
    db: Db,
    runtime: Arc<RuntimeState>,
    cache: Arc<ApiCache>,
    latency: Arc<LatencyStats>,
}

/// Upper bounds (ms) of the API latency histogram buckets; the last bucket is
/// open-ended.
const LATENCY_BUCKET_UPPER_MS: [u64; 11] = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000];
const SLOW_REQUEST_LOG_THRESHOLD: Duration = Duration::from_millis(1000);

/// Lock-free rolling latency histogram over every `/api/*` response since
/// startup. Answers "are we actually fast in production?" via `/api/runtime`.
#[derive(Default)]
struct LatencyStats {
    buckets: [AtomicU64; LATENCY_BUCKET_UPPER_MS.len() + 1],
    requests: AtomicU64,
    total_micros: AtomicU64,
}

#[derive(Debug, Default, Clone, Serialize, ToSchema)]
struct LatencySnapshot {
    requests: u64,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

impl LatencyStats {
    fn record(&self, elapsed: Duration) {
        let ms = elapsed.as_millis() as u64;
        let idx = LATENCY_BUCKET_UPPER_MS
            .iter()
            .position(|upper| ms <= *upper)
            .unwrap_or(LATENCY_BUCKET_UPPER_MS.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.total_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    }

    fn percentile(&self, counts: &[u64], total: u64, q: f64) -> f64 {
        if total == 0 {
            return 0.0;
        }
        let target = (total as f64 * q).ceil() as u64;
        let mut cumulative = 0u64;
        for (idx, count) in counts.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                return LATENCY_BUCKET_UPPER_MS
                    .get(idx)
                    .copied()
                    .unwrap_or(LATENCY_BUCKET_UPPER_MS[LATENCY_BUCKET_UPPER_MS.len() - 1] * 2)
                    as f64;
            }
        }
        0.0
    }

    fn snapshot(&self) -> LatencySnapshot {
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        let requests = self.requests.load(Ordering::Relaxed);
        let avg_ms = if requests == 0 {
            0.0
        } else {
            self.total_micros.load(Ordering::Relaxed) as f64 / requests as f64 / 1000.0
        };
        LatencySnapshot {
            requests,
            avg_ms,
            p50_ms: self.percentile(&counts, requests, 0.50),
            p95_ms: self.percentile(&counts, requests, 0.95),
            p99_ms: self.percentile(&counts, requests, 0.99),
        }
    }
}

/// Record latency for every API request; anything past the slow threshold is
/// logged so regressions surface in the journal, not just in percentiles.
async fn track_latency(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let path_is_api = req.uri().path().starts_with("/api/");
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let started = Instant::now();
    let response = next.run(req).await;
    if path_is_api {
        let elapsed = started.elapsed();
        state.latency.record(elapsed);
        if elapsed >= SLOW_REQUEST_LOG_THRESHOLD {
            tracing::warn!(
                "slow dashboard request: {} {} took {:.1}ms",
                method,
                path,
                elapsed.as_secs_f64() * 1000.0
            );
        }
    }
    response
}

const API_CACHE_TTL: Duration = Duration::from_secs(600);
const HIGHEST_BLOCK_TTL: Duration = Duration::from_secs(5);
const TAIL_START_DELAY: Duration = Duration::from_secs(15);
const VA_SYNC_START_DELAY: Duration = Duration::from_secs(30);
const VA_SYNC_MIN_INTERVAL: Duration = Duration::from_secs(900);
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
    highest_blocks: CacheMap<u64, u64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BucketCacheKey {
    chain_id: u64,
    bucket: u64,
    start_block: Option<u64>,
    end_block: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RangeCacheKey {
    chain_id: u64,
    bucket: Option<u64>,
    start_block: Option<u64>,
    end_block: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct LimitRangeCacheKey {
    chain_id: u64,
    limit: u32,
    bucket: Option<u64>,
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

/// Stale-while-revalidate, single-flight cache.
///
/// Reads never block on the database once a key has been populated: expired
/// entries are served as-is while one background task per key refreshes them.
/// Cold misses are single-flight too — concurrent requests for the same key
/// (e.g. a dashboard fanning out while the boot prewarm runs) wait for the
/// one in-flight computation instead of duplicating it on a small host.
struct CacheMap<K, V> {
    inner: Arc<CacheMapInner<K, V>>,
}

struct CacheMapInner<K, V> {
    values: StdMutex<HashMap<K, (Instant, V)>>,
    /// Keys currently being computed; waiters subscribe to the receiver and
    /// wake when the compute holder drops its sender.
    inflight: StdMutex<HashMap<K, watch::Receiver<()>>>,
}

impl<K, V> Clone for CacheMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> Default for CacheMap<K, V> {
    fn default() -> Self {
        Self {
            inner: Arc::new(CacheMapInner {
                values: StdMutex::new(HashMap::new()),
                inflight: StdMutex::new(HashMap::new()),
            }),
        }
    }
}

impl<K, V> CacheMap<K, V>
where
    K: Copy + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn get_or_refresh<F, Fut>(&self, key: K, ttl: Duration, make: F) -> Result<V>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V>> + Send + 'static,
    {
        let mut make = Some(make);
        loop {
            if let Some((age, value)) = self.lookup(&key) {
                if age > ttl {
                    if let Ok(slot) = self.claim(key) {
                        let this = self.clone();
                        let refresh = (make.take().expect("make consumed once"))();
                        tokio::spawn(async move {
                            match refresh.await {
                                Ok(fresh) => this.insert(key, fresh),
                                Err(err) => tracing::warn!(
                                    "background dashboard cache refresh failed: {:#}",
                                    err
                                ),
                            }
                            this.release(&key);
                            drop(slot);
                        });
                    }
                }
                return Ok(value);
            }

            match self.claim(key) {
                Ok(slot) => {
                    let result = (make.take().expect("make consumed once"))().await;
                    if let Ok(value) = &result {
                        self.insert(key, value.clone());
                    }
                    self.release(&key);
                    drop(slot);
                    return result;
                }
                Err(mut waiter) => {
                    let _ = waiter.changed().await;
                }
            }
        }
    }

    fn lookup(&self, key: &K) -> Option<(Duration, V)> {
        self.inner
            .values
            .lock()
            .expect("cache map poisoned")
            .get(key)
            .map(|(at, value)| (at.elapsed(), value.clone()))
    }

    fn insert(&self, key: K, value: V) {
        self.inner
            .values
            .lock()
            .expect("cache map poisoned")
            .insert(key, (Instant::now(), value));
    }

    fn get(&self, key: &K) -> Option<V> {
        self.lookup(key).map(|(_, value)| value)
    }

    /// Refresh a key without discarding its current value. If another task is
    /// already refreshing the key, that work wins and this call is a no-op.
    async fn refresh_if_idle<F, Fut>(&self, key: K, make: F) -> Result<bool>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V>> + Send,
    {
        let Ok(slot) = self.claim(key) else {
            return Ok(false);
        };
        let result = make().await;
        if let Ok(value) = &result {
            self.insert(key, value.clone());
        }
        self.release(&key);
        drop(slot);
        result.map(|_| true)
    }

    fn expire_where(&self, predicate: impl Fn(K) -> bool) {
        let expired_at = Instant::now()
            .checked_sub(API_CACHE_TTL + Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        for (key, (stored_at, _)) in self
            .inner
            .values
            .lock()
            .expect("cache map poisoned")
            .iter_mut()
        {
            if predicate(*key) {
                *stored_at = expired_at;
            }
        }
    }

    fn touch_where(&self, predicate: impl Fn(K) -> bool) {
        let now = Instant::now();
        for (key, (stored_at, _)) in self
            .inner
            .values
            .lock()
            .expect("cache map poisoned")
            .iter_mut()
        {
            if predicate(*key) {
                *stored_at = now;
            }
        }
    }

    /// Claim the compute slot for `key`: the holder gets the sender (waiters
    /// wake when it drops); if already claimed, the receiver to wait on.
    fn claim(&self, key: K) -> Result<watch::Sender<()>, watch::Receiver<()>> {
        let mut inflight = self.inner.inflight.lock().expect("cache map poisoned");
        if let Some(receiver) = inflight.get(&key) {
            return Err(receiver.clone());
        }
        let (sender, receiver) = watch::channel(());
        inflight.insert(key, receiver);
        Ok(sender)
    }

    fn release(&self, key: &K) {
        self.inner
            .inflight
            .lock()
            .expect("cache map poisoned")
            .remove(key);
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    #[tokio::test]
    async fn explicit_refresh_keeps_stale_value_available() {
        let cache = CacheMap::<u64, u64>::default();
        cache.insert(1, 10);
        cache.expire_where(|key| key == 1);

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let refresh_cache = cache.clone();
        let refresh = tokio::spawn(async move {
            refresh_cache
                .refresh_if_idle(1, || async move {
                    let _ = started_tx.send(());
                    let _ = finish_rx.await;
                    Ok(20)
                })
                .await
        });

        started_rx.await.expect("refresh started");
        let stale = tokio::time::timeout(
            Duration::from_millis(100),
            cache.get_or_refresh(1, API_CACHE_TTL, || async {
                Err(anyhow::anyhow!("must not start a duplicate refresh"))
            }),
        )
        .await
        .expect("stale cache read must not block")
        .expect("stale cache value");
        assert_eq!(stale, 10);

        finish_tx.send(()).expect("finish refresh");
        assert!(refresh
            .await
            .expect("refresh task")
            .expect("refresh result"));
        assert_eq!(cache.get(&1), Some(20));
    }

    #[test]
    fn recognizes_errors_that_require_a_database_restart() {
        assert!(is_fatal_database_error(
            "FATAL Error: Corrupted ART index - likely the same row id was inserted twice"
        ));
        assert!(is_fatal_database_error(
            "database has been invalidated because of a previous fatal error"
        ));
        assert!(!is_fatal_database_error("HTTP error 429 Too Many Requests"));
    }
}

#[derive(Debug)]
pub struct RuntimeState {
    read_only: bool,
    tail_enabled: bool,
    tail_interval_secs: u64,
    snapshot: Mutex<RuntimeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, ToSchema)]
pub struct RuntimeSnapshot {
    pub tail_running: bool,
    pub tail_running_count: u64,
    pub tail_last_ok_at: Option<DateTime<Utc>>,
    pub tail_last_block: Option<u64>,
    pub tail_last_rows: Option<u64>,
    pub tail_last_error_at: Option<DateTime<Utc>>,
    pub tail_last_error: Option<String>,
    pub tail_chains: Vec<ChainRuntimeSnapshot>,
}

#[derive(Debug, Default, Clone, Serialize, ToSchema)]
pub struct ChainRuntimeSnapshot {
    pub chain_id: u64,
    pub tail_running: bool,
    pub tail_running_count: u64,
    pub tail_last_ok_at: Option<DateTime<Utc>>,
    pub tail_last_block: Option<u64>,
    pub tail_last_rows: Option<u64>,
    pub tail_last_error_at: Option<DateTime<Utc>>,
    pub tail_last_error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RuntimeResponse {
    pub read_only: bool,
    pub tail_enabled: bool,
    pub tail_interval_secs: u64,
    #[serde(flatten)]
    pub snapshot: RuntimeSnapshot,
}

struct TailLoopConfig {
    rpc: String,
    interval: Duration,
    confirmations: u64,
    batch_size: usize,
    max_concurrent: usize,
    data_dir: PathBuf,
}

struct TailLoopSettings {
    rpcs: Vec<String>,
    interval: Duration,
    confirmations: u64,
    batch_size: usize,
    max_concurrent: usize,
    data_dir: PathBuf,
}

impl RuntimeState {
    pub fn new(read_only: bool, tail_enabled: bool, tail_interval_secs: u64) -> Self {
        Self {
            read_only,
            tail_enabled,
            tail_interval_secs,
            snapshot: Mutex::new(RuntimeSnapshot::default()),
        }
    }

    pub async fn response(&self) -> RuntimeResponse {
        RuntimeResponse {
            read_only: self.read_only,
            tail_enabled: self.tail_enabled,
            tail_interval_secs: self.tail_interval_secs,
            snapshot: self.snapshot.lock().await.clone(),
        }
    }

    pub async fn mark_tail_start(&self, chain_id: Option<u64>) {
        let mut snapshot = self.snapshot.lock().await;
        if updates_default_runtime(chain_id) && snapshot.tail_last_ok_at.is_none() {
            snapshot.tail_running_count = snapshot.tail_running_count.saturating_add(1);
            snapshot.tail_running = true;
        }
        if let Some(chain_id) = chain_id {
            let chain = chain_runtime_snapshot_mut(&mut snapshot, chain_id);
            if chain.tail_last_ok_at.is_none() {
                chain.tail_running_count = chain.tail_running_count.saturating_add(1);
                chain.tail_running = true;
            }
        }
    }

    pub async fn mark_tail_ok(&self, chain_id: Option<u64>, end_block: Option<u64>, rows: u64) {
        let mut snapshot = self.snapshot.lock().await;
        let now = Some(Utc::now());
        if updates_default_runtime(chain_id) {
            snapshot.tail_running_count = snapshot.tail_running_count.saturating_sub(1);
            snapshot.tail_running = snapshot.tail_running_count > 0;
            snapshot.tail_last_ok_at = now;
            if let Some(block) = end_block {
                snapshot.tail_last_block = Some(block);
            }
            snapshot.tail_last_rows = Some(rows);
            snapshot.tail_last_error = None;
            snapshot.tail_last_error_at = None;
        }
        if let Some(chain_id) = chain_id {
            let chain = chain_runtime_snapshot_mut(&mut snapshot, chain_id);
            chain.tail_running_count = chain.tail_running_count.saturating_sub(1);
            chain.tail_running = chain.tail_running_count > 0;
            chain.tail_last_ok_at = now;
            if let Some(block) = end_block {
                chain.tail_last_block = Some(block);
            }
            chain.tail_last_rows = Some(rows);
            chain.tail_last_error = None;
            chain.tail_last_error_at = None;
        }
    }

    pub async fn mark_tail_ready(&self, chain_id: u64, block: Option<u64>) {
        let mut snapshot = self.snapshot.lock().await;
        let now = Some(Utc::now());
        if updates_default_runtime(Some(chain_id)) {
            snapshot.tail_last_ok_at = now;
            if let Some(block) = block {
                snapshot.tail_last_block = Some(block);
            }
            snapshot.tail_last_rows = Some(0);
            snapshot.tail_last_error = None;
            snapshot.tail_last_error_at = None;
        }

        let chain = chain_runtime_snapshot_mut(&mut snapshot, chain_id);
        chain.tail_last_ok_at = now;
        chain.tail_last_block = block;
        chain.tail_last_rows = Some(0);
        chain.tail_last_error = None;
        chain.tail_last_error_at = None;
    }

    pub async fn mark_tail_error(&self, chain_id: Option<u64>, message: String) {
        let mut snapshot = self.snapshot.lock().await;
        let now = Some(Utc::now());
        if updates_default_runtime(chain_id) {
            snapshot.tail_running_count = snapshot.tail_running_count.saturating_sub(1);
            snapshot.tail_running = snapshot.tail_running_count > 0;
            snapshot.tail_last_error_at = now;
            snapshot.tail_last_error = Some(message.clone());
        }
        if let Some(chain_id) = chain_id {
            let chain = chain_runtime_snapshot_mut(&mut snapshot, chain_id);
            chain.tail_running_count = chain.tail_running_count.saturating_sub(1);
            chain.tail_running = chain.tail_running_count > 0;
            chain.tail_last_error_at = now;
            chain.tail_last_error = Some(message);
        }
    }
}

fn updates_default_runtime(chain_id: Option<u64>) -> bool {
    chain_id
        .map(|chain_id| chain_id == chains::default_chain_id())
        .unwrap_or(true)
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
    let db = Db::open(
        &args.data_dir,
        &args.contracts_glob,
        DbOptions {
            read_only: args.read_only,
            memory_limit: args.db_memory_limit.clone(),
            threads: args.db_threads,
            readers: args.db_readers,
        },
    )?;
    let rpcs = args
        .rpc
        .iter()
        .map(|rpc| rpc.trim())
        .filter(|rpc| !rpc.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let tail_enabled = !rpcs.is_empty() && !args.read_only;
    let verifier_alliance_dir = args.verifier_alliance_dir.clone();
    let runtime = Arc::new(RuntimeState::new(
        args.read_only,
        tail_enabled,
        args.tail_interval_secs.max(15),
    ));
    let cache = Arc::new(ApiCache::default());

    seed_runtime_snapshot(&db, &runtime).await;
    prewarm_initial_dashboard_cache(db.clone(), cache.clone()).await;

    let state = AppState {
        db: db.clone(),
        runtime: runtime.clone(),
        cache: cache.clone(),
        latency: Arc::new(LatencyStats::default()),
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
        .layer(CompressionLayer::new())
        .layer(dashboard_cors_layer())
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn_with_state(state.clone(), track_latency))
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
        // The materialized SQL-explorer table is maintenance work. Start it
        // only after the dashboard cache is ready so it cannot contend with
        // cold aggregate queries during startup.
        let db_explorer = db.clone();
        tokio::spawn(async move {
            match db_explorer.refresh_explorer().await {
                Ok(true) => tracing::info!("sql explorer table ready"),
                Ok(false) => {}
                Err(err) => tracing::warn!("sql explorer table rebuild failed: {:#}", err),
            }
        });
        spawn_tail_loops(
            db.clone(),
            runtime.clone(),
            cache.clone(),
            TailLoopSettings {
                rpcs,
                interval: Duration::from_secs(args.tail_interval_secs.max(15)),
                confirmations: args.tail_confirmations,
                batch_size: args.tail_batch_size,
                max_concurrent: args.tail_max_concurrent_requests,
                data_dir: args.data_dir.clone(),
            },
        );
        if let Some(verifier_alliance_dir) = verifier_alliance_dir {
            spawn_verifier_alliance_sync_loop(
                db.clone(),
                cache.clone(),
                verifier_alliance_dir,
                Duration::from_secs(args.verifier_alliance_sync_interval_secs),
            );
        }
    } else if verifier_alliance_dir.is_some() {
        tracing::warn!("--read-only is set; automatic Verifier Alliance sync is disabled");
    }
    axum::serve(listener, app)
        .await
        .context("axum server failed")
}

fn spawn_verifier_alliance_sync_loop(
    db: Db,
    cache: Arc<ApiCache>,
    verifier_alliance_dir: PathBuf,
    interval: Duration,
) {
    let interval = interval.max(VA_SYNC_MIN_INTERVAL);
    tracing::info!(
        "automatic Verifier Alliance sync enabled (dir={}, interval={}s)",
        verifier_alliance_dir.display(),
        interval.as_secs()
    );
    tokio::spawn(async move {
        tokio::time::sleep(VA_SYNC_START_DELAY).await;
        loop {
            let started = Instant::now();
            defer_dashboard_cache_refreshes(&cache);
            tracing::info!("syncing Verifier Alliance dataset from object storage");
            match crate::va_sync::sync_verifier_alliance_files(&verifier_alliance_dir).await {
                Ok(()) => {
                    for chain in chains::supported_chains() {
                        match db
                            .import_verifier_alliance(verifier_alliance_dir.clone(), chain.chain_id)
                            .await
                        {
                            Ok(changed) => {
                                if changed {
                                    refresh_verification_cache(&db, &cache, chain.chain_id).await;
                                }
                                tracing::info!(
                                    "Verifier Alliance data current for chain_id={}{}",
                                    chain.chain_id,
                                    if changed { " (updated)" } else { "" }
                                );
                            }
                            Err(error) => tracing::warn!(
                                "Verifier Alliance import failed for chain_id={}: {:#}",
                                chain.chain_id,
                                error
                            ),
                        }
                    }
                    tracing::info!(
                        "Verifier Alliance sync completed in {:.1}s",
                        started.elapsed().as_secs_f64()
                    );
                }
                Err(error) => tracing::warn!("Verifier Alliance download failed: {:#}", error),
            }
            tokio::time::sleep(interval).await;
        }
    });
}

fn defer_dashboard_cache_refreshes(cache: &ApiCache) {
    cache.stats.touch_where(|_| true);
    cache.deploys.touch_where(|_| true);
    cache.verified.touch_where(|_| true);
    cache.bytecode_sizes.touch_where(|_| true);
    cache.compilers.touch_where(|_| true);
    cache.recent.touch_where(|_| true);
    cache.languages.touch_where(|_| true);
    cache.standards.touch_where(|_| true);
    cache.highest_blocks.touch_where(|_| true);
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

async fn cached_highest_block(state: &AppState, chain_id: u64) -> Result<u64> {
    let db = state.db.clone();
    state
        .cache
        .highest_blocks
        .get_or_refresh(chain_id, HIGHEST_BLOCK_TTL, move || async move {
            Ok(db.highest_contract_block(chain_id).await?.unwrap_or(0))
        })
        .await
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
    let db = state.db.clone();
    let stats = state
        .cache
        .stats
        .get_or_refresh(chain_id, API_CACHE_TTL, move || async move {
            db.stats(chain_id).await
        })
        .await?;
    Ok(Json(stats))
}

#[derive(Serialize, ToSchema)]
struct RuntimeApiResponse {
    #[serde(flatten)]
    runtime: RuntimeResponse,
    /// Rolling latency of every `/api/*` request since startup.
    api_latency: LatencySnapshot,
}

#[utoipa::path(
    get,
    path = "/api/runtime",
    tag = API_TAG,
    responses((status = OK, body = RuntimeApiResponse))
)]
async fn runtime_handler(State(state): State<AppState>) -> Json<RuntimeApiResponse> {
    Json(RuntimeApiResponse {
        runtime: state.runtime.response().await,
        api_latency: state.latency.snapshot(),
    })
}

#[derive(Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct BucketQuery {
    /// Chain ID to query. Defaults to Ethereum mainnet (1).
    pub chain_id: Option<u64>,
    /// Optional internal aggregation bucket: `hour`, `day`, `week`, `month`, `year`, or raw block count.
    #[serde(default)]
    pub bucket: Option<String>,
    /// Visible time range: `hour`, `day`, `week`, `month`, or `year`.
    #[serde(default)]
    pub range: Option<String>,
    /// Optional block number where the visible range should end. Defaults to the latest indexed block.
    #[serde(default)]
    pub end_block: Option<u64>,
    /// Optional block number where the visible range should start.
    #[serde(default)]
    pub start_block: Option<u64>,
    /// Optional ISO-8601 timestamp where the visible range should start.
    #[serde(default)]
    pub start_time: Option<DateTime<Utc>>,
    /// Optional ISO-8601 timestamp where the visible range should end.
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
    /// Maximum number of compiler versions to return.
    #[serde(default)]
    pub limit: Option<u32>,
}

fn relative_range_code(range: &str) -> u64 {
    match range {
        "hour" => 1,
        "day" => 2,
        "week" => 3,
        "month" => 4,
        "year" => 5,
        _ => 0,
    }
}

fn uses_relative_preset_window(q: &BucketQuery) -> bool {
    q.range.is_some()
        && q.start_block.is_none()
        && q.end_block.is_none()
        && q.start_time.is_none()
        && q.end_time.is_none()
}

fn normalized_cache_range(
    q: &BucketQuery,
    window: TimeSeriesWindow,
    bucket_blocks: u64,
) -> (Option<u64>, Option<u64>, Option<u64>) {
    if uses_relative_preset_window(q) {
        let range_code = q.range.as_deref().map(relative_range_code);
        // A relative range is one logical cache entry as the chain advances.
        // Keeping the key stable lets stale-while-revalidate serve the last
        // result immediately instead of creating a cold miss at each bucket
        // boundary.
        return (Some(bucket_blocks.max(1)), range_code, None);
    }
    (
        None,
        window.block_range.map(|(start, _)| start),
        window.block_range.map(|(_, end)| end),
    )
}

pub fn bucket_cache_key(
    chain_id: u64,
    q: &BucketQuery,
    window: TimeSeriesWindow,
) -> BucketCacheKey {
    let (_, start_block, end_block) = normalized_cache_range(q, window, window.bucket_blocks);
    BucketCacheKey {
        chain_id,
        bucket: window.bucket_blocks,
        start_block,
        end_block,
    }
}

pub fn range_cache_key_for_query(
    chain_id: u64,
    q: &BucketQuery,
    window: TimeSeriesWindow,
) -> RangeCacheKey {
    let (bucket, start_block, end_block) = normalized_cache_range(q, window, window.bucket_blocks);
    RangeCacheKey {
        chain_id,
        bucket,
        start_block,
        end_block,
    }
}

#[derive(Clone, Copy)]
pub struct TimeSeriesWindow {
    pub bucket_blocks: u64,
    pub block_range: Option<(u64, u64)>,
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

pub fn parse_time_series_window(
    q: &BucketQuery,
    chain_id: u64,
    anchor_block: u64,
) -> TimeSeriesWindow {
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
    let highest = cached_highest_block(&state, chain_id).await?;
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = bucket_cache_key(chain_id, &q, window);
    let db = state.db.clone();
    let buckets = state
        .cache
        .deploys
        .get_or_refresh(cache_key, API_CACHE_TTL, move || async move {
            db.deploys_over_time(chain_id, window.bucket_blocks, window.block_range)
                .await
        })
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
    let highest = cached_highest_block(&state, chain_id).await?;
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = bucket_cache_key(chain_id, &q, window);
    let db = state.db.clone();
    let buckets = state
        .cache
        .verified
        .get_or_refresh(cache_key, API_CACHE_TTL, move || async move {
            db.verified_ratio_over_time(chain_id, window.bucket_blocks, window.block_range)
                .await
        })
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
    let highest = cached_highest_block(&state, chain_id).await?;
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = range_cache_key_for_query(chain_id, &q, window);
    let db = state.db.clone();
    let bins_out = state
        .cache
        .bytecode_sizes
        .get_or_refresh(cache_key, API_CACHE_TTL, move || async move {
            db.bytecode_size_distribution(chain_id, window.block_range)
                .await
        })
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
    let highest = cached_highest_block(&state, chain_id).await?;
    let window = parse_time_series_window(&q, chain_id, highest);
    let (bucket, start_block, end_block) = normalized_cache_range(&q, window, window.bucket_blocks);
    let cache_key = LimitRangeCacheKey {
        chain_id,
        limit,
        bucket,
        start_block,
        end_block,
    };
    let db = state.db.clone();
    let (compilers, total_known) = state
        .cache
        .compilers
        .get_or_refresh(cache_key, API_CACHE_TTL, move || async move {
            Ok((
                db.top_compilers(chain_id, limit, window.block_range)
                    .await?,
                db.compiler_version_total(chain_id, window.block_range)
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
    let db = state.db.clone();
    let page = match state
        .cache
        .recent
        .get_or_refresh(cache_key, API_CACHE_TTL, move || async move {
            db.recent_contracts(chain_id, limit, cursor).await
        })
        .await
    {
        Ok(page) => page,
        Err(err) => {
            tracing::warn!("recent contracts query failed: {:#}", err);
            state
                .cache
                .recent
                .get(&cache_key)
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
    let db = state.db.clone();
    let languages = state
        .cache
        .languages
        .get_or_refresh(chain_id, API_CACHE_TTL, move || async move {
            db.language_distribution(chain_id).await
        })
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
    let highest = cached_highest_block(&state, chain_id).await?;
    let window = parse_time_series_window(&q, chain_id, highest);
    let cache_key = range_cache_key_for_query(chain_id, &q, window);
    let db = state.db.clone();
    let standards = state
        .cache
        .standards
        .get_or_refresh(cache_key, API_CACHE_TTL, move || async move {
            db.standards_breakdown(chain_id, window.block_range).await
        })
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
    tracing::info!("warming dashboard cache in background");
    for chain in chains::supported_chains() {
        prewarm_chain_dashboard_cache(
            &db,
            &cache,
            chain.chain_id,
            &[INITIAL_DEPLOYS_RANGE, INITIAL_VERIFIED_RANGE],
            true,
        )
        .await;
    }
    tracing::info!(
        "dashboard cache warmed in {:.1}s",
        started.elapsed().as_secs_f64()
    );
}

fn spawn_tail_loops(
    db: Db,
    runtime: Arc<RuntimeState>,
    cache: Arc<ApiCache>,
    settings: TailLoopSettings,
) {
    for rpc in settings.rpcs {
        let db_bg = db.clone();
        let runtime_bg = runtime.clone();
        let cache_bg = cache.clone();
        let data_dir = settings.data_dir.clone();
        let config = TailLoopConfig {
            rpc,
            interval: settings.interval,
            confirmations: settings.confirmations,
            batch_size: settings.batch_size,
            max_concurrent: settings.max_concurrent,
            data_dir,
        };
        tokio::spawn(async move {
            tokio::time::sleep(TAIL_START_DELAY).await;
            background_tail_loop(db_bg, config, runtime_bg, cache_bg).await;
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
    let db_for_highest = db.clone();
    let highest = match cache
        .highest_blocks
        .get_or_refresh(chain_id, HIGHEST_BLOCK_TTL, move || async move {
            Ok(db_for_highest
                .highest_contract_block(chain_id)
                .await?
                .unwrap_or(0))
        })
        .await
    {
        Ok(block) => block,
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
        let aggregate_key = range_cache_key_for_query(chain_id, &aggregate_query, aggregate_window);

        match cache
            .stats
            .refresh_if_idle(chain_id, || db.stats(chain_id))
            .await
        {
            Ok(_) => {}
            Err(err) => log_prewarm_error(chain_id, "stats", err),
        }

        match cache
            .bytecode_sizes
            .refresh_if_idle(aggregate_key, || {
                db.bytecode_size_distribution(chain_id, aggregate_window.block_range)
            })
            .await
        {
            Ok(_) => {}
            Err(err) => log_prewarm_error(chain_id, "bytecode sizes", err),
        }

        let (compiler_bucket, compiler_start_block, compiler_end_block) = normalized_cache_range(
            &aggregate_query,
            aggregate_window,
            aggregate_window.bucket_blocks,
        );
        let compiler_key = LimitRangeCacheKey {
            chain_id,
            limit: DEFAULT_COMPILER_LIMIT,
            bucket: compiler_bucket,
            start_block: compiler_start_block,
            end_block: compiler_end_block,
        };
        match cache
            .compilers
            .refresh_if_idle(compiler_key, || async {
                Ok((
                    db.top_compilers(
                        chain_id,
                        DEFAULT_COMPILER_LIMIT,
                        aggregate_window.block_range,
                    )
                    .await?,
                    db.compiler_version_total(chain_id, aggregate_window.block_range)
                        .await?,
                ))
            })
            .await
        {
            Ok(_) => {}
            Err(err) => log_prewarm_error(chain_id, "compilers", err),
        }

        match cache
            .languages
            .refresh_if_idle(chain_id, || db.language_distribution(chain_id))
            .await
        {
            Ok(_) => {}
            Err(err) => log_prewarm_error(chain_id, "languages", err),
        }

        match cache
            .standards
            .refresh_if_idle(aggregate_key, || {
                db.standards_breakdown(chain_id, aggregate_window.block_range)
            })
            .await
        {
            Ok(_) => {}
            Err(err) => log_prewarm_error(chain_id, "standards", err),
        }

        let recent_key = RecentCacheKey {
            chain_id,
            limit: DEFAULT_RECENT_LIMIT,
            before_block: None,
            before_create_index: None,
        };
        match cache
            .recent
            .refresh_if_idle(recent_key, || {
                db.recent_contracts(chain_id, DEFAULT_RECENT_LIMIT, None)
            })
            .await
        {
            Ok(_) => {}
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
    let cache_key = bucket_cache_key(chain_id, &query, window);

    match cache
        .deploys
        .refresh_if_idle(cache_key, || {
            db.deploys_over_time(chain_id, window.bucket_blocks, window.block_range)
        })
        .await
    {
        Ok(_) => {}
        Err(err) => log_prewarm_error(chain_id, &format!("deployments {range}"), err),
    }

    match cache
        .verified
        .refresh_if_idle(cache_key, || {
            db.verified_ratio_over_time(chain_id, window.bucket_blocks, window.block_range)
        })
        .await
    {
        Ok(_) => {}
        Err(err) => log_prewarm_error(chain_id, &format!("verification {range}"), err),
    }
}

async fn refresh_verification_cache(db: &Db, cache: &ApiCache, chain_id: u64) {
    cache.stats.expire_where(|key| key == chain_id);
    cache.verified.expire_where(|key| key.chain_id == chain_id);
    cache.recent.expire_where(|key| key.chain_id == chain_id);

    match cache
        .stats
        .refresh_if_idle(chain_id, || db.stats(chain_id))
        .await
    {
        Ok(_) => {}
        Err(error) => log_prewarm_error(chain_id, "stats after VA sync", error),
    }

    let db_for_highest = db.clone();
    let highest = match cache
        .highest_blocks
        .get_or_refresh(chain_id, HIGHEST_BLOCK_TTL, move || async move {
            Ok(db_for_highest
                .highest_contract_block(chain_id)
                .await?
                .unwrap_or(0))
        })
        .await
    {
        Ok(block) => block,
        Err(error) => {
            log_prewarm_error(chain_id, "highest block after VA sync", error);
            return;
        }
    };
    let query = BucketQuery {
        chain_id: Some(chain_id),
        range: Some(INITIAL_VERIFIED_RANGE.to_string()),
        ..BucketQuery::default()
    };
    let window = parse_time_series_window(&query, chain_id, highest);
    let cache_key = bucket_cache_key(chain_id, &query, window);
    match cache
        .verified
        .refresh_if_idle(cache_key, || {
            db.verified_ratio_over_time(chain_id, window.bucket_blocks, window.block_range)
        })
        .await
    {
        Ok(_) => {}
        Err(error) => log_prewarm_error(chain_id, "verification after VA sync", error),
    }

    let recent_key = RecentCacheKey {
        chain_id,
        limit: DEFAULT_RECENT_LIMIT,
        before_block: None,
        before_create_index: None,
    };
    match cache
        .recent
        .refresh_if_idle(recent_key, || {
            db.recent_contracts(chain_id, DEFAULT_RECENT_LIMIT, None)
        })
        .await
    {
        Ok(_) => {}
        Err(error) => log_prewarm_error(chain_id, "recent deployments after VA sync", error),
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

fn is_relative_range(start_block: Option<u64>, end_block: Option<u64>) -> bool {
    end_block.is_none() && matches!(start_block, Some(1..=5))
}

fn is_live_relative_range(start_block: Option<u64>, end_block: Option<u64>) -> bool {
    end_block.is_none() && matches!(start_block, Some(1 | 2))
}

async fn refresh_tail_dashboard_cache(db: &Db, cache: &ApiCache, chain_id: u64) {
    // Keep existing values available while refreshes run. The time-series
    // rollups are cheap enough to refresh each tail; metadata widgets only
    // expire their 1H/1D views and refresh on demand because their joins are
    // much heavier on the production dataset.
    cache.stats.expire_where(|key| key == chain_id);
    cache.deploys.expire_where(|key| {
        key.chain_id == chain_id && is_relative_range(key.start_block, key.end_block)
    });
    cache.verified.expire_where(|key| {
        key.chain_id == chain_id && is_relative_range(key.start_block, key.end_block)
    });
    cache.bytecode_sizes.expire_where(|key| {
        key.chain_id == chain_id && is_live_relative_range(key.start_block, key.end_block)
    });
    cache.compilers.expire_where(|key| {
        key.chain_id == chain_id && is_live_relative_range(key.start_block, key.end_block)
    });
    cache.standards.expire_where(|key| {
        key.chain_id == chain_id && is_live_relative_range(key.start_block, key.end_block)
    });
    cache.recent.expire_where(|key| {
        key.chain_id == chain_id && key.before_block.is_none() && key.before_create_index.is_none()
    });

    let highest = match db.highest_contract_block(chain_id).await {
        Ok(Some(block)) => block,
        Ok(None) => 0,
        Err(error) => {
            log_prewarm_error(chain_id, "highest block after tail", error);
            return;
        }
    };
    cache.highest_blocks.insert(chain_id, highest);

    match cache
        .stats
        .refresh_if_idle(chain_id, || db.stats(chain_id))
        .await
    {
        Ok(_) => {}
        Err(error) => log_prewarm_error(chain_id, "stats after tail", error),
    }

    for range in [INITIAL_DEPLOYS_RANGE, INITIAL_VERIFIED_RANGE] {
        prewarm_chart_range(db, cache, chain_id, highest, range).await;
    }

    let recent_key = RecentCacheKey {
        chain_id,
        limit: DEFAULT_RECENT_LIMIT,
        before_block: None,
        before_create_index: None,
    };
    match cache
        .recent
        .refresh_if_idle(recent_key, || {
            db.recent_contracts(chain_id, DEFAULT_RECENT_LIMIT, None)
        })
        .await
    {
        Ok(_) => {}
        Err(error) => log_prewarm_error(chain_id, "recent deployments after tail", error),
    }
}

async fn background_tail_loop(
    db: Db,
    config: TailLoopConfig,
    runtime: Arc<RuntimeState>,
    cache: Arc<ApiCache>,
) {
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
                // Refresh the tail-sensitive entries without deleting stale
                // values that active dashboard requests can serve.
                if let Some(chain_id) = chain_id {
                    if report.rows > 0 {
                        refresh_tail_dashboard_cache(&db, &cache, chain_id).await;
                    }
                }
            }
            Ok(None) => {
                runtime.mark_tail_ok(chain_id, None, 0).await;
                tracing::debug!("tail: no new blocks");
            }
            Err(err) => {
                let msg = format!("{:#}", err);
                runtime.mark_tail_error(chain_id, msg.clone()).await;
                tracing::warn!("tail failed: {}", msg);
                if is_fatal_database_error(&msg) {
                    tracing::error!(
                        "tail loop stopped for chain_id={}: DuckDB was invalidated; restart blink serve",
                        chain_id
                            .map(|chain_id| chain_id.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    );
                    break;
                }
            }
        }
        tokio::time::sleep(config.interval).await;
    }
}

fn is_fatal_database_error(message: &str) -> bool {
    message.contains("database has been invalidated")
        || message.contains("Corrupted ART index")
        || message.contains("FATAL Error")
}

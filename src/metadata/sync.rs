//! Verification metadata sync loop.
//!
//! Local decode remains the source of truth for bytecode-derived metadata.
//! This loop only answers "is this contract verified by a source service?"
//! and stores the verification source plus a human-readable contract name
//! when one is available.
//!
//! Source order is intentional:
//! - with an Etherscan key: Etherscan first, Sourcify fallback;
//! - without an Etherscan key: Sourcify only;
//! - explicit `--skip-*` flags narrow that set.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};

use crate::{
    cli::MetadataSyncArgs,
    db::{Db, MetadataSyncTarget},
    util::{print_header, print_kv, print_kv_accent},
};

use super::{
    etherscan::{EtherscanClient, EtherscanFail, VerificationResult},
    sourcify::{SourcifyClient, SourcifyFail},
};

#[derive(Debug, Default, Clone)]
pub struct MetadataSyncStats {
    pub processed: u64,
    pub verified: u64,
    pub unverified: u64,
    pub failed: u64,
    pub from_sourcify: u64,
    pub from_etherscan: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct MetadataSyncOptions {
    pub limit: u64,
    pub rate_limit_rps: u32,
    pub recheck_after_secs: i64,
    pub newest_first: bool,
}

#[derive(Clone)]
pub struct VerificationSources {
    pub etherscan: Option<Arc<EtherscanClient>>,
    pub sourcify: Option<Arc<SourcifyClient>>,
}

impl VerificationSources {
    fn is_empty(&self) -> bool {
        self.etherscan.is_none() && self.sourcify.is_none()
    }

    fn label(&self) -> &'static str {
        match (self.etherscan.is_some(), self.sourcify.is_some()) {
            (true, true) => "etherscan -> sourcify (fallback)",
            (true, false) => "etherscan only",
            (false, true) => "sourcify only",
            (false, false) => "none",
        }
    }
}

#[derive(Debug)]
struct VerifiedOutcome {
    result: VerificationResult,
    source: &'static str,
    match_type: Option<String>,
}

pub async fn run_metadata_sync(args: MetadataSyncArgs) -> Result<()> {
    if args.skip_sourcify && args.skip_etherscan {
        return Err(anyhow!(
            "both --skip-sourcify and --skip-etherscan set; nothing to do"
        ));
    }

    let sources = build_sources(&args)?;
    if sources.is_empty() {
        return Err(anyhow!("no verification sources enabled"));
    }

    let db = Db::open(&args.data_dir, &args.contracts_glob)?;

    print_header("blink metadata-sync");
    print_kv("source", &args.data_dir.display().to_string());
    print_kv(
        "writing to",
        &format!(
            "{} · verification metadata",
            args.data_dir.join("blink.duckdb").display()
        ),
    );
    print_kv("chain id", &args.chain_id.to_string());
    print_kv_accent("rate limit", &format!("{} req/sec", args.rate_limit_rps));
    print_kv("sources", sources.label());
    if args.limit > 0 {
        print_kv_accent("limit", &format!("{} addresses", args.limit));
    } else {
        print_kv("limit", "none (process all pending)");
    }
    println!();

    let stats = metadata_sync_loop(
        db,
        sources,
        MetadataSyncOptions {
            limit: args.limit,
            rate_limit_rps: args.rate_limit_rps,
            recheck_after_secs: args.recheck_unverified_after_secs,
            newest_first: args.newest_first,
        },
    )
    .await?;

    println!();
    print_kv_accent("processed", &stats.processed.to_string());
    print_kv_accent("verified", &stats.verified.to_string());
    print_kv("unverified", &stats.unverified.to_string());
    if stats.from_etherscan > 0 {
        print_kv("via etherscan", &stats.from_etherscan.to_string());
    }
    if stats.from_sourcify > 0 {
        print_kv("via sourcify", &stats.from_sourcify.to_string());
    }
    if stats.failed > 0 {
        print_kv("failed", &stats.failed.to_string());
    }
    Ok(())
}

fn build_sources(args: &MetadataSyncArgs) -> Result<VerificationSources> {
    let etherscan = if args.skip_etherscan {
        None
    } else {
        match resolve_api_key(args.etherscan_api_key.clone()) {
            Ok(key) => Some(Arc::new(EtherscanClient::new(
                args.etherscan_url.clone(),
                key,
                args.chain_id,
            )?)),
            Err(_) if !args.skip_sourcify => {
                tracing::warn!(
                    "no Etherscan API key - using Sourcify only \
                     (pass --skip-etherscan to silence this warning)"
                );
                None
            }
            Err(e) => return Err(e),
        }
    };

    let sourcify = if args.skip_sourcify {
        None
    } else {
        Some(Arc::new(SourcifyClient::new(
            args.sourcify_url.clone(),
            args.chain_id,
        )?))
    };

    Ok(VerificationSources {
        etherscan,
        sourcify,
    })
}

pub fn resolve_api_key(arg: Option<String>) -> Result<String> {
    arg.or_else(|| std::env::var("ETHERSCAN_API_KEY").ok())
        .ok_or_else(|| anyhow!("missing --etherscan-api-key and ETHERSCAN_API_KEY"))
}

pub async fn metadata_sync_loop(
    db: Db,
    sources: VerificationSources,
    options: MetadataSyncOptions,
) -> Result<MetadataSyncStats> {
    if sources.is_empty() {
        return Err(anyhow!("no verification sources enabled"));
    }

    tracing::info!(
        "selecting addresses for metadata sync (this may take a moment on first run)..."
    );
    let select_start = Instant::now();
    let targets = db
        .pick_metadata_sync_targets(
            options.limit,
            options.recheck_after_secs,
            options.newest_first,
        )
        .await
        .context("select metadata sync targets")?;
    tracing::info!(
        "selected {} addresses in {:.1}s",
        targets.len(),
        select_start.elapsed().as_secs_f64()
    );

    if targets.is_empty() {
        tracing::info!("nothing to sync (all addresses already up to date)");
        return Ok(MetadataSyncStats::default());
    }

    let total = targets.len();
    let rps = options.rate_limit_rps.max(1) as u64;
    let interval = Duration::from_millis(1000 / rps);
    let mut stats = MetadataSyncStats::default();
    let mut last_log = Instant::now();
    let mut next_send = Instant::now();

    tracing::info!(
        "starting metadata sync: {} addresses, {} req/sec ({}ms apart), sources={}",
        total,
        rps,
        interval.as_millis(),
        sources.label()
    );

    for (i, target) in targets.iter().enumerate() {
        let outcome = match verify_target(&sources, target, interval, &mut next_send).await {
            Ok(outcome) => outcome,
            Err(EtherscanFail::InvalidApiKey) => {
                return Err(anyhow!(
                    "etherscan rejected the API key - fix ETHERSCAN_API_KEY or pass --skip-etherscan"
                ));
            }
            Err(err) => {
                tracing::warn!("{} verification failed: {}", target.address_hex, err);
                stats.processed += 1;
                stats.failed += 1;
                log_progress(&stats, total, &mut last_log, i + 1);
                continue;
            }
        };

        let (result, source, match_type) = match outcome {
            Some(outcome) => (
                outcome.result,
                Some(outcome.source.to_string()),
                outcome.match_type,
            ),
            None => (
                VerificationResult {
                    is_verified: false,
                    contract_name: None,
                },
                None,
                None,
            ),
        };

        let is_verified = result.is_verified;
        if let Err(err) = db
            .upsert_enrichment(
                target.address.clone(),
                result,
                source.clone(),
                match_type,
                target.block_number,
                target.create_index,
            )
            .await
        {
            tracing::warn!("upsert {} failed: {:#}", target.address_hex, err);
            stats.failed += 1;
        } else if is_verified {
            stats.verified += 1;
            match source.as_deref() {
                Some("etherscan") => stats.from_etherscan += 1,
                Some("sourcify") => stats.from_sourcify += 1,
                _ => {}
            }
        } else {
            stats.unverified += 1;
        }
        stats.processed += 1;

        log_progress(&stats, total, &mut last_log, i + 1);
    }

    Ok(stats)
}

async fn verify_target(
    sources: &VerificationSources,
    target: &MetadataSyncTarget,
    interval: Duration,
    next_send: &mut Instant,
) -> std::result::Result<Option<VerifiedOutcome>, EtherscanFail> {
    let mut etherscan_error: Option<String> = None;

    if let Some(etherscan) = &sources.etherscan {
        wait_for_slot(interval, next_send).await;
        match etherscan.get_source_code(&target.address_hex).await {
            Ok(result) if result.is_verified => {
                return Ok(Some(VerifiedOutcome {
                    result,
                    source: "etherscan",
                    match_type: None,
                }));
            }
            Ok(_) => {
                // Not verified on Etherscan. Try Sourcify before marking final
                // unverified, because either service can have unique coverage.
            }
            Err(EtherscanFail::InvalidApiKey) => return Err(EtherscanFail::InvalidApiKey),
            Err(EtherscanFail::RateLimited) => return Err(EtherscanFail::RateLimited),
            Err(err) => {
                tracing::warn!("etherscan {} failed: {}", target.address_hex, err);
                etherscan_error = Some(err.to_string());
                if sources.sourcify.is_none() {
                    return Err(err);
                }
            }
        }
    }

    if let Some(sourcify) = &sources.sourcify {
        wait_for_slot(interval, next_send).await;
        match sourcify.lookup(&target.address_hex).await {
            Ok(result) => {
                return Ok(Some(VerifiedOutcome {
                    result: VerificationResult {
                        is_verified: true,
                        contract_name: result.contract_name,
                    },
                    source: "sourcify",
                    match_type: result.match_type,
                }));
            }
            Err(SourcifyFail::NotFound) => {
                if let Some(err) = etherscan_error {
                    return Err(EtherscanFail::Other(anyhow!(
                        "etherscan failed and sourcify had no match: {}",
                        err
                    )));
                }
            }
            Err(SourcifyFail::RateLimited) => {
                return Err(EtherscanFail::Other(anyhow!("sourcify: rate limited")));
            }
            Err(err) => {
                tracing::warn!("sourcify {} failed: {}", target.address_hex, err);
                return Err(EtherscanFail::Other(anyhow!(err.to_string())));
            }
        }
    }

    if let Some(err) = etherscan_error {
        return Err(EtherscanFail::Other(anyhow!(
            "etherscan failed and no fallback verified: {}",
            err
        )));
    }

    Ok(None)
}

async fn wait_for_slot(interval: Duration, next_send: &mut Instant) {
    let now = Instant::now();
    if now < *next_send {
        tokio::time::sleep(*next_send - now).await;
    }
    *next_send = Instant::now() + interval;
}

fn log_progress(
    stats: &MetadataSyncStats,
    total: usize,
    last_log: &mut Instant,
    current_index: usize,
) {
    if last_log.elapsed() < Duration::from_secs(2) && current_index < total {
        return;
    }
    let pct = (stats.processed as f64 / total as f64 * 100.0).min(100.0);
    tracing::info!(
        "sync progress: {}/{} ({:.0}%) · verified={} (etherscan={} sourcify={}) · unverified={} · failed={}",
        stats.processed,
        total,
        pct,
        stats.verified,
        stats.from_etherscan,
        stats.from_sourcify,
        stats.unverified,
        stats.failed
    );
    *last_log = Instant::now();
}

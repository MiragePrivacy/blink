mod batch;
mod parquet_io;
pub(crate) mod tail;
mod traces;

use std::{collections::BTreeMap, fs, sync::Arc, time::Duration};

use alloy::{
    providers::{Provider, ProviderBuilder},
    rpc::types::trace::parity::LocalizedTransactionTrace,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures::{stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};

use self::{batch::BatchClient, traces::extract_contracts};
use crate::{
    cli::ContractsArgs,
    types::{ChunkReport, RunReport},
    util::{
        build_chunks, chunk_path, color_accent, color_dim, color_red, format_count,
        format_duration, print_header, print_kv, print_kv_accent, resolve_end_block,
        resolve_rpc_url, write_report,
    },
};

pub async fn run_contracts(mut args: ContractsArgs) -> Result<()> {
    if args.fast {
        args.batch_size = args.batch_size.max(100);
        args.max_concurrent_requests = args.max_concurrent_requests.max(200);
        args.max_concurrent_chunks = args.max_concurrent_chunks.max(8);
        args.initial_backoff_ms = args.initial_backoff_ms.min(500);
    }

    let rpc_url = resolve_rpc_url(args.rpc.clone())?;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse().context("invalid rpc url")?);
    let chain_id = provider
        .get_chain_id()
        .await
        .context("failed to fetch chain id")?;

    let end_block = resolve_end_block(&provider, &args.end_block).await?;
    if end_block < args.start_block {
        return Err(anyhow!(
            "end block {} is before start block {}",
            end_block,
            args.start_block
        ));
    }

    fs::create_dir_all(&args.output_dir)
        .with_context(|| format!("create output dir {}", args.output_dir.display()))?;
    let report_dir = args
        .report_dir
        .clone()
        .unwrap_or_else(|| args.output_dir.join(".blink").join("reports"));
    fs::create_dir_all(&report_dir)
        .with_context(|| format!("create report dir {}", report_dir.display()))?;

    let chunk_ranges = build_chunks(args.start_block, end_block, args.chunk_size);
    let total_blocks = end_block - args.start_block + 1;
    let start_time = Utc::now();

    print_header("blink contracts");
    print_kv_accent(
        "blocks",
        &format!(
            "{} ({} → {})",
            format_count(total_blocks),
            format_count(args.start_block),
            format_count(end_block)
        ),
    );
    print_kv("rpc", &rpc_url);
    print_kv(
        "concurrency",
        &format!(
            "{} requests · {} chunks",
            args.max_concurrent_requests, args.max_concurrent_chunks
        ),
    );
    print_kv(
        "output",
        &format!(
            "{} · parquet · {} chunks of {}",
            args.output_dir.display(),
            chunk_ranges.len(),
            format_count(args.chunk_size)
        ),
    );
    println!();

    let batch_client = Arc::new(BatchClient::new(
        rpc_url.clone(),
        args.max_concurrent_requests,
    )?);

    let progress = ProgressBar::new(total_blocks);
    progress.set_style(
        ProgressStyle::default_bar()
            .template(
                "  \x1b[38;2;189;255;0m▸\x1b[0m [{elapsed_precise}] \x1b[38;2;189;255;0m{bar:40}\x1b[38;2;64;64;64m{bar:0}\x1b[0m {pos}/{len} blocks · {percent}% · eta {eta} · {msg}",
            )
            .unwrap()
            .progress_chars("█░ "),
    );
    progress.enable_steady_tick(Duration::from_millis(200));

    let mut reports = Vec::with_capacity(chunk_ranges.len());
    let chunks_total = chunk_ranges.len();
    let mut error: Option<String> = None;

    let mut chunk_stream = stream::iter(chunk_ranges)
        .map(|chunk| {
            let batch_client = batch_client.clone();
            let args = args.clone();
            let progress = progress.clone();
            async move {
                process_chunk(
                    &batch_client,
                    &args,
                    chain_id,
                    chunk.clone(),
                    progress,
                    chunks_total,
                )
                .await
            }
        })
        .buffer_unordered(args.max_concurrent_chunks);

    let mut completed = 0usize;
    while let Some(result) = chunk_stream.next().await {
        match result {
            Ok(report) => {
                completed += 1;
                let status = if report.skipped {
                    color_dim("skipped")
                } else {
                    color_accent(&format!("{} contracts", format_count(report.rows as u64)))
                };
                progress.set_message(format!(
                    "chunk {}/{} ({}–{}) {}",
                    completed, chunks_total, report.start_block, report.end_block, status
                ));
                reports.push(report);
            }
            Err(err) => {
                progress.abandon_with_message(color_red("failed"));
                error = Some(err.to_string());
                break;
            }
        }
    }

    if error.is_none() {
        progress.finish_with_message(color_accent("done"));
    }

    let end_time = Utc::now();
    let duration = end_time - start_time;
    let total_rows: usize = reports.iter().map(|r| r.rows).sum();

    println!();
    print_kv_accent("elapsed", &format_duration(duration));
    print_kv_accent("contracts", &format_count(total_rows as u64));
    print_kv_accent(
        "speed",
        &format!(
            "{:.1} blocks/sec",
            total_blocks as f64 / duration.num_seconds().max(1) as f64
        ),
    );

    reports.sort_by_key(|r| r.index);
    let status = if error.is_some() {
        "failed"
    } else {
        "completed"
    };
    let report = RunReport {
        started_at: start_time,
        finished_at: end_time,
        status: status.to_string(),
        error: error.clone(),
        chain_id,
        rpc_url: rpc_url.clone(),
        start_block: args.start_block,
        end_block,
        chunk_size: args.chunk_size,
        batch_size: args.batch_size,
        max_concurrent_requests: args.max_concurrent_requests,
        max_concurrent_chunks: args.max_concurrent_chunks,
        output_dir: args.output_dir.clone(),
        chunks: reports,
    };
    let report_path = report_dir.join(format!("{}.json", start_time.format("%Y-%m-%d_%H-%M-%S")));
    write_report(&report_path, &report)?;
    print_kv("report", &report_path.display().to_string());

    if let Some(err) = error {
        return Err(anyhow!(err));
    }

    Ok(())
}

async fn process_chunk(
    batch_client: &Arc<BatchClient>,
    args: &ContractsArgs,
    chain_id: u64,
    chunk: crate::types::ChunkRange,
    progress: ProgressBar,
    chunks_total: usize,
) -> Result<ChunkReport> {
    let started_at = Utc::now();
    let output_path = chunk_path(&args.output_dir, chunk.start, chunk.end);

    if output_path.exists() && !args.overwrite {
        let size_bytes = match fs::metadata(&output_path) {
            Ok(meta) => Some(meta.len()),
            Err(_) => None,
        };
        progress.inc(chunk.end - chunk.start + 1);
        return Ok(ChunkReport {
            index: chunk.index,
            start_block: chunk.start,
            end_block: chunk.end,
            rows: 0,
            output_path,
            size_bytes,
            started_at,
            finished_at: Utc::now(),
            skipped: true,
        });
    }

    let mut batches = Vec::new();
    let mut current = chunk.start;
    while current <= chunk.end {
        let batch_end = (current + args.batch_size as u64 - 1).min(chunk.end);
        let blocks: Vec<u64> = (current..=batch_end).collect();
        batches.push(blocks);
        if batch_end == chunk.end {
            break;
        }
        current = batch_end + 1;
    }

    let batch_sizes: Vec<u64> = batches.iter().map(|b| b.len() as u64).collect();
    let batches_total = batch_sizes.len();
    let schema = parquet_io::schema();
    let mut writer = None;
    let mut rows_written = 0usize;

    let mut pending: BTreeMap<usize, Vec<(u64, Vec<LocalizedTransactionTrace>)>> = BTreeMap::new();
    let mut next_index = 0usize;

    let mut batch_stream = stream::iter(batches.into_iter().enumerate())
        .map(|(index, blocks)| {
            let client = batch_client.clone();
            let max_retries = args.max_retries;
            let backoff = Duration::from_millis(args.initial_backoff_ms);
            let max_backoff = Duration::from_millis(args.max_backoff_ms);
            async move {
                let traces = client
                    .trace_block_batch(&blocks, max_retries, backoff, max_backoff)
                    .await?;
                Ok::<(usize, Vec<(u64, Vec<LocalizedTransactionTrace>)>), anyhow::Error>((
                    index, traces,
                ))
            }
        })
        .buffer_unordered(args.max_concurrent_requests);

    while let Some(result) = batch_stream.next().await {
        let (index, mut traces) = result?;
        traces.sort_by_key(|(block, _)| *block);
        pending.insert(index, traces);

        while let Some(traces) = pending.remove(&next_index) {
            let mut batch_rows = Vec::new();
            for (_, block_traces) in traces {
                let mut block_rows = extract_contracts(&block_traces, chain_id)?;
                batch_rows.append(&mut block_rows);
            }
            if !batch_rows.is_empty() {
                batch_rows.sort_unstable_by(|a, b| {
                    a.block_number
                        .cmp(&b.block_number)
                        .then_with(|| a.create_index.cmp(&b.create_index))
                });
                if writer.is_none() {
                    writer = Some(parquet_io::create_writer(&output_path, schema.clone())?);
                }
                let batch = parquet_io::rows_to_batch(&batch_rows, schema.clone())?;
                if let Some(writer) = writer.as_mut() {
                    writer.write(&batch)?;
                }
                rows_written += batch_rows.len();
            }
            next_index += 1;
            progress.inc(batch_sizes[next_index - 1]);
            progress.set_message(format!(
                "chunk {}/{} batch {}/{}",
                chunk.index + 1,
                chunks_total,
                next_index,
                batches_total
            ));
        }
    }

    if let Some(writer) = writer {
        writer.close()?;
    } else if output_path.exists() && args.overwrite {
        fs::remove_file(&output_path)
            .with_context(|| format!("remove {}", output_path.display()))?;
    }

    let size_bytes = match fs::metadata(&output_path) {
        Ok(meta) => Some(meta.len()),
        Err(_) => None,
    };

    Ok(ChunkReport {
        index: chunk.index,
        start_block: chunk.start,
        end_block: chunk.end,
        rows: rows_written,
        output_path,
        size_bytes,
        started_at,
        finished_at: Utc::now(),
        skipped: false,
    })
}

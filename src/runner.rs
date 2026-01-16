use std::{
    collections::BTreeMap,
    fs,
    sync::Arc,
    time::Duration,
};

use alloy::{
    providers::{Provider, ProviderBuilder},
    rpc::types::trace::parity::LocalizedTransactionTrace,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures::{stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    batch::BatchClient,
    cli::ContractsArgs,
    extract::extract_contracts,
    parquet_io,
    types::{ChunkReport, RunReport},
    util::{
        build_chunks, chunk_path, color_green, color_red, color_white, format_duration,
        resolve_end_block, resolve_rpc_url, write_report,
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

    println!("{}", color_white("blink parameters"));
    println!("{}", color_white("───────────────"));
    println!("{}", color_white("- data:"));
    println!(
        "{}",
        color_white(&format!(
            "    - blocks: n={} min={} max={}",
            total_blocks, args.start_block, end_block
        ))
    );
    println!("{}", color_white("- source:"));
    println!("{}", color_white(&format!("    - rpc url: {}", rpc_url)));
    println!(
        "{}",
        color_white(&format!(
            "    - max concurrent requests: {}",
            args.max_concurrent_requests
        ))
    );
    println!(
        "{}",
        color_white(&format!(
            "    - max concurrent chunks: {}",
            args.max_concurrent_chunks
        ))
    );
    println!("{}", color_white("- output:"));
    println!(
        "{}",
        color_white(&format!("    - chunk size: {}", args.chunk_size))
    );
    println!(
        "{}",
        color_white(&format!(
            "    - chunks to collect: {} / {}",
            chunk_ranges.len(),
            chunk_ranges.len()
        ))
    );
    println!("{}", color_white("    - output format: parquet"));
    println!(
        "{}",
        color_white(&format!("    - output dir: {}", args.output_dir.display()))
    );
    println!(
        "{}",
        color_white(&format!(
            "    - report file: {}/.blink/reports/<timestamp>.json",
            args.output_dir.display()
        ))
    );
    println!();
    println!("{}", color_white("schema for contracts"));
    println!("{}", color_white("────────────────────"));
    println!("{}", color_white("- block_number: uint32"));
    println!("{}", color_white("- block_hash: binary"));
    println!("{}", color_white("- create_index: uint32"));
    println!("{}", color_white("- transaction_hash: binary"));
    println!("{}", color_white("- contract_address: binary"));
    println!("{}", color_white("- deployer: binary"));
    println!("{}", color_white("- factory: binary"));
    println!("{}", color_white("- init_code: binary"));
    println!("{}", color_white("- code: binary"));
    println!("{}", color_white("- init_code_hash: binary"));
    println!("{}", color_white("- n_init_code_bytes: uint32"));
    println!("{}", color_white("- n_code_bytes: uint32"));
    println!("{}", color_white("- code_hash: binary"));
    println!("{}", color_white("- chain_id: uint64"));
    println!();
    println!(
        "{}",
        color_white("sorting contracts by: block_number, create_index")
    );
    println!();
    println!("{}", color_white("other available columns: [none]"));
    println!();
    println!("{}", color_white("collecting data"));
    println!("{}", color_white("───────────────"));

    let batch_client = Arc::new(BatchClient::new(
        rpc_url.clone(),
        args.max_concurrent_requests,
    )?);

    let progress = ProgressBar::new(total_blocks);
    progress.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.white} [{elapsed_precise}] [{bar:50.white/white}] {pos}/{len} blocks ({percent}%) | ETA: {eta} | {msg}",
            )
            .unwrap()
            .progress_chars("█▓░"),
    );
    progress.enable_steady_tick(Duration::from_millis(200));

    let mut reports = Vec::with_capacity(chunk_ranges.len());
    let chunks_total = chunk_ranges.len();
    let mut error: Option<String> = None;

    let mut chunk_stream = stream::iter(chunk_ranges.into_iter())
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
                    color_white("skipped")
                } else {
                    color_green(&format!("{} contracts", report.rows))
                };
                progress.set_message(format!(
                    "chunk {}/{} ({}-{}) {}",
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
        progress.finish_with_message(color_green("done"));
    }

    let end_time = Utc::now();
    let duration = end_time - start_time;
    let total_rows: usize = reports.iter().map(|r| r.rows).sum();

    println!(
        "\n{}",
        color_white(&format!("completed in {}", format_duration(duration)))
    );
    println!(
        "{}",
        color_white(&format!("total contracts: {}", total_rows))
    );
    println!(
        "{}",
        color_white(&format!(
            "speed: {:.1} blocks/sec",
            total_blocks as f64 / duration.num_seconds().max(1) as f64
        ))
    );

    reports.sort_by_key(|r| r.index);
    let status = if error.is_some() { "failed" } else { "completed" };
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
    println!(
        "{}",
        color_white(&format!("report: {}", report_path.display()))
    );

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

//! Continuous tail extraction for the dashboard's `serve` mode.

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
    time::Duration,
};

use alloy::{
    providers::{Provider, ProviderBuilder},
    rpc::types::trace::parity::LocalizedTransactionTrace,
};
use anyhow::{Context, Result};
use chrono::Utc;
use futures::{stream, StreamExt};

use super::{batch::BatchClient, parquet_io, traces::extract_contracts};
use crate::{db::Db, types::ChunkReport};

const TAIL_BATCH_BLOCK_LIMIT: u64 = 1_000;

pub async fn rpc_chain_id(rpc_url: &str) -> Result<u64> {
    let provider =
        ProviderBuilder::new().connect_http(rpc_url.parse().context("invalid tail rpc url")?);
    provider.get_chain_id().await.context("tail: get chain id")
}

pub async fn tail_once(
    db: &Db,
    rpc_url: &str,
    confirmations: u64,
    batch_size: usize,
    max_concurrent_requests: usize,
    data_dir: &Path,
) -> Result<Option<ChunkReport>> {
    let provider =
        ProviderBuilder::new().connect_http(rpc_url.parse().context("invalid tail rpc url")?);
    let chain_id = provider
        .get_chain_id()
        .await
        .context("tail: get chain id")?;
    let head = provider
        .get_block_number()
        .await
        .context("tail: get head block")?;
    let target = head.saturating_sub(confirmations);

    sync_block_time_checkpoint(db, rpc_url, chain_id, target).await?;

    let highest_indexed = db.highest_block(chain_id).await?.unwrap_or(0);
    let start_block = if highest_indexed == 0 {
        target
    } else {
        highest_indexed + 1
    };
    if start_block > target {
        return Ok(None);
    }
    let end_block = (start_block + TAIL_BATCH_BLOCK_LIMIT - 1).min(target);
    tracing::info!(
        "tail scanning chain_id={} blocks {}-{}",
        chain_id,
        start_block,
        end_block
    );

    let started_at = Utc::now();
    let output_path = data_dir.join(format!(
        "tail__chain_{:010}__{:010}__{:010}.parquet",
        chain_id, start_block, end_block
    ));
    let temp_output_path = output_path.with_extension("parquet.tmp");
    let _ = std::fs::remove_file(&temp_output_path);

    let batch_client = Arc::new(BatchClient::new(
        rpc_url.to_string(),
        max_concurrent_requests,
    )?);

    let mut batches = Vec::new();
    let mut current = start_block;
    while current <= end_block {
        let batch_end = (current + batch_size as u64 - 1).min(end_block);
        batches.push((current..=batch_end).collect::<Vec<u64>>());
        if batch_end == end_block {
            break;
        }
        current = batch_end + 1;
    }

    let schema = parquet_io::schema();
    let mut writer = None;
    let mut rows_written = 0usize;
    let mut live_bytecodes = HashMap::<Vec<u8>, Vec<u8>>::new();

    let mut pending: BTreeMap<usize, Vec<(u64, Vec<LocalizedTransactionTrace>)>> = BTreeMap::new();
    let mut next_index = 0usize;

    let mut batch_stream = stream::iter(batches.into_iter().enumerate())
        .map(|(index, blocks)| {
            let client = batch_client.clone();
            async move {
                let traces = client
                    .trace_block_batch(
                        &blocks,
                        5,
                        Duration::from_millis(500),
                        Duration::from_secs(15),
                    )
                    .await?;
                Ok::<(usize, Vec<(u64, Vec<LocalizedTransactionTrace>)>), anyhow::Error>((
                    index, traces,
                ))
            }
        })
        .buffer_unordered(max_concurrent_requests);

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
                for row in &batch_rows {
                    live_bytecodes
                        .entry(row.code_hash.clone())
                        .or_insert_with(|| row.code.clone());
                }
                batch_rows.sort_unstable_by(|a, b| {
                    a.block_number
                        .cmp(&b.block_number)
                        .then_with(|| a.create_index.cmp(&b.create_index))
                });
                if writer.is_none() {
                    writer = Some(parquet_io::create_writer(
                        &temp_output_path,
                        schema.clone(),
                    )?);
                }
                let batch = parquet_io::rows_to_batch(&batch_rows, schema.clone())?;
                if let Some(writer) = writer.as_mut() {
                    writer.write(&batch)?;
                }
                rows_written += batch_rows.len();
            }
            next_index += 1;
        }
    }

    if let Some(writer) = writer {
        writer.close()?;
        std::fs::rename(&temp_output_path, &output_path).with_context(|| {
            format!(
                "rename {} -> {}",
                temp_output_path.display(),
                output_path.display()
            )
        })?;
    }

    db.refresh().await?;
    let decoded = db
        .decode_live_bytecodes(live_bytecodes.into_iter().collect())
        .await?;
    if decoded > 0 {
        tracing::info!("decoded {} new live bytecode(s)", decoded);
    }

    let size_bytes = std::fs::metadata(&output_path).ok().map(|m| m.len());
    Ok(Some(ChunkReport {
        index: 0,
        start_block,
        end_block,
        rows: rows_written,
        output_path,
        size_bytes,
        started_at,
        finished_at: Utc::now(),
        skipped: false,
    }))
}

async fn sync_block_time_checkpoint(
    db: &Db,
    rpc_url: &str,
    chain_id: u64,
    target: u64,
) -> Result<()> {
    let latest = db.latest_checkpoint_block(chain_id).await?;
    let interval = crate::blocks::blocks_per_day(chain_id, target).max(1);
    if latest.is_some_and(|block| target.saturating_sub(block) < interval) {
        return Ok(());
    }

    let client = BatchClient::new(rpc_url.to_string(), 1)?;
    let checkpoint = client
        .block_timestamps_batch(
            &[target],
            5,
            Duration::from_millis(500),
            Duration::from_secs(15),
        )
        .await?
        .into_iter()
        .next()
        .context("tail: missing checkpoint block response")?;
    db.record_block_checkpoint(chain_id, checkpoint.0, checkpoint.1)
        .await?;
    tracing::info!(
        "recorded block-time checkpoint chain_id={} block={}",
        chain_id,
        checkpoint.0
    );
    Ok(())
}

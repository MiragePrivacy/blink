//! Batched JSON-RPC client for `trace_block`.
//!
//! Groups multiple block traces into a single HTTP request, with semaphore-
//! controlled concurrency, exponential backoff, and rate-limit detection.
//! Used by both the bulk extractor and the dashboard's tail loop.

use std::{sync::Arc, time::Duration};

use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Serialize)]
struct JsonRpcRequest {
    jsonrpc: &'static str,
    method: &'static str,
    params: Vec<serde_json::Value>,
    id: u64,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    id: u64,
    result: Option<serde_json::Value>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize, Debug)]
struct JsonRpcError {
    code: i64,
    message: String,
}

pub struct BatchClient {
    client: Client,
    rpc_url: String,
    semaphore: Arc<Semaphore>,
}

impl BatchClient {
    pub fn new(rpc_url: String, max_concurrent: usize) -> Result<Self> {
        let client = Client::builder()
            .gzip(true)
            .pool_max_idle_per_host(max_concurrent)
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|err| anyhow!("failed to build HTTP client: {}", err))?;
        Ok(Self {
            client,
            rpc_url,
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        })
    }

    pub async fn trace_block_batch(
        &self,
        blocks: &[u64],
        max_retries: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Vec<(u64, Vec<LocalizedTransactionTrace>)>> {
        let requests: Vec<JsonRpcRequest> = blocks
            .iter()
            .enumerate()
            .map(|(i, &block)| JsonRpcRequest {
                jsonrpc: "2.0",
                method: "trace_block",
                params: vec![serde_json::Value::String(format!("0x{:x}", block))],
                id: i as u64,
            })
            .collect();

        let mut attempts = 0u32;
        let mut backoff = initial_backoff;

        loop {
            attempts += 1;
            let _permit = self.semaphore.acquire().await?;
            let result = self.client.post(&self.rpc_url).json(&requests).send().await;

            match result {
                Ok(response) => {
                    if !response.status().is_success() {
                        if attempts <= max_retries {
                            drop(_permit);
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff.saturating_mul(2)).min(max_backoff);
                            continue;
                        }
                        return Err(anyhow!(
                            "HTTP error {} for blocks {:?}",
                            response.status(),
                            blocks
                        ));
                    }

                    let responses: Vec<JsonRpcResponse> = response.json().await?;
                    let mut results: Vec<Option<Vec<LocalizedTransactionTrace>>> =
                        vec![None; blocks.len()];

                    let mut rate_limited = false;
                    for resp in responses {
                        if resp.id as usize >= blocks.len() {
                            return Err(anyhow!(
                                "invalid response id {} for batch len {}",
                                resp.id,
                                blocks.len()
                            ));
                        }
                        if let Some(err) = resp.error {
                            if is_rate_limited(&err) {
                                rate_limited = true;
                                break;
                            }
                            return Err(anyhow!(
                                "RPC error for block {}: {} (code {})",
                                blocks[resp.id as usize],
                                err.message,
                                err.code
                            ));
                        }
                        let value = resp.result.ok_or_else(|| {
                            anyhow!("missing result for block {}", blocks[resp.id as usize])
                        })?;
                        let traces = decode_trace_block_value(blocks[resp.id as usize], value)?;
                        results[resp.id as usize] = Some(traces);
                    }

                    if rate_limited {
                        if attempts <= max_retries {
                            drop(_permit);
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff.saturating_mul(2)).min(max_backoff);
                            continue;
                        }
                        return Err(anyhow!(
                            "RPC rate limited for blocks {:?} after {} retries",
                            blocks,
                            max_retries
                        ));
                    }

                    let mut output = Vec::with_capacity(blocks.len());
                    for (idx, block) in blocks.iter().enumerate() {
                        let traces = results[idx]
                            .take()
                            .ok_or_else(|| anyhow!("missing response for block {}", block))?;
                        output.push((*block, traces));
                    }
                    return Ok(output);
                }
                Err(_) if attempts <= max_retries => {
                    drop(_permit);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff.saturating_mul(2)).min(max_backoff);
                    continue;
                }
                Err(err) => {
                    return Err(anyhow!("Request failed for blocks {:?}: {}", blocks, err));
                }
            }
        }
    }

    pub async fn block_timestamps_batch(
        &self,
        blocks: &[u64],
        max_retries: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Vec<(u64, i64)>> {
        let requests = blocks
            .iter()
            .enumerate()
            .map(|(index, block)| JsonRpcRequest {
                jsonrpc: "2.0",
                method: "eth_getBlockByNumber",
                params: vec![
                    serde_json::Value::String(format!("0x{block:x}")),
                    serde_json::Value::Bool(false),
                ],
                id: index as u64,
            })
            .collect::<Vec<_>>();

        let mut attempts = 0u32;
        let mut backoff = initial_backoff;
        loop {
            attempts += 1;
            let permit = self.semaphore.acquire().await?;
            let response = self.client.post(&self.rpc_url).json(&requests).send().await;
            match response {
                Ok(response) if response.status().is_success() => {
                    let responses: Vec<JsonRpcResponse> = response.json().await?;
                    let mut timestamps = vec![None; blocks.len()];
                    let mut rate_limited = false;
                    for response in responses {
                        let index = response.id as usize;
                        if index >= blocks.len() {
                            return Err(anyhow!(
                                "invalid response id {} for batch len {}",
                                response.id,
                                blocks.len()
                            ));
                        }
                        if let Some(error) = response.error {
                            if is_rate_limited(&error) {
                                rate_limited = true;
                                break;
                            }
                            return Err(anyhow!(
                                "RPC error for block {}: {} (code {})",
                                blocks[index],
                                error.message,
                                error.code
                            ));
                        }
                        let value = response
                            .result
                            .ok_or_else(|| anyhow!("missing block {}", blocks[index]))?;
                        let timestamp = value
                            .get("timestamp")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                anyhow!("missing timestamp for block {}", blocks[index])
                            })?;
                        let timestamp = parse_quantity(timestamp).with_context(|| {
                            format!("invalid timestamp for block {}", blocks[index])
                        })?;
                        timestamps[index] = Some(timestamp as i64);
                    }

                    if rate_limited {
                        if attempts <= max_retries {
                            drop(permit);
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff.saturating_mul(2)).min(max_backoff);
                            continue;
                        }
                        return Err(anyhow!(
                            "RPC rate limited for block timestamps {:?} after {} retries",
                            blocks,
                            max_retries
                        ));
                    }

                    return blocks
                        .iter()
                        .enumerate()
                        .map(|(index, block)| {
                            timestamps[index]
                                .map(|timestamp| (*block, timestamp))
                                .ok_or_else(|| anyhow!("missing timestamp for block {block}"))
                        })
                        .collect();
                }
                Ok(response) if attempts <= max_retries => {
                    tracing::debug!(
                        "block timestamp RPC returned {}; retrying",
                        response.status()
                    );
                    drop(permit);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff.saturating_mul(2)).min(max_backoff);
                }
                Ok(response) => {
                    return Err(anyhow!(
                        "HTTP error {} fetching block timestamps {:?}",
                        response.status(),
                        blocks
                    ));
                }
                Err(_) if attempts <= max_retries => {
                    drop(permit);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff.saturating_mul(2)).min(max_backoff);
                }
                Err(error) => {
                    return Err(anyhow!(
                        "request failed fetching block timestamps {:?}: {}",
                        blocks,
                        error
                    ));
                }
            }
        }
    }
}

fn parse_quantity(value: &str) -> Result<u64> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    u64::from_str_radix(value, 16).map_err(Into::into)
}

fn is_rate_limited(err: &JsonRpcError) -> bool {
    err.code == 429 || err.message.to_ascii_lowercase().contains("compute units")
}

pub fn decode_trace_block_value(
    block: u64,
    value: serde_json::Value,
) -> Result<Vec<LocalizedTransactionTrace>> {
    let value = filter_reward_traces(value);
    serde_json::from_value(value)
        .map_err(|err| anyhow!("decode error for block {}: {}", block, err))
}

fn filter_reward_traces(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .filter(|item| !is_reward_trace(item))
                .collect(),
        ),
        other => other,
    }
}

fn is_reward_trace(item: &serde_json::Value) -> bool {
    item.get("type").and_then(|value| value.as_str()) == Some("reward")
        || item
            .get("action")
            .and_then(|action| action.get("rewardType"))
            .is_some()
}

//! Batched JSON-RPC client for `trace_block`.
//!
//! Groups multiple block traces into a single HTTP request, with semaphore-
//! controlled concurrency, exponential backoff, and rate-limit detection.
//! Used by both the bulk extractor and the dashboard's tail loop.

use std::{sync::Arc, time::Duration};

use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Serialize)]
struct JsonRpcRequest {
    jsonrpc: &'static str,
    method: &'static str,
    params: [serde_json::Value; 1],
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
                params: [serde_json::Value::String(format!("0x{:x}", block))],
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
}

fn is_rate_limited(err: &JsonRpcError) -> bool {
    err.code == 429 || err.message.to_ascii_lowercase().contains("compute units")
}

fn decode_trace_block_value(
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

#[cfg(test)]
mod tests {
    use super::decode_trace_block_value;

    #[test]
    fn trace_decode_ignores_gnosis_external_reward_traces() {
        let value = serde_json::json!([
            {
                "action": {
                    "author": "0x0000000000000000000000000000000000000000",
                    "rewardType": "external",
                    "value": "0x0"
                },
                "blockHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "blockNumber": 46630628,
                "result": null,
                "subtraces": 0,
                "traceAddress": [],
                "transactionHash": null,
                "transactionPosition": null,
                "type": "reward"
            }
        ]);

        let traces = decode_trace_block_value(46630628, value).unwrap();
        assert!(traces.is_empty());
    }
}

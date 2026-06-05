//! Thin Etherscan v2 API client.
//!
//! Currently exposes a single endpoint, `getsourcecode`, which the
//! metadata sync loop uses to populate verification status and contract name.
//! Compiler version, language, standards, and proxy signals are intentionally
//! decoded locally from bytecode. Surfaces rate-limit errors so the caller can
//! back off; treats empty-source responses as unverified rather than as
//! failures.

use std::{fmt, time::Duration};

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;

use crate::util::truncate_chars;

/// Categorized failure surface so callers can distinguish fatal auth errors
/// (where retrying makes things worse) from transient rate-limit / network
/// errors (where backoff helps).
#[derive(Debug)]
pub enum EtherscanFail {
    InvalidApiKey,
    RateLimited,
    Other(anyhow::Error),
}

impl fmt::Display for EtherscanFail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidApiKey => write!(f, "etherscan: invalid api key"),
            Self::RateLimited => write!(f, "etherscan: rate limited"),
            Self::Other(e) => write!(f, "{:#}", e),
        }
    }
}

impl std::error::Error for EtherscanFail {}

impl From<anyhow::Error> for EtherscanFail {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}

#[derive(Debug, Clone)]
pub struct EtherscanClient {
    http: Client,
    base_url: String,
    api_key: String,
    chain_id: u64,
}

/// Etherscan only adds two things we can't derive from bytecode:
/// **verification status** and **human-readable contract name**.
/// Compiler version, language, and EIP-1967 proxy status all come from local
/// decoding (see [`crate::decode`]).
#[derive(Debug, Clone)]
pub struct VerificationResult {
    pub is_verified: bool,
    pub contract_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiEnvelope {
    status: String,
    message: String,
    result: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct SourceCodeRow {
    #[serde(rename = "SourceCode")]
    source_code: String,
    #[serde(rename = "ContractName", default)]
    contract_name: String,
}

impl EtherscanClient {
    pub fn new(base_url: String, api_key: String, chain_id: u64) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .gzip(true)
            .build()
            .context("build etherscan http client")?;
        Ok(Self {
            http,
            base_url,
            api_key,
            chain_id,
        })
    }

    pub async fn get_source_code(
        &self,
        address_hex: &str,
    ) -> std::result::Result<VerificationResult, EtherscanFail> {
        let resp = self
            .http
            .get(&self.base_url)
            .query(&[
                ("chainid", self.chain_id.to_string().as_str()),
                ("module", "contract"),
                ("action", "getsourcecode"),
                ("address", address_hex),
                ("apikey", self.api_key.as_str()),
            ])
            .send()
            .await
            .context("etherscan request failed")?;

        let status = resp.status();
        let body = resp.text().await.context("read etherscan body")?;
        if !status.is_success() {
            return Err(EtherscanFail::Other(anyhow!(
                "etherscan http {}: {}",
                status,
                truncate_chars(&body, 200)
            )));
        }

        let envelope: ApiEnvelope = serde_json::from_str(&body)
            .with_context(|| format!("decode etherscan body: {}", truncate_chars(&body, 200)))?;

        if envelope.status != "1" {
            let result_text = envelope.result.as_str().unwrap_or("").to_string();
            let combined = format!("{} {}", envelope.message, result_text).to_lowercase();
            if combined.contains("invalid api key")
                || combined.contains("invalid apikey")
                || combined.contains("too many invalid api key")
            {
                return Err(EtherscanFail::InvalidApiKey);
            }
            if combined.contains("rate limit")
                || combined.contains("max calls per sec")
                || combined.contains("max rate limit reached")
            {
                return Err(EtherscanFail::RateLimited);
            }
            // status "0" with no specific failure marker => treat as unverified.
            return Ok(VerificationResult {
                is_verified: false,
                contract_name: None,
            });
        }

        let rows: Vec<SourceCodeRow> =
            serde_json::from_value(envelope.result).context("decode etherscan result rows")?;
        let row = match rows.into_iter().next() {
            Some(row) => row,
            None => {
                return Ok(VerificationResult {
                    is_verified: false,
                    contract_name: None,
                })
            }
        };

        if row.source_code.trim().is_empty() {
            return Ok(VerificationResult {
                is_verified: false,
                contract_name: None,
            });
        }

        Ok(VerificationResult {
            is_verified: true,
            contract_name: empty_to_none(row.contract_name),
        })
    }
}

fn empty_to_none(s: String) -> Option<String> {
    if s.trim().is_empty() {
        None
    } else {
        Some(s)
    }
}

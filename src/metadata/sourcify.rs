//! Sourcify API v2 client.
//!
//! For each address we make a single GET to
//! `/v2/contract/{chainId}/{address}?fields=compilation`:
//! - 200 → verified, response contains the match quality and compilation info.
//! - 404 → not verified on Sourcify (the address may still be verified on
//!   Etherscan, so the caller should fall back).
//! - other → transient error; caller can retry or fall back.

use std::{fmt, time::Duration};

use anyhow::{anyhow, Context, Result};
use reqwest::{Client, StatusCode};
use serde::Deserialize;

use crate::util::truncate_chars;

/// Categorized failure surface so the metadata sync loop can decide whether to
/// fall back to Etherscan, retry, or abort.
#[derive(Debug)]
pub enum SourcifyFail {
    /// 404 — contract is not verified on Sourcify. Try Etherscan instead.
    NotFound,
    /// Throttled by the public instance; caller should back off.
    RateLimited,
    /// Network / decode / unknown HTTP error.
    Other(anyhow::Error),
}

impl fmt::Display for SourcifyFail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "sourcify: not verified"),
            Self::RateLimited => write!(f, "sourcify: rate limited"),
            Self::Other(e) => write!(f, "sourcify: {:#}", e),
        }
    }
}

impl std::error::Error for SourcifyFail {}

impl From<anyhow::Error> for SourcifyFail {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}

#[derive(Debug, Clone)]
pub struct SourcifyResult {
    /// "exact_match" (metadata hash matches exactly) or "match" (bytecode
    /// matches but metadata differs). Both count as verified.
    pub match_type: Option<String>,
    pub contract_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SourcifyClient {
    http: Client,
    base_url: String,
    chain_id: u64,
}

#[derive(Debug, Deserialize)]
struct ContractLookupResponse {
    #[serde(default, rename = "match")]
    match_status: Option<String>,
    #[serde(default, rename = "runtimeMatch")]
    runtime_match: Option<String>,
    #[serde(default, rename = "creationMatch")]
    creation_match: Option<String>,
    #[serde(default)]
    compilation: Option<CompilationInfo>,
}

#[derive(Debug, Deserialize)]
struct CompilationInfo {
    #[serde(default)]
    name: Option<String>,
}

impl SourcifyClient {
    pub fn new(base_url: String, chain_id: u64) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(20))
            .gzip(true)
            .user_agent("blink/0.1 (+https://github.com/g4titanx/blink)")
            .build()
            .context("build sourcify http client")?;
        Ok(Self {
            http,
            base_url,
            chain_id,
        })
    }

    /// One call per address. Returns `Ok(SourcifyResult)` if verified,
    /// `Err(SourcifyFail::NotFound)` if Sourcify has nothing for this address,
    /// other variants for transient failures.
    pub async fn lookup(
        &self,
        address_hex: &str,
    ) -> std::result::Result<SourcifyResult, SourcifyFail> {
        let base = self.base_url.trim_end_matches('/');
        let url = format!("{base}/v2/contract/{}/{}", self.chain_id, address_hex);
        let resp = self
            .http
            .get(&url)
            .query(&[("fields", "compilation")])
            .send()
            .await
            .with_context(|| format!("sourcify GET {}", url))
            .map_err(SourcifyFail::from)?;

        match resp.status() {
            StatusCode::OK => {}
            StatusCode::NOT_FOUND => return Err(SourcifyFail::NotFound),
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
                return Err(SourcifyFail::RateLimited);
            }
            status => {
                let body = resp.text().await.unwrap_or_default();
                return Err(SourcifyFail::Other(anyhow!(
                    "sourcify HTTP {}: {}",
                    status,
                    truncate_chars(&body, 200)
                )));
            }
        }

        let body = resp
            .text()
            .await
            .context("read sourcify body")
            .map_err(SourcifyFail::from)?;
        let parsed: ContractLookupResponse = serde_json::from_str(&body)
            .with_context(|| {
                format!(
                    "decode sourcify contract response: {}",
                    truncate_chars(&body, 200)
                )
            })
            .map_err(SourcifyFail::from)?;
        let match_type = parsed
            .runtime_match
            .or(parsed.match_status)
            .or(parsed.creation_match);
        if match_type.is_none() {
            return Err(SourcifyFail::NotFound);
        }
        let compilation = parsed.compilation;
        let contract_name = compilation.as_ref().and_then(|c| c.name.clone());

        Ok(SourcifyResult {
            match_type,
            contract_name,
        })
    }
}

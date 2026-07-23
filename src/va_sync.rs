//! Verifier Alliance parquet synchronization used by the serve background job.

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use tokio::process::Command;

pub async fn sync_verifier_alliance_files(root: &Path) -> Result<()> {
    tokio::fs::create_dir_all(root.join("contract_deployments"))
        .await
        .with_context(|| format!("create Verifier Alliance dir {}", root.display()))?;
    tokio::fs::create_dir_all(root.join("verified_contracts"))
        .await
        .with_context(|| format!("create Verifier Alliance dir {}", root.display()))?;

    sync_prefix(
        "s3://verifier-alliance-parquet-export/v2/contract_deployments/",
        &root.join("contract_deployments"),
    )
    .await?;
    sync_prefix(
        "s3://verifier-alliance-parquet-export/v2/verified_contracts/",
        &root.join("verified_contracts"),
    )
    .await
}

async fn sync_prefix(source: &str, destination: &Path) -> Result<()> {
    let mut command = Command::new("aws");
    command.kill_on_drop(true).args([
        "s3",
        "sync",
        source,
        destination
            .to_str()
            .ok_or_else(|| anyhow!("non-UTF-8 VA path {}", destination.display()))?,
        "--endpoint-url",
        "https://storage.googleapis.com",
        "--no-sign-request",
        "--only-show-errors",
    ]);
    let output = tokio::time::timeout(std::time::Duration::from_secs(30 * 60), command.output())
        .await
        .context("aws s3 sync timed out after 30 minutes")??;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let details = if stderr.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };
    Err(anyhow!(
        "aws s3 sync failed for {source} ({}): {}",
        output.status,
        details
    ))
}

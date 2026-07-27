//! Persistent block-time checkpoints and the `blink checkpoints` bootstrap.

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    time::Duration,
};

use alloy::providers::{Provider, ProviderBuilder};
use anyhow::{anyhow, Context, Result};
use duckdb::{params, Connection};

use crate::{
    blocks::{self, ChainCheckpoints},
    cli::CheckpointsArgs,
    extract::batch::BatchClient,
    util::{format_count, print_header, print_kv, print_kv_accent},
};

pub(crate) fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS chain_block_checkpoints (
            chain_id UBIGINT NOT NULL,
            block_number UBIGINT NOT NULL,
            block_timestamp BIGINT NOT NULL,
            source VARCHAR NOT NULL,
            observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chain_id, block_number)
        );
        "#,
    )
    .context("create block checkpoint schema")
}

pub(crate) fn load(conn: &Connection) -> Result<ChainCheckpoints> {
    if !crate::db::table_exists(conn, "chain_block_checkpoints")? {
        return Ok(HashMap::new());
    }
    let mut stmt = conn.prepare(
        r#"
        SELECT chain_id, block_number, block_timestamp
        FROM chain_block_checkpoints
        ORDER BY chain_id, block_number
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, u64>(0)?,
            row.get::<_, u64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let mut checkpoints: ChainCheckpoints = HashMap::new();
    for row in rows {
        let (chain_id, block_number, timestamp) = row?;
        checkpoints
            .entry(chain_id)
            .or_default()
            .push((block_number, timestamp));
    }
    Ok(checkpoints)
}

pub(crate) fn load_runtime(conn: &Connection) -> Result<usize> {
    let checkpoints = load(conn)?;
    let count = checkpoints.values().map(Vec::len).sum();
    blocks::replace_runtime_checkpoints(checkpoints);
    Ok(count)
}

pub(crate) fn upsert(
    conn: &Connection,
    chain_id: u64,
    block_number: u64,
    timestamp: i64,
) -> Result<()> {
    conn.execute(
        r#"
        INSERT INTO chain_block_checkpoints (
            chain_id, block_number, block_timestamp, source, observed_at
        ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (chain_id, block_number) DO UPDATE SET
            block_timestamp = excluded.block_timestamp,
            source = excluded.source,
            observed_at = excluded.observed_at
        "#,
        params![chain_id, block_number, timestamp, "rpc"],
    )?;
    Ok(())
}

fn persist_batch(db_path: &Path, chain_id: u64, checkpoints: &[(u64, i64)]) -> Result<()> {
    let conn =
        Connection::open(db_path).with_context(|| format!("open duckdb {}", db_path.display()))?;
    ensure_schema(&conn)?;
    conn.execute_batch("BEGIN")?;
    let result = checkpoints
        .iter()
        .try_for_each(|(block, timestamp)| upsert(&conn, chain_id, *block, *timestamp));
    match result {
        Ok(()) => conn.execute_batch("COMMIT").map_err(Into::into),
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(error).context("persist block checkpoint batch")
        }
    }
}

fn indexed_chain_ranges(conn: &Connection) -> Result<HashMap<u64, (u64, u64)>> {
    if !crate::db::table_exists(conn, "rollup_block_counts")? {
        return Ok(HashMap::new());
    }
    let mut stmt = conn.prepare(
        r#"
        SELECT chain_id, MIN(block_number), MAX(block_number)
        FROM rollup_block_counts
        GROUP BY chain_id
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, u64>(0)?,
            u64::from(row.get::<_, u32>(1)?),
            u64::from(row.get::<_, u32>(2)?),
        ))
    })?;
    rows.map(|row| row.map(|(chain, start, end)| (chain, (start, end))))
        .collect::<Result<HashMap<_, _>, _>>()
        .context("read indexed chain ranges")
}

fn existing_checkpoint_blocks(conn: &Connection) -> Result<HashMap<u64, HashSet<u64>>> {
    let mut existing: HashMap<u64, HashSet<u64>> = HashMap::new();
    for (chain_id, checkpoints) in load(conn)? {
        existing
            .entry(chain_id)
            .or_default()
            .extend(checkpoints.into_iter().map(|(block, _)| block));
    }
    Ok(existing)
}

fn sample_blocks(start: u64, end: u64, interval: u64) -> Vec<u64> {
    if start > end {
        return Vec::new();
    }
    let interval = interval.max(1);
    let mut blocks = vec![start];
    let mut next = start
        .checked_div(interval)
        .and_then(|bucket| bucket.checked_add(1))
        .and_then(|bucket| bucket.checked_mul(interval))
        .unwrap_or(end);
    while next < end {
        blocks.push(next);
        next = match next.checked_add(interval) {
            Some(value) => value,
            None => break,
        };
    }
    if blocks.last().copied() != Some(end) {
        blocks.push(end);
    }
    blocks
}

pub async fn run_checkpoints(args: CheckpointsArgs) -> Result<()> {
    let rpcs = args
        .rpc
        .into_iter()
        .map(|rpc| rpc.trim().to_string())
        .filter(|rpc| !rpc.is_empty())
        .collect::<Vec<_>>();
    if rpcs.is_empty() {
        return Err(anyhow!(
            "no RPC URLs configured; set BLINK_SERVE_RPCS or pass --rpc"
        ));
    }
    if args.batch_size == 0 {
        return Err(anyhow!("--batch-size must be greater than zero"));
    }

    fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("create data dir {}", args.data_dir.display()))?;
    let db_path = args.data_dir.join("blink.duckdb");
    let (ranges, existing) = {
        let conn = Connection::open(&db_path)
            .with_context(|| format!("open duckdb {}", db_path.display()))?;
        ensure_schema(&conn)?;
        (
            indexed_chain_ranges(&conn)?,
            existing_checkpoint_blocks(&conn)?,
        )
    };

    print_header("blink checkpoints");
    print_kv("data dir", &args.data_dir.display().to_string());
    print_kv_accent("interval", &format_count(args.interval_blocks.max(1)));

    let mut stored = 0usize;
    for rpc in rpcs {
        let provider = ProviderBuilder::new().connect_http(
            rpc.parse()
                .with_context(|| format!("invalid rpc url {rpc}"))?,
        );
        let chain_id = provider
            .get_chain_id()
            .await
            .with_context(|| format!("fetch chain id from {rpc}"))?;
        let head = provider
            .get_block_number()
            .await
            .with_context(|| format!("fetch head block for chain {chain_id}"))?;
        let start = ranges.get(&chain_id).map(|range| range.0).unwrap_or(0);
        let known = existing.get(&chain_id);
        let blocks = sample_blocks(start, head, args.interval_blocks)
            .into_iter()
            .filter(|block| !known.is_some_and(|known| known.contains(block)))
            .collect::<Vec<_>>();

        print_kv(
            &format!("chain {chain_id}"),
            &format!(
                "{} → {} · {} missing checkpoint(s)",
                format_count(start),
                format_count(head),
                format_count(blocks.len() as u64)
            ),
        );
        if blocks.is_empty() {
            continue;
        }

        let client = BatchClient::new(rpc, 1)?;
        for batch in blocks.chunks(args.batch_size) {
            let rows = client
                .block_timestamps_batch(
                    batch,
                    5,
                    Duration::from_millis(500),
                    Duration::from_secs(15),
                )
                .await?;
            persist_batch(&db_path, chain_id, &rows)?;
            stored += rows.len();
        }
    }

    print_kv_accent("stored", &format_count(stored as u64));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::sample_blocks;

    #[test]
    fn samples_indexed_start_interval_boundaries_and_head() {
        assert_eq!(
            sample_blocks(47_205, 250_001, 100_000),
            vec![47_205, 100_000, 200_000, 250_001]
        );
    }
}

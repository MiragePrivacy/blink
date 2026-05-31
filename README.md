# blink

Fast contract bytecode indexer for Ethereum. Extracts contract creation data to Parquet files.

## Installation

```bash
cargo install --path .
```

Or build from source:

```bash
cargo build --release
./target/release/blink --help
```

## Usage

```bash
# Basic usage (uses BLINK_CONTRACTS_RPC environment variable)
blink contracts --start-block 0 --end-block 1000000

# With explicit RPC URL
blink contracts --rpc https://eth.llamarpc.com --start-block 19000000

# Custom concurrency settings for faster extraction
blink contracts --start-block 0 \
    --batch-size 100 \
    --max-concurrent-requests 64 \
    --max-concurrent-chunks 8

# Alchemy-friendly throttled settings
blink contracts --start-block 16800000 --end-block 17799999 \
    --batch-size 5 \
    --max-concurrent-requests 4 \
    --max-concurrent-chunks 1 \
    --max-retries 20 \
    --initial-backoff-ms 2000 \
    --max-backoff-ms 10000

# Resume from where you left off (skips existing chunks)
blink contracts --start-block 0 --end-block 20000000

# Force overwrite existing chunks
blink contracts --start-block 0 --overwrite
```

## Options

```
blink contracts [OPTIONS] --start-block <START_BLOCK>

Options:
      --rpc <RPC>                          RPC URL (env: BLINK_CONTRACTS_RPC)
      --start-block <START_BLOCK>          Start block (inclusive)
      --end-block <END_BLOCK>              End block or "latest" [default: latest]
      --chunk-size <CHUNK_SIZE>            Blocks per output file [default: 100000]
      --batch-size <BATCH_SIZE>            Blocks per JSON-RPC request [default: 50]
      --max-concurrent-requests <N>        Concurrent HTTP requests [default: 32]
      --max-concurrent-chunks <N>          Concurrent chunk processing [default: 4]
      --output-dir <OUTPUT_DIR>            Output directory [default: ./data/blink]
      --overwrite                          Overwrite existing chunk files
      --max-retries <MAX_RETRIES>          Max retries per batch [default: 5]
      --initial-backoff-ms <MS>            Initial retry backoff [default: 1000]
      --max-backoff-ms <MS>                Max retry backoff [default: 30000]
      --fast                               Use aggressive defaults
```

## Output Schema

Each parquet file contains contract creations with this schema:

| Column | Type | Description |
|--------|------|-------------|
| block_number | uint32 | Block where contract was created |
| block_hash | binary | Block hash |
| create_index | uint32 | Index of creation within block |
| transaction_hash | binary | Transaction hash (nullable for genesis) |
| contract_address | binary | Deployed contract address |
| deployer | binary | EOA that initiated the transaction |
| factory | binary | Address that called CREATE/CREATE2 |
| init_code | binary | Constructor bytecode |
| code | binary | Deployed runtime bytecode |
| init_code_hash | binary | Keccak256 of init_code |
| n_init_code_bytes | uint32 | Length of init_code |
| n_code_bytes | uint32 | Length of deployed code |
| code_hash | binary | Keccak256 of deployed code |
| chain_id | uint64 | Chain ID |

## Performance

blink is designed for speed:

- **Batch requests**: Groups multiple `trace_block` calls into single HTTP requests
- **Parallel chunks**: Processes multiple 100k block ranges simultaneously
- **Semaphore-controlled concurrency**: Prevents RPC overload while maximizing throughput

Typical performance with a good RPC endpoint:
- ~500-2000 blocks/sec depending on RPC and block density

On rate-limited RPCs (e.g., Alchemy), lower concurrency and batch sizes to avoid 429s.
Backoff is exponential with a cap (see --max-backoff-ms).

## Reports

Each run writes a report JSON to `./data/blink/.blink/reports/` with status, error (if any),
and per-chunk metadata including output file paths and sizes.

## Requirements

- Ethereum node with `trace_block` support (Erigon, Reth, or tracing-enabled Geth)
- Rust 1.70+

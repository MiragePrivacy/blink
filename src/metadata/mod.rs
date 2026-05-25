//! Verification metadata clients and sync loop.

pub mod etherscan;
pub mod sourcify;
pub mod sync;

pub use etherscan::{EtherscanClient, VerificationResult};
pub use sourcify::SourcifyClient;
pub use sync::{
    metadata_sync_loop, resolve_api_key, run_metadata_sync, MetadataSyncOptions,
    VerificationSources,
};

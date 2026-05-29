//! Supported chain metadata for dashboard filtering.

use serde::Serialize;
use utoipa::ToSchema;

pub const ETHEREUM_CHAIN_ID: u64 = 1;
pub const GNOSIS_CHAIN_ID: u64 = 100;

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
pub struct ChainInfo {
    pub chain_id: u64,
    pub name: &'static str,
    pub short_name: &'static str,
    pub native_symbol: &'static str,
    pub explorer_url: &'static str,
    pub icon_key: &'static str,
}

pub const SUPPORTED_CHAINS: &[ChainInfo] = &[
    ChainInfo {
        chain_id: ETHEREUM_CHAIN_ID,
        name: "Ethereum",
        short_name: "ETH",
        native_symbol: "ETH",
        explorer_url: "https://etherscan.io",
        icon_key: "ethereum",
    },
    ChainInfo {
        chain_id: GNOSIS_CHAIN_ID,
        name: "Gnosis Chain",
        short_name: "GNOSIS",
        native_symbol: "xDAI",
        explorer_url: "https://gnosisscan.io",
        icon_key: "gnosis",
    },
];

pub fn default_chain_id() -> u64 {
    ETHEREUM_CHAIN_ID
}

pub fn supported_chains() -> &'static [ChainInfo] {
    SUPPORTED_CHAINS
}

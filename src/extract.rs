use alloy::{
    primitives::{keccak256, Address},
    rpc::types::trace::parity::{Action, LocalizedTransactionTrace, TraceOutput},
};
use anyhow::{anyhow, Result};

use crate::types::ContractRow;

pub fn extract_contracts(
    traces: &[LocalizedTransactionTrace],
    chain_id: u64,
) -> Result<Vec<ContractRow>> {
    let mut rows = Vec::new();
    let mut deployer = Address::ZERO;
    let mut create_index = 0u32;

    for trace in traces {
        if trace.trace.trace_address.is_empty() {
            deployer = match &trace.trace.action {
                Action::Call(call) => call.from,
                Action::Create(create) => create.from,
                Action::Selfdestruct(suicide) => suicide.refund_address,
                Action::Reward(reward) => reward.author,
            };
        }

        if let (Action::Create(create), Some(TraceOutput::Create(result))) =
            (&trace.trace.action, &trace.trace.result)
        {
            let block_number = trace
                .block_number
                .ok_or_else(|| anyhow!("missing block_number in trace"))?;
            if block_number > u64::from(u32::MAX) {
                return Err(anyhow!("block number {} overflows u32", block_number));
            }
            let block_hash = trace
                .block_hash
                .ok_or_else(|| anyhow!("missing block_hash in trace"))?;
            let tx_hash = trace.transaction_hash.map(|hash| hash.to_vec());

            rows.push(ContractRow {
                block_number: block_number as u32,
                block_hash: block_hash.to_vec(),
                create_index,
                transaction_hash: tx_hash,
                contract_address: result.address.to_vec(),
                deployer: deployer.to_vec(),
                factory: create.from.to_vec(),
                init_code: create.init.to_vec(),
                code: result.code.to_vec(),
                init_code_hash: keccak256(&create.init).to_vec(),
                n_init_code_bytes: create.init.len() as u32,
                n_code_bytes: result.code.len() as u32,
                code_hash: keccak256(&result.code).to_vec(),
                chain_id,
            });
            create_index = create_index.saturating_add(1);
        }
    }
    Ok(rows)
}

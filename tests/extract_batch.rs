//! Trace batch decoding tests.

use blink::extract::batch::decode_trace_block_value;

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

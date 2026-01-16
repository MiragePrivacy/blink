use std::{fs::File, path::Path, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, BinaryBuilder, UInt32Builder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;

use crate::types::ContractRow;

pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt32, false),
        Field::new("block_hash", DataType::Binary, false),
        Field::new("create_index", DataType::UInt32, false),
        Field::new("transaction_hash", DataType::Binary, true),
        Field::new("contract_address", DataType::Binary, false),
        Field::new("deployer", DataType::Binary, false),
        Field::new("factory", DataType::Binary, false),
        Field::new("init_code", DataType::Binary, false),
        Field::new("code", DataType::Binary, false),
        Field::new("init_code_hash", DataType::Binary, false),
        Field::new("n_init_code_bytes", DataType::UInt32, false),
        Field::new("n_code_bytes", DataType::UInt32, false),
        Field::new("code_hash", DataType::Binary, false),
        Field::new("chain_id", DataType::UInt64, false),
    ]))
}

pub fn create_writer(path: &Path, schema: Arc<Schema>) -> Result<ArrowWriter<File>> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    Ok(ArrowWriter::try_new(file, schema, None)?)
}

pub fn rows_to_batch(rows: &[ContractRow], schema: Arc<Schema>) -> Result<RecordBatch> {
    let mut block_number = UInt32Builder::with_capacity(rows.len());
    let mut block_hash = BinaryBuilder::new();
    let mut create_index = UInt32Builder::with_capacity(rows.len());
    let mut transaction_hash = BinaryBuilder::new();
    let mut contract_address = BinaryBuilder::new();
    let mut deployer = BinaryBuilder::new();
    let mut factory = BinaryBuilder::new();
    let mut init_code = BinaryBuilder::new();
    let mut code = BinaryBuilder::new();
    let mut init_code_hash = BinaryBuilder::new();
    let mut n_init_code_bytes = UInt32Builder::with_capacity(rows.len());
    let mut n_code_bytes = UInt32Builder::with_capacity(rows.len());
    let mut code_hash = BinaryBuilder::new();
    let mut chain_id_col = UInt64Builder::with_capacity(rows.len());

    for row in rows {
        block_number.append_value(row.block_number);
        block_hash.append_value(&row.block_hash);
        create_index.append_value(row.create_index);
        match &row.transaction_hash {
            Some(hash) => transaction_hash.append_value(hash),
            None => transaction_hash.append_null(),
        }
        contract_address.append_value(&row.contract_address);
        deployer.append_value(&row.deployer);
        factory.append_value(&row.factory);
        init_code.append_value(&row.init_code);
        code.append_value(&row.code);
        init_code_hash.append_value(&row.init_code_hash);
        n_init_code_bytes.append_value(row.n_init_code_bytes);
        n_code_bytes.append_value(row.n_code_bytes);
        code_hash.append_value(&row.code_hash);
        chain_id_col.append_value(row.chain_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(block_number.finish()),
        Arc::new(block_hash.finish()),
        Arc::new(create_index.finish()),
        Arc::new(transaction_hash.finish()),
        Arc::new(contract_address.finish()),
        Arc::new(deployer.finish()),
        Arc::new(factory.finish()),
        Arc::new(init_code.finish()),
        Arc::new(code.finish()),
        Arc::new(init_code_hash.finish()),
        Arc::new(n_init_code_bytes.finish()),
        Arc::new(n_code_bytes.finish()),
        Arc::new(code_hash.finish()),
        Arc::new(chain_id_col.finish()),
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

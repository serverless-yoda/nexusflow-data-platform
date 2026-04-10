-- src/sql/ddl/create_bronze_tx.sql
-- 1. Create the Schema using variables
CREATE SCHEMA IF NOT EXISTS ${catalog}.${bronze_schema}
MANAGED LOCATION '${storage_root}/${bronze_path}';

-- 2. Define the Table (No LOCATION needed anymore)
CREATE TABLE IF NOT EXISTS ${catalog}.${bronze_schema}.transactions (
    tx_id STRING,
    customer_id STRING,
    amount DOUBLE,
    currency STRING,
    tx_time TIMESTAMP,
    region STRING,
    _rescued_data STRING,
    _ingestion_timestamp TIMESTAMP DEFAULT current_timestamp(),
    _source_file STRING,
    _batch_id BIGINT
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'bronze'
);
-- src/sql/ddl/create_bronze_txt.sql

-- 1. Create the Schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS nff_catalog.bronze
COMMENT 'Landing area for raw, immutable fintech transactions';

-- 2. Define the Bronze Transactions Table
CREATE EXTERNAL TABLE IF NOT EXISTS nff_catalog.bronze.transactions (
    -- Primary Data Fields (Inferred or Explicit)
    tx_id STRING,
    customer_id STRING,
    amount DOUBLE,
    currency STRING,
    tx_time TIMESTAMP,
    region STRING,
    
    -- Metadata Fields for Traceability (Lead Architect Standard)
    _rescued_data STRING COMMENT 'Captures malformed JSON data',
    _ingestion_timestamp TIMESTAMP DEFAULT current_timestamp(),
    _source_file STRING COMMENT 'Path to the raw file in ADLS',
    _batch_id BIGINT COMMENT 'Unique ID for the ingestion job run'
)
USING DELTA
LOCATION 'abfss://bronze@nexusstorage.dfs.core.windows.net/transactions/'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true', -- Enables downstream incremental processing
    'quality' = 'bronze'
);
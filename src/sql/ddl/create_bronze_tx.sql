-- sql/ddl/create_bronze_tx.sql

/*
  NEXUSFLOW BRONZE LAYER: RAW TRANSACTIONS
  Contract: Append-only, schema-relaxed, contains audit metadata.
*/

CREATE TABLE IF NOT EXISTS nff_catalog.bronze.transactions (
    -- 1. Metadata for Traceability
    _ingestion_timestamp TIMESTAMP,
    _source_file_path STRING,
    
    -- 2. The 'Safety Net' (Crucial for Lead-Level Ingestion)
    -- Any data that doesn't match the schema below is stored here as JSON
    _rescued_data STRING,

    -- 3. Raw Business Fields (Inferred or Explicit)
    tx_id STRING,
    customer_id STRING,
    event_type STRING,
    amount DOUBLE,
    currency STRING,
    tx_time TIMESTAMP,
    metadata JSON  -- Flexible field for varying upstream attributes
)
USING DELTA
-- Optimized for high-frequency writes
TBLPROPERTIES (
    'delta.appendOnly' = 'true',
    'delta.enableDeletionVectors' = 'false', -- Not needed for append-only Bronze
    'delta.autoOptimize.optimizeWrite' = 'true'
);
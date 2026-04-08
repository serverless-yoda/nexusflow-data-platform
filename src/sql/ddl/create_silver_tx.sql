-- src/sql/ddl/create_silver_txt.sql

-- 1. Create the Schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS nff_catalog.silver
COMMENT 'Refined and cleaned fintech transactions for regional analysis';

-- 2. Define the Silver Transactions Table
CREATE TABLE IF NOT EXISTS nff_catalog.silver.transactions (
    -- Business Keys
    tx_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    
    -- Cleaned & Typed Measures
    amount DOUBLE COMMENT 'Cleaned transaction amount in NZD',
    currency STRING,
    tx_time TIMESTAMP,
    tx_date DATE GENERATED ALWAYS AS (CAST(tx_time AS DATE)),
    
    -- Governance Attributes
    region STRING COMMENT 'Standardized NZ Region (e.g., AUCKLAND, WELLINGTON)',
    merchant_name STRING,
    
    -- Audit Metadata
    _bronze_source_file STRING,
    _last_modified_time TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
-- 2026 Standard: Cluster by columns frequently used in WHERE clauses
CLUSTER BY (region, tx_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7'
);
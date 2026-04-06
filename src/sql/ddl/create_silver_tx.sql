-- sql/ddl/create_silver_tx.sql
CREATE TABLE IF NOT EXISTS nff_catalog.silver.transactions (
    tx_id STRING,
    customer_id STRING,
    tx_amount DOUBLE,
    tx_timestamp TIMESTAMP,
    region STRING
)
USING DELTA
-- Phase 3 Tech: Multidimensional indexing without folder hierarchies
CLUSTER BY (customer_id, tx_timestamp);

-- Enable Deletion Vectors for rapid SCD Type 2 updates later
ALTER TABLE nff_catalog.silver.transactions 
SET TBLPROPERTIES (delta.enableDeletionVectors = true);
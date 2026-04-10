-- src/sql/ddl/create_silver_tx.sql

-- Use placeholders for Catalog, Schema, and Location
CREATE SCHEMA IF NOT EXISTS ${catalog}.${silver_schema}
MANAGED LOCATION '${storage_root}/${silver_path}';

CREATE TABLE IF NOT EXISTS ${catalog}.${silver_schema}.transactions (
    tx_id STRING NOT NULL,
    amount DOUBLE,
    region STRING,
    tx_time TIMESTAMP,
    tx_date DATE GENERATED ALWAYS AS (CAST(tx_time AS DATE))
)
USING DELTA
CLUSTER BY (region, tx_date);
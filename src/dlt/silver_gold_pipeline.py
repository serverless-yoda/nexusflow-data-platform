# src/dlt/silver_gold_pipeline.py
import dlt
from pyspark.sql.functions import col, upper
from src.common.governance import GovernanceManager

# 1. SILVER LAYER: Cleaning & Quality
@dlt.table(
    name="silver_transactions",
    comment="Cleaned transactions with regional standardization."
)
@dlt.expect_or_drop("valid_amount", "amount > 0") # Phase 6: Data Quality
@dlt.expect_or_fail("valid_id", "tx_id IS NOT NULL")
def silver_transactions():
    return (
        dlt.read("bronze_transactions")
        .filter(col("_rescued_data").isNull()) # Only process non-corrupt data
        .select(
            "tx_id",
            "customer_id",
            col("amount").cast("double"),
            upper(col("region")).alias("region"), # Standardize for RLS
            "tx_time"
        )
    )

# 2. GOLD LAYER: Business Aggregates
@dlt.table(
    name="gold_regional_summary",
    comment="High-level financial KPIs per region for NZ dashboards."
)
def gold_regional_summary():
    return (
        dlt.read("silver_transactions")
        .groupBy("region")
        .agg({"amount": "sum", "tx_id": "count"})
        .withColumnRenamed("sum(amount)", "total_revenue")
        .withColumnRenamed("count(tx_id)", "transaction_volume")
    )
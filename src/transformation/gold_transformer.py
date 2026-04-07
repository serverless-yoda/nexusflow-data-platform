# src/transformation/gold_transformer.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class GoldTransformer:
    """
    Refines Silver data into Business-Ready Gold Fact and Dimension tables.
    Implements Point-in-Time joins for SCD Type 2 compliance.
    """

    def __init__(self, spark: SparkSession, catalog: str = "nff_catalog"):
        self.spark = spark
        self.catalog = catalog

    def create_fct_transactions(self):
        """
        Creates the Gold Fact table by joining transactions with 
        the correct version of the customer dimension at the time of TX.
        """
        df_tx = self.spark.table(f"{self.catalog}.silver.transactions")
        df_cust = self.spark.table(f"{self.catalog}.gold.dim_customers")

        # The 'Lead' Secret: Point-in-Time Join
        # We ensure the transaction joins to the customer record that was 
        # active between the record's __start_at and __end_at timestamps.
        fct_df = df_tx.alias("t").join(
            df_cust.alias("c"),
            (F.col("t.customer_id") == F.col("c.customer_id")) &
            (F.col("t.tx_timestamp") >= F.col("c.__start_at")) &
            (F.col("t.tx_timestamp") < F.functions.coalesce(F.col("c.__end_at"), F.lit("9999-12-31")))
        ).select(
            "t.tx_id",
            "t.tx_timestamp",
            "t.tx_amount",
            "t.region",
            "c.customer_tier",        # Captured as it was at time of purchase
            "c.customer_risk_rating"   # Crucial for NZ compliance auditing
        )

        fct_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.fct_transactions")
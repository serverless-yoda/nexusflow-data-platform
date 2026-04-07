# src/transformation/silver_transformer.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.transformation.quality_rules import NexusQualityRules

class SilverTransformer:
    def __init__(self, spark: SparkSession, catalog: str = "nff_catalog"):
        self.spark = spark
        self.catalog = catalog

    def transform_transactions(self):
        """
        Cleanses Bronze transactions, hashes PII, and handles rescued data.
        """
        rules = NexusQualityRules.get_silver_transaction_rules()
        
        # 1. Read from Bronze
        df_bronze = self.spark.table(f"{self.catalog".bronze.transactions")

        # 2. Apply Transformation & PII Hashing
        # We hash the customer_id to allow joins without exposing the raw ID 
        # to developers who don't have PII access.
        df_silver = df_bronze.select(
            F.col("tx_id").cast("string"),
            F.sha2(F.col("customer_id"), 256).alias("customer_hash"),
            F.col("amount").cast("double").alias("tx_amount"),
            F.col("currency").upper().alias("currency"),
            F.col("tx_time").cast("timestamp").alias("tx_timestamp"),
            F.col("region").cast("string"),
            # Audit metadata
            F.col("_ingestion_timestamp"),
            F.current_timestamp().alias("_processing_timestamp")
        ).filter(rules["positive_amount"]) # Apply quality rule from P6 library

        # 3. Handle 'Rescued' records
        # If _rescued_data is populated, we route it to a side-table for investigation
        df_errors = df_bronze.filter("_rescued_data IS NOT NULL")
        if df_errors.count() > 0:
            df_errors.write.mode("append").saveAsTable(f"{self.catalog}.governance.quarantine_tx")

        # 4. Upsert into Silver (Merge to handle duplicates)
        df_silver.createOrReplaceTempView("v_temp_silver")
        self.spark.sql(f"""
            MERGE INTO {self.catalog}.silver.transactions AS target
            USING v_temp_silver AS source
            ON target.tx_id = source.tx_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
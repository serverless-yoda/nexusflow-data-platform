# src/transformations/silver_transformer.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from .quality_rules import QualityRules
class SilverTransformer:
    """
    Handles cleaning and normalization for the Silver layer.
    Focuses on NZ region standardization and data integrity.
    """
    def __init__(self, spark):
        self.spark = spark

    def clean_transactions(self, df: DataFrame) -> DataFrame:
        """
        Standardizes raw transactions:
        1. Casts types (Amount to Double, Time to Timestamp)
        2. Normalizes Region names (e.g., 'akl' -> 'AUCKLAND')
        3. Adds a 'is_valid' flag based on QualityRules
        """
        # Get our centralized rules
        rules = QualityRules.get_transaction_rules()
        
        # Build a consolidated quality check expression
        # This creates a boolean flag: True if ALL rules pass
        quality_expr = " AND ".join([f"({rule})" for rule in rules.values()])

        return (
            df.select(
                F.col("tx_id").cast("string"),
                F.col("customer_id").cast("string"),
                F.col("amount").cast("double"),
                # Normalize Region for our Governance Manager
                F.upper(F.trim(F.col("region"))).alias("region"),
                # Standardize to NZ Time (UTC+12/13)
                F.to_timestamp(F.col("tx_time")).alias("tx_time"),
                F.col("_source_file")
            )
            .withColumn("is_valid", F.expr(quality_expr))
            .dropDuplicates(["tx_id"]) # Lead Standard: Never allow duplicate IDs in Silver
        )
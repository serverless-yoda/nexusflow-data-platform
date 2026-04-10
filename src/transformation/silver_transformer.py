# src/transformation/silver_transformer.py
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

    def clean_transactions(self, df: DataFrame, rules_method: str) -> DataFrame:
        # Use getattr to call the specific rules needed for this table
        rules_func = getattr(QualityRules, rules_method)
        rules = rules_func()
        
        quality_expr = " AND ".join([f"({rule})" for rule in rules.values()])

        return (
            df.select(
                F.col("tx_id").cast("string"),
                F.col("customer_id").cast("string"),
                F.col("amount").cast("double"),
                F.upper(F.trim(F.col("region"))).alias("region"),
                F.to_timestamp(F.col("tx_time")).alias("tx_time")
                # F.col("_source_file")  <-- REMOVE OR COMMENT THIS LINE
            )
            .withColumn("is_valid", F.expr(quality_expr))
            .dropDuplicates(["tx_id"])
        )
# src/transformations/gold_transformer.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class GoldTransformer:
    """
    Business Logic Layer for NexusFlow.
    Aggregates Silver data into high-value Gold KPIs.
    """
    def __init__(self, spark):
        self.spark = spark

    def calculate_regional_kpis(self, silver_df: DataFrame) -> DataFrame:
        """
        Aggregates transaction data into regional summaries.
        Calculates: Total Revenue, Avg Ticket Size, and Unique Customers.
        """
        return (
            silver_df
            .groupBy("region", F.window("tx_time", "1 day"))
            .agg(
                F.sum("amount").alias("daily_revenue"),
                F.count("tx_id").alias("transaction_count"),
                F.count_distinct("customer_id").alias("unique_customers")
            )
            .withColumn("avg_transaction_value", 
                        F.col("daily_revenue") / F.col("transaction_count"))
            # Lead Architect Tip: Flatten the window for easier dashboarding
            .withColumn("report_date", F.col("window.start"))
            .drop("window")
        )
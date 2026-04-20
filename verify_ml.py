import os
from src.common.path_resolver import PathResolver
from src.common.spark_session import NexusSpark
from pyspark.sql import functions as F

# 1. Connect to your Nexus Spark Session
spark = NexusSpark.get_session('local')

delta_path = PathResolver.resolve('./spark_warehouse', 'bronze_raw.db', 'local')
df = spark.read.format("delta").load(f"{delta_path}/ml_universal")
df.show(truncate=True)

# df_filtered = df.filter(F.col("file_format") == 'parquet')
# df_filtered.show(5, truncate=True)
 
# rows = df_filtered.select("source_file").distinct().collect() 
# paths = [row.source_file for row in rows]   
# clean_paths = [
#                     p.replace("file:///", "").replace("%20", " ")   
#                     for p in paths
#                 ]  
                             
# processed_df = spark.read.schema("tx_id STRING, customer_id STRING, amount DOUBLE, tx_time STRING").parquet(*clean_paths)
# processed_df.show(truncate=False)

delta_path = PathResolver.resolve('./spark_warehouse', 'silver_refined.db', 'local')
tables = [
    'customers_ml',
    'customers_ml_quarantine',
    'transactions_ml',
    'transactions_ml_quarantine'   
]
for table in tables:
    print(f"\n--- Inspecting SILVER: {table} ---")
    df = spark.read.format("delta").load(f"{delta_path}/{table}")
    df.show()


delta_path = PathResolver.resolve('./spark_warehouse', 'gold_ml.db', 'local')
tables = [
    'churn_features'
]
for table in tables:
    print(f"\n--- Inspecting GOLD: {table} ---")
    df = spark.read.format("delta").load(f"{delta_path}/{table}")
    df.show(9)

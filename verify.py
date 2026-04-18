import os
from src.common.path_resolver import PathResolver
from src.common.spark_session import NexusSpark
from pyspark.sql import functions as F

# 1. Connect to your Nexus Spark Session
spark = NexusSpark.get_session('local')

# 2. Base path matches your YAML 'local_base_path'
base_path = os.path.abspath("./data")

def inspect_layer(layer_name, table_name, sub_path):
    # This now resolves to ./data/bronze, ./data/silver, etc.
    full_path = os.path.abspath(os.path.join(base_path, sub_path.lstrip("/")))
    
    print(f"\n{'='*15} 🔍 {layer_name.upper()} LAYER {'='*15}")
    
    # Check 1: Catalog Registration
    exists_in_catalog = spark.catalog.tableExists(table_name)
    catalog_status = "✅ REGISTERED" if exists_in_catalog else "❌ NOT FOUND"
    print(f"Table: {table_name.ljust(20)} | Catalog: {catalog_status}")

    # Check 2: Physical Storage
    if os.path.exists(full_path):
        try:
            df = spark.read.format("delta").load(full_path)
            print(f"Path:  {full_path}")
            print(f"Count: {df.count()} records")
            
            if df.count() > 0:
                df.show(5, truncate=False)
        except Exception as e:
            print(f"⚠️ Delta Load Error: {e}")
    else:
        print(f"⚠️ Storage folder missing at: {full_path}")

# 3. Execution based on your NEW YML
#inspect_layer("Bronze", "bronze.universal_raw", "/bronze")
#inspect_layer("Silver", "silver.transactions", "/silver")
#inspect_layer("Gold", "gold.regional_kpis", "/gold")

# 4. Quarantine Check (Implicitly created in your Silver logic)
#inspect_layer("Quarantine", "N/A", "/silver_quarantine")

delta_path = PathResolver.resolve('./spark_warehouse', 'bronze_raw.db', 'local')
df = spark.read.format("delta").load(f"{delta_path}/universal_raw")
df.show(truncate=True)

delta_path = PathResolver.resolve('./spark_warehouse', 'silver_refined.db', 'local')
tables = [
    'transactions_parquet',
    'transactions_parquet_quarantine',
    'transactions_csv',
    'transactions_csv_quarantine',
    'transactions_json',
    'transactions_json_quarantine',
]
for table in tables:
    print(f"\n--- Inspecting SILVER: {table} ---")
    df = spark.read.format("delta").load(f"{delta_path}/{table}")
    df.show()


delta_path = PathResolver.resolve('./spark_warehouse', 'gold_analytics.db', 'local')
tables = [
    'regional_kpis'
]
for table in tables:
    print(f"\n--- Inspecting GOLD: {table} ---")
    df = spark.read.format("delta").load(f"{delta_path}/{table}")
    df.show()

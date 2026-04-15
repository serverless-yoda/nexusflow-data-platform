import os
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
inspect_layer("Bronze", "bronze.universal_raw", "/bronze")
inspect_layer("Silver", "silver.transactions", "/silver")
inspect_layer("Gold", "gold.regional_kpis", "/gold")

# 4. Quarantine Check (Implicitly created in your Silver logic)
#inspect_layer("Quarantine", "N/A", "/silver_quarantine")
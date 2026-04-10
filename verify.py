import os
from src.common.spark_session import NexusSpark
from pyspark.sql import functions as F

# 1. Initialize the Nexus Spark Session
spark = NexusSpark(run_mode="local")

# 2. Define your base path
base_path = os.path.abspath("./data")

# 3. Helper to display tables
def inspect_layer(layer_name, sub_path):
    full_path = os.path.join(base_path, sub_path.strip("/"))
    print(f"\n{'='*20} 🔍 {layer_name.upper()} LAYER {'='*20}")
    
    if os.path.exists(full_path):
        # Read directly from Delta files using the Nexus Session
        df = spark.read.format("delta").load(full_path)
        print(f"📍 Location: {full_path}")
        print(f"📊 Total Records: {df.count()}")
        df.show(5, truncate=False)
    else:
        print(f"⚠️ {layer_name} not found at {full_path}")

# 4. Run inspections
inspect_layer("Silver", "silver/transactions")
inspect_layer("Quarantine", "quarantine/transactions")
inspect_layer("Gold", "gold/regional_kpis")
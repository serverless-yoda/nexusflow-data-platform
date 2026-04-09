import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # Added for metadata injection
from datetime import datetime
from delta import configure_spark_with_delta_pip

# 1. SHIM LAYER: Mocking Databricks/DLT before imports
class MockDLT:
    def table(self, **kwargs): return lambda func: func
    def expect_or_drop(self, *args): return lambda func: func
    def expect_or_fail(self, *args): return lambda func: func
    def read(self, name): return SparkSession.active().read.table(name)

sys.modules["dlt"] = MockDLT()

# 2. INITIALIZE LOCAL SPARK WITH DELTA

builder = (SparkSession.builder
    .appName("NexusFlow-Local-Runner")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.ui.port", "4050")
    .master("local[*]"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# --- IMPORTS ---
sys.path.append(os.getcwd())
from src.common.data_generator import NexusDataGenerator
from src.transformation.silver_transformer import SilverTransformer
from src.transformation.gold_transformer import GoldTransformer

def run_local_pipeline():
    base_dir = os.path.abspath("./data")
    landing_path = os.path.join(base_dir, "landing")
    bronze_path = os.path.join(base_dir, "bronze")
    silver_path = os.path.join(base_dir, "silver")
    
    os.makedirs(landing_path, exist_ok=True)

    print("🚀 Phase 1: Seeding Local Landing Zone...")
    gen = NexusDataGenerator() 
    file_name = f"tx_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    full_path = os.path.join(landing_path, file_name)

    gen.write_to_landing(full_path, num_records=10, corruption_rate=0.02)
    print(f"✅ Data seeded at {full_path}")

    print("🚀 Phase 2: Bronze Ingestion (with Metadata Fix)...")
    # Load raw JSON
    raw_df = spark.read.json(landing_path + "/*.json")
    
    # --- FIX: INJECT DATABRICKS METADATA COLUMNS ---
    # SilverTransformer expects '_source_file' which isn't in raw JSON.
    bronze_df = (raw_df
        .withColumn("_source_file", F.lit(full_path))
        .withColumn("_rescued_data", F.lit(None).cast("string"))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )
    
    # Write to local Delta Lake
    bronze_df.write.format("delta").mode("overwrite").save(bronze_path)
    spark.read.load(bronze_path).createOrReplaceTempView("bronze_transactions")

    print("🚀 Phase 3: Silver Transformation...")
    silver_tool = SilverTransformer(spark)
    
    # This will now succeed because '_source_file' exists
    silver_df = silver_tool.clean_transactions(spark.table("bronze_transactions"))
    silver_df.write.format("delta").mode("overwrite").save(silver_path)
    spark.read.load(silver_path).createOrReplaceTempView("silver_transactions")

    print("🚀 Phase 4: Gold Transformation...")
    gold_tool = GoldTransformer(spark)
    gold_df = gold_tool.calculate_regional_kpis(spark.table("silver_transactions"))
    
    print("\n--- GOLD LAYER RESULTS ---")
    gold_df.show()
    print("✅ Local Pipeline Execution Successful.")

if __name__ == "__main__":
    run_local_pipeline()
# src/common/spark_session.py
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

class NexusSpark:
    _instance = None

    def __new__(cls, run_mode="local"):
        # 1. Normalize the run_mode and detect environment
        is_databricks = run_mode == "databricks" or "DATABRICKS_RUNTIME_VERSION" in os.environ
        app_name = f"NexusFlow-{'Databricks' if is_databricks else 'Local'}-Runner"

        if cls._instance is None:
            
            if is_databricks:
                # --- DATABRICKS OPTIMIZATIONS (2026 Standards) ---
                # Set Delta Lake configurations directly on the active Spark session
                active_spark = SparkSession.getActiveSession()
                active_spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                active_spark.conf.set("spark.databricks.delta.deletionVectors.enabled", "true")
                active_spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
                return active_spark
            else:
                builder = SparkSession.builder.appName(app_name)
        
                # --- LOCAL DELTA OPTIMIZATIONS ---
                # Create a local temp dir that isn't on the C: root
                local_temp = os.path.abspath("./spark_temp")
                os.makedirs(local_temp, exist_ok=True)

                builder.master("local[*]") \
                       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                       .config("spark.sql.warehouse.dir", os.path.abspath("./spark_warehouse")) \
                       .config("spark.local.dir", local_temp) \
                       .config("spark.sql.streaming.schemaInference", "true") \
                       .config("spark.ui.port", "4050")
                
                # Use the Delta-PIP wrapper for local Spark
                cls._instance = configure_spark_with_delta_pip(builder).getOrCreate()
                
                # Silence the ShutdownHookManager and log noise
                cls._instance.sparkContext.setLogLevel("ERROR")
                
                return cls._instance
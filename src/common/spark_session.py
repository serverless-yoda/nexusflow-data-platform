import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

class NexusSpark:
    """
    SOLID Factory for Spark Session management.
    """
    _session = None

    @classmethod
    def get_session(cls, run_mode="local"):
        """
        The explicit entry point to get the Spark Session.
        """
        if cls._session is None:
            # 1. Determine environment
            is_databricks = run_mode == "databricks" or "DATABRICKS_RUNTIME_VERSION" in os.environ
            
            if is_databricks:
                # --- DATABRICKS CONFIG ---
                spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
                cls._session = spark
            else:
                # --- LOCAL CONFIG ---
                local_temp = os.path.abspath("./spark_temp")
                os.makedirs(local_temp, exist_ok=True)
                
                builder = (SparkSession.builder
                           .appName("NexusFlow-Local")
                           .master("local[*]")
                           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                            .config("spark.sql.warehouse.dir", os.path.abspath("./spark_warehouse")) \
                            .config("spark.local.dir", local_temp) \
                            .config("spark.sql.streaming.schemaInference", "true") \
                            .config("spark.ui.port", "4050")
                        )

                cls._session = configure_spark_with_delta_pip(builder).getOrCreate()
                cls._session.sparkContext.setLogLevel("ERROR")
        
        return cls._session
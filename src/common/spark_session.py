# src/common/spark_session.py
from pyspark.sql import SparkSession
import os

class NexusSparkFactory:
    """
    Centralized Factory for NexusFlow Spark Sessions.
    Handles environment-specific configs for the NZ region.
    """
    _instance = None

    @staticmethod
    def get_session(app_name: str = "NexusFlow-Task"):
        """Returns the optimized Spark session."""
        if NexusSparkFactory._instance is None:
            # Determine environment
            env = os.getenv("NEXUS_ENV", "dev")
            
            builder = SparkSession.builder.appName(app_name)
            
            # 1. Apply Global Optimizations (2026 Standards)
            builder.config("spark.databricks.delta.optimizeWrite.enabled", "true")
            builder.config("spark.databricks.delta.autoCompact.enabled", "true")
            
            # 2. Apply Environment-Specific Configs
            if env == "prod":
                builder.config("spark.databricks.io.cache.enabled", "true") # Faster IO for Gold layer
            
            NexusSparkFactory._instance = builder.get_OrCreate()
            
        return NexusSparkFactory._instance

# Alias for ease of use in notebooks
NexusSpark = NexusSparkFactory.get_session
# src/common/spark_session.py
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

class NexusSparkFactory:
    """
    Centralized Factory to generate a secure, production-grade Spark Session
    for the NexusFlow platform.
    """
    
    def __init__(self, env: str, storage_account: str):
        self.env = env
        self.storage_account = storage_account
        self.spark = SparkSession.builder.getOrCreate()
        self.dbutils = DBUtils(self.spark)
        self.scope = "nff-secrets" # Defined in Phase 2 Terraform

    def _apply_storage_security(self):
        """
        Fetches credentials from Azure Key Vault via Secret Scope
        and injects them into the Spark Context.
        """
        try:
            # Securely fetch the key - this value is masked in logs
            storage_key = self.dbutils.secrets.get(
                scope=self.scope, 
                key="storage-account-key"
            )
            
            # Configure ADLS Gen2 Connectivity
            self.spark.conf.set(
                f"fs.azure.account.key.{self.storage_account}.dfs.core.windows.net",
                storage_key
            )
        except Exception as e:
            raise PermissionError(f"Failed to secure Spark Session: {str(e)}")

    def get_session(self):
        """
        Returns the configured Spark session.
        """
        self._apply_storage_security()
        
        # Optimize for 2026 Databricks Runtimes (Liquid Clustering Support)
        self.spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        self.spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        
        return self.spark
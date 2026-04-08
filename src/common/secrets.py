# src/common/secrets.py
from pyspark.sql import SparkSession

class NexusSecrets:
    """
    Service Layer for retrieving credentials from Databricks Secret Scopes.
    Scope 'nexus-flow-scope' must be linked to Azure Key Vault in Phase 2.
    """
    def __init__(self, spark: SparkSession, scope: str = "nexus-flow-scope"):
        self.spark = spark
        self.scope = scope
        
        # Initialize dbutils for secret retrieval
        try:
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(self.spark)
        except ImportError:
            # Fallback for local unit testing where dbutils isn't available
            self.dbutils = None

    def get_secret(self, key: str) -> str:
        """Generic fetch for any key in the Nexus scope."""
        if not self.dbutils:
            return "mock-secret-for-local-test"
        return self.dbutils.secrets.get(scope=self.scope, key=key)

    def get_adls_config(self):
        """Returns a dict of credentials for Spark Conf auth."""
        return {
            "client_id": self.get_secret("sp-client-id"),
            "tenant_id": self.get_secret("azure-tenant-id"),
            "client_secret": self.get_secret("sp-client-secret")
        }
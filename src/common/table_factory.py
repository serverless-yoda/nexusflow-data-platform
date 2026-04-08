# src/common/table_factory.py
from pyspark.sql import DataFrame
from src.common.governance import GovernanceManager

class NexusTableFactory:
    """
    Standardizes table creation in Unity Catalog.
    Ensures Liquid Clustering and Delta Lake best practices.
    """
    def __init__(self, spark, catalog="nff_catalog"):
        self.spark = spark
        self.catalog = catalog
        self.gov = GovernanceManager(spark, catalog)

    def create_managed_table(self, df: DataFrame, schema: str, table: str, cluster_by: list = None):
        """
        Saves a DataFrame as a Managed Unity Catalog table 
        with Liquid Clustering enabled.
        """
        full_table_name = f"{self.catalog}.{schema}.{table}"
        
        writer = df.write.format("delta").mode("overwrite")
        
        # 2026 Lead-Level Standard: Use Liquid Clustering instead of partitions
        if cluster_by:
            writer = writer.clusterBy(*cluster_by)
            
        writer.saveAsTable(full_table_name)
        
        # Auto-apply governance if 'region' is in the table
        if "region" in df.columns:
            self.gov.apply_policy_to_table(schema, table)
            
        print(f"✅ Table {full_table_name} created with Liquid Clustering and Governance.")

    def optimize_table(self, schema: str, table: str):
        """Triggers manual maintenance for high-churn tables."""
        self.spark.sql(f"OPTIMIZE {self.catalog}.{schema}.{table}")
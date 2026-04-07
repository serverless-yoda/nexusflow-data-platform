# src/common/governance.py
from pyspark.sql import SparkSession

class GovernanceManager:
    """
    Automates the application of Unity Catalog governance policies 
    (RLS and Masking) across the NexusFlow platform.
    """
    
    def __init__(self, spark: SparkSession, catalog: str = "nff_catalog"):
        self.spark = spark
        self.catalog = catalog
        self.gov_schema = f"{catalog}.governance"

    def apply_regional_filter(self, table_fqn: str, region_col: str = "region"):
        """
        Attaches the standard regional row filter to a table.
        """
        print(f"🔐 Applying Row Filter to {table_fqn}...")
        self.spark.sql(f"""
            ALTER TABLE {table_fqn} 
            SET ROW FILTER {self.gov_schema}.region_filter ON ({region_col})
        """)

    def mask_pii_column(self, table_fqn: str, column_name: str, mask_type: str = "email"):
        """
        Applies a dynamic masking function to a specific PII column.
        """
        mask_function = {
            "email": f"{self.gov_schema}.email_mask",
            "phone": f"{self.gov_schema}.phone_mask"
        }.get(mask_type)

        if not mask_function:
            raise ValueError(f"Unsupported mask type: {mask_type}")

        print(f"🎭 Masking {column_name} in {table_fqn} using {mask_type} logic...")
        self.spark.sql(f"""
            ALTER TABLE {table_fqn} 
            ALTER COLUMN {column_name} SET MASK {mask_function}
        """)

    def tag_sensitive_data(self, table_fqn: str, level: str = "PII"):
        """
        Applies Unity Catalog Tags for automated discovery and auditing.
        """
        self.spark.sql(f"ALTER TABLE {table_fqn} SET TAGS ('security_level' = '{level}')")
# src/common/governance.py
from pyspark.sql import SparkSession

class GovernanceManager:
    """
    Enforces Row-Level Security (RLS) and Column Masking.
    Used to ensure regional data isolation in the NexusFlow catalog.
    """
    def __init__(self, spark: SparkSession, catalog: str = "nff_catalog"):
        self.spark = spark
        self.catalog = catalog

    def register_security_functions(self):
        """
        Initializes the SQL UDFs used for filtering. 
        Usually called once during Phase 4 (Setup).
        """
        # Row Filter: Checks if user belongs to a group matching the 'region' column
        self.spark.sql(f"""
            CREATE FUNCTION IF NOT EXISTS {self.catalog}.main.regional_filter(region STRING)
            RETURN is_account_group_member(concat('nff_region_', lower(region)))
            OR is_account_group_member('nff_admin')
        """)
        
        # Masking Function: Hides PII for anyone not in the 'Data Steward' group
        self.spark.sql(f"""
            CREATE FUNCTION IF NOT EXISTS {self.catalog}.main.pii_mask(col STRING)
            RETURN CASE 
                WHEN is_account_group_member('nff_data_stewards') THEN col 
                ELSE '### MASKED ###' 
            END
        """)

    def apply_policy_to_table(self, schema: str, table: str, region_col: str = "region"):
        """Attaches the RLS filter to a specific table."""
        full_path = f"{self.catalog}.{schema}.{table}"
        self.spark.sql(f"ALTER TABLE {full_path} SET ROW FILTER {self.catalog}.main.regional_filter ON ({region_col})")

    def mask_column(self, schema: str, table: str, column: str):
        """Attaches a PII mask to a specific column."""
        full_path = f"{self.catalog}.{schema}.{table}"
        self.spark.sql(f"ALTER TABLE {full_path} ALTER COLUMN {column} SET MASK {self.catalog}.main.pii_mask")
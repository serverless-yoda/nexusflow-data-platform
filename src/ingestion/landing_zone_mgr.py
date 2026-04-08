# src/ingestion/landing_zone.py
from src.common.secrets import NexusSecrets

class LandingZoneManager:
    """
    Manages the physical raw storage layer.
    Handles file listing, archiving, and structure validation.
    """
    def __init__(self, spark):
        self.spark = spark
        self.secrets = NexusSecrets(spark)
        # Path for the NZ Fintech raw data
        self.base_path = "abfss://raw@nexusstorage.dfs.core.windows.net/"

    def validate_structure(self, folder="transactions"):
        """Checks if the required directory structure exists in ADLS."""
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            full_path = f"{self.base_path}{folder}/"
            dbutils.fs.ls(full_path)
            return True
        except Exception:
            print(f"⚠️ Warning: Landing zone path {full_path} not found.")
            return False

    def get_file_count(self, folder="transactions"):
        """Returns the number of files waiting to be ingested."""
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(self.spark)
        files = dbutils.fs.ls(f"{self.base_path}{folder}/")
        return len(files)

    def archive_processed_files(self, source_folder, archive_folder):
        """
        Manually moves files to an archive folder after 
        successful non-streaming batch ingestion.
        """
        # Note: Auto Loader handles its own checkpointing, 
        # but this is used for manual Phase 3 backfills.
        pass
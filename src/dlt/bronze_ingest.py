# src/dlt/bronze_ingest.py
import dlt
from pyspark.sql.functions import current_timestamp
from src.common.secrets import NexusSecrets

# Initialize secrets (using our Common Manager)
secrets = NexusSecrets(spark)
landing_path = "abfss://raw@nexusstorage.dfs.core.windows.net/transactions/"

@dlt.table(
    name="bronze_transactions",
    comment="Raw transactions ingested via Auto Loader with Rescued Data support.",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true"
    }
)
def bronze_transactions():
    """
    Ingests JSON files from ADLS using Auto Loader.
    Captures corrupt records in '_rescued_data' to prevent pipeline failure.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        # The 'Rescued Data' column catches anything that doesn't match the schema
        .option("cloudFiles.schemaEvolutionMode", "rescue") 
        .load(landing_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", "_metadata.file_path") # Built-in file tracking
    )
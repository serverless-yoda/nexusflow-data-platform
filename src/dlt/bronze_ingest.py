# dlt/bronze_ingest.py
import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

# Configuration is managed via the DLT Pipeline settings (JSON)
# We pull these variables at runtime
source_path = dlt.get_param("landing_path")
checkpoint_path = dlt.get_param("checkpoint_path")

@dlt.table(
    name="transactions_bronze",
    comment="Raw financial transactions ingested via Auto Loader",
    table_properties={
        "quality": "bronze",
        "delta.appendOnly": "true"
    }
)
@dlt.expect("valid_file_source", "_source_file_path IS NOT NULL")
def transactions_bronze():
    """
    Ingests raw JSON from the landing zone into the Bronze Delta table.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema/bronze")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Captures malformed data without killing the pipeline
        .option("cloudFiles.rescuedDataColumn", "_rescued_data") 
        .load(source_path)
        .select(
            "*",
            current_timestamp().alias("_ingestion_timestamp"),
            input_file_name().alias("_source_file_path")
        )
    )
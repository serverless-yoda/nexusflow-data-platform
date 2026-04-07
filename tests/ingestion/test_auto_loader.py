# tests/ingestion/test_auto_loader.py
import pytest
from pyspark.sql import Row
import time

@pytest.fixture
def landing_zone(tmp_path):
    """Creates a temporary local directory to simulate ADLS Landing."""
    return str(tmp_path / "landing")

@pytest.fixture
def checkpoint_path(tmp_path):
    """Temporary checkpoint directory for the stream."""
    return str(tmp_path / "checkpoints")

def test_auto_loader_schema_evolution(spark, landing_zone, checkpoint_path):
    """
    Verify that Auto Loader adds new columns automatically 
    without failing the pipeline.
    """
    # 1. Drop a standard file
    spark.createDataFrame([Row(tx_id="TX1", amount=100.0)]) \
         .write.json(f"{landing_zone}/file1.json")
    
    # 2. Start the stream using our Phase 5 wrapper
    from src.ingestion.auto_loader import NexusAutoLoader
    loader = NexusAutoLoader(spark, checkpoint_path)
    
    # Process first file
    loader.read_json_stream(landing_zone, "test_bronze_table")
    
    # 3. Drop a file with a NEW column ('currency')
    spark.createDataFrame([Row(tx_id="TX2", amount=200.0, currency="NZD")]) \
         .write.json(f"{landing_zone}/file2.json")
    
    # Process second file (Trigger once)
    loader.read_json_stream(landing_zone, "test_bronze_table")
    
    # 4. Assert schema evolved
    df = spark.table("test_bronze_table")
    assert "currency" in df.columns, "Schema failed to evolve to include 'currency'."

def test_rescued_data_captures_corruption(spark, landing_zone, checkpoint_path):
    """
    Verify that malformed JSON is moved to _rescued_data 
    instead of crashing the stream.
    """
    # 1. Manually write a 'poison' JSON string (amount is a string, expected double)
    bad_json = '{"tx_id": "TX_BAD", "amount": "NOT_A_NUMBER"}'
    spark.createDataFrame([Row(value=bad_json)]).write.text(f"{landing_zone}/bad.json")
    
    from src.ingestion.auto_loader import NexusAutoLoader
    loader = NexusAutoLoader(spark, checkpoint_path)
    loader.read_json_stream(landing_zone, "test_bronze_table")
    
    # 2. Verify the record exists but the 'amount' is null and the raw JSON is rescued
    res = spark.table("test_bronze_table").filter("tx_id = 'TX_BAD'").collect()[0]
    
    assert res["amount"] is None, "Corrupted numeric field should be null."
    assert "_rescued_data" in res, "Rescued data column is missing."
    assert "NOT_A_NUMBER" in res["_rescued_data"], "Raw corrupted string was not captured."
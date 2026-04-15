# tests/integration/test_storage_layer.py
import pytest
from src.common.____spark_session import NexusSpark
from src.common.secrets import NexusSecrets

@pytest.mark.integration
def test_adls_read_write_connectivity():
    """
    Verifies that Spark can authenticate and write to the Azure Landing Zone.
    This is a "Smoke Test" for the 2026 NZ Environment setup.
    """
    spark = NexusSpark()
    secrets = NexusSecrets(spark)
    
    # Path for the integration canary file
    test_path = "abfss://raw@nexusstorage.dfs.core.windows.net/integration_test/canary.parquet"
    
    # 1. Create dummy data
    data = [("connectivity_test", 1.0)]
    df = spark.createDataFrame(data, ["test_name", "status"])
    
    try:
        # 2. Attempt Write
        df.write.mode("overwrite").parquet(test_path)
        
        # 3. Attempt Read
        read_df = spark.read.parquet(test_path)
        
        assert read_df.count() == 1
        assert read_df.collect()[0]["test_name"] == "connectivity_test"
        print("✅ Integration: ADLS Storage Layer Connectivity Verified.")
        
    except Exception as e:
        pytest.fail(f"❌ Storage Integration Failed: {str(e)}")
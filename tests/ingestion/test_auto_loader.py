# tests/ingestion/test_auto_loader.py
import pytest
from src.ingestion.autoloader import NexusAutoLoader
from unittest.mock import patch, MagicMock

def test_autoloader_initialization(spark_session):
    """Verifies that the loader initializes with the correct paths."""
    source = "/tmp/nexus/raw/"
    target = "nff_catalog.bronze.transactions"
    
    loader = NexusAutoLoader(source, target)
    
    assert loader.source_path == source
    assert loader.target_table == target
    assert loader.spark is not None

@patch("src.common.secrets.NexusSecrets.get_adls_config")
def test_autoloader_configures_secrets(mock_secrets, spark_session):
    """Ensures the loader fetches and applies Azure credentials."""
    # Setup Mock
    mock_secrets.return_text = {
        "client_id": "test-id",
        "client_secret": "test-secret",
        "tenant_id": "test-tenant"
    }
    
    loader = NexusAutoLoader("/tmp/raw", "bronze.test")
    
    # Trigger logic (we only check the config side-effects)
    loader.run_ingestion()
    
    # Assert that Spark was configured with our mock values
    conf = spark_session.conf
    assert conf.get("fs.azure.account.oauth2.client.id") == "test-id"
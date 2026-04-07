# tests/integration/test_storage_layer.py
import pytest
from src.common.spark_session import NexusSparkFactory

@pytest.fixture(scope="module")
def spark():
    # Initialize the secure session for the test environment
    factory = NexusSparkFactory(env="test", storage_account="stnexusflowtest")
    return factory.get_session()

def test_bronze_append_only_prop(spark):
    """
    Verify Bronze tables are set to Append-Only to protect raw history.
    """
    details = spark.sql("DESCRIBE DETAIL nff_catalog.bronze.transactions").collect()[0]
    properties = details['properties']
    
    assert properties.get('delta.appendOnly') == 'true', "Bronze table must be Append-Only."

def test_liquid_clustering_enabled(spark):
    """
    Verify Silver and Gold tables are using Liquid Clustering, not Hive partitioning.
    """
    # Check the Silver transactions table
    details = spark.sql("DESCRIBE DETAIL nff_catalog.silver.transactions").collect()[0]
    
    # Liquid clustering columns appear in the 'clusteringColumns' metadata field in Delta 3.x+
    clustering_cols = details['clusteringColumns']
    
    assert 'customer_id' in clustering_cols, "Liquid clustering missing 'customer_id' index."
    assert 'tx_timestamp' in clustering_cols, "Liquid clustering missing 'tx_timestamp' index."

def test_deletion_vectors_active(spark):
    """
    Ensure Deletion Vectors are enabled to optimize SCD Type 2 merge performance.
    """
    details = spark.sql("DESCRIBE DETAIL nff_catalog.silver.transactions").collect()[0]
    properties = details['properties']
    
    # Check for the specific Delta feature property
    dv_enabled = properties.get('delta.enableDeletionVectors')
    assert dv_enabled == 'true', "Deletion Vectors must be enabled for Silver/Gold layers."
# src/transformation/quality_rules.py

class NexusQualityRules:
    """
    Centralized repository of Data Quality 'Expectations' 
    for the NexusFlow Medallion layers.
    """

    @staticmethod
    def get_bronze_expectations():
        """Basic structural integrity for raw ingestion."""
        return {
            "valid_json": "_rescued_data IS NULL",
            "has_source": "_source_file_path IS NOT NULL"
        }

    @staticmethod
    def get_silver_transaction_rules():
        """Critical financial integrity rules for Silver."""
        return {
            "non_null_tx_id": "tx_id IS NOT NULL",
            "positive_amount": "tx_amount > 0",
            "valid_currency": "currency IN ('NZD', 'AUD', 'USD', 'GBP')",
            "recent_timestamp": "tx_timestamp > '2020-01-01'"
        }

    @staticmethod
    def get_gold_business_rules():
        """Business-level consistency for the Star Schema."""
        return {
            "valid_customer_link": "customer_id IS NOT NULL",
            "assigned_region": "region IS NOT NULL"
        }
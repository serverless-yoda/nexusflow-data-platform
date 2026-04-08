# src/sql/transformations/quality_rules.py

class QualityRules:
    """
    Centralized repository of Data Quality expectations.
    Returns dictionaries compatible with DLT @dlt.expect decorators.
    """

    @staticmethod
    def get_transaction_rules():
        """Rules for the Silver Transactions layer."""
        return {
            "valid_id": "tx_id IS NOT NULL",
            "positive_amount": "amount > 0",
            "known_region": "region IN ('AUCKLAND', 'WELLINGTON', 'CHRISTCHURCH', 'HAMILTON')",
            "future_date_check": "tx_time <= current_timestamp()"
        }

    @staticmethod
    def get_customer_rules():
        """Rules for Customer master data."""
        return {
            "valid_email": "customer_email LIKE '%@%.%'",
            "nz_phone_format": "phone_number RLIKE '^(\\+64|0)[2-9][0-9]{7,9}$'"
        }
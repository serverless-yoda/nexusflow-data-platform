# src/common/data_generator.py
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

class NexusDataGenerator:
    """
    Generates synthetic NZ-centric fintech transactions for the 
    NexusFlow Medallion pipeline.
    """
    
    def __init__(self):
        self.fake = Faker(['en_NZ']) # Specifically use NZ locale
        self.regions = ["Auckland", "Wellington", "Canterbury", "Otago", "Waikato"]
        self.currencies = ["NZD", "AUD", "USD", "GBP"]
        self.tiers = ["Silver", "Gold", "Platinum"]

    def generate_transaction(self, is_corrupt=False):
        """Creates a single transaction record."""
        
        # Intentional corruption for testing Phase 5/6 'Rescued Data'
        if is_corrupt:
            return {
                "tx_id": "CORRUPT_RECORD",
                "amount": "NOT_A_NUMBER", # String instead of Double
                "tx_time": "INVALID_DATE"
            }

        return {
            "tx_id": str(uuid.uuid4()),
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "amount": round(random.uniform(10.0, 5000.0), 2),
            "currency": random.choice(self.currencies),
            "tx_time": (datetime.now() - timedelta(minutes=random.randint(0, 1000))).isoformat(),
            "region": random.choice(self.regions),
            "merchant_metadata": {
                "name": self.fake.company(),
                "category": random.choice(["Retail", "Food", "Utility", "Transfer"])
            }
        }

    def write_to_landing(self, file_path, num_records=5, corruption_rate=0.05):
        """Writes a batch of JSON records to the ADLS Landing Zone."""
        records = []
        for _ in range(num_records):
            should_corrupt = random.random() < corruption_rate
            records.append(self.generate_transaction(is_corrupt=should_corrupt))
        
        with open(file_path, 'w') as f:
            # Write as JSON Lines (standard for high-volume ingestion)
            for entry in records:
                f.write(json.dumps(entry) + '\n')
        
        print(f"✅ Generated {num_records} records at {file_path}")
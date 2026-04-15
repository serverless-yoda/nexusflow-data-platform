# src/common/data_generator.py
import os
import json
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

class NexusDataGenerator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def generate_mock_transactions(self, num_records=5):
        """Generates a list of dictionaries simulating valid and invalid NZ transaction data."""
        regions = ["AUCKLAND", "WELLINGTON", "CHRISTCHURCH", "HAMILTON"]
        data = []
        
        for i in range(num_records):
            # 20% chance to generate "invalid" data for testing quarantine
            is_invalid = random.random() < 0.20
            
            tx_time = datetime.now() - timedelta(minutes=random.randint(0, 1000))
            
            if is_invalid:
                # Pick a specific failure mode
                failure_mode = random.choice(["null_region", "negative_amt", "bad_id"])
                
                record = {
                    "tx_id": f"TXN-{random.randint(10000, 99999)}" if failure_mode != "bad_id" else "INVALID_ID_###",
                    "customer_id": f"CUST-{random.randint(100, 999)}",
                    "amount": round(random.uniform(-500.0, -10.0), 2) if failure_mode == "negative_amt" else round(random.uniform(10.0, 500.0), 2),
                    "region": None if failure_mode == "null_region" else random.choice(regions),
                    "tx_time": tx_time.strftime("%Y-%m-%dT%H:%M:%S")
                }
            else:
                # Standard valid record
                record = {
                    "tx_id": f"TXN-{random.randint(10000, 99999)}",
                    "customer_id": f"CUST-{random.randint(100, 999)}",
                    "amount": round(random.uniform(10.0, 500.0), 2),
                    "region": random.choice(regions),
                    "tx_time": tx_time.strftime("%Y-%m-%dT%H:%M:%S")
                }
            data.append(record)
        return data
        

    def write_to_landing(self, target_path, num_records=5, run_mode="local"):
        """
        Writes JSON data to the landing zone. 
        Handles local OS paths or Spark-compatible cloud paths.
        """
        data = self.generate_mock_transactions(num_records)
        
        if run_mode == "local":
            # Ensure local directory exists
            # 1. Ensure the landing folder exists as a DIRECTORY
            os.makedirs(target_path, exist_ok=True) 

            # 2. Create a unique filename for the records
            import uuid
            filename = f"batch_{uuid.uuid4().hex[:8]}.json"
            full_file_path = os.path.join(target_path, filename)

            # 3. Write the data to that specific file
            with open(full_file_path, "w") as f:
                json.dump(data, f)

            print(f"📍 [LOCAL] Seeded {num_records} records to {full_file_path}")

        else:
            # In Databricks, use Spark to write to ABFSS/Unity Catalog paths
            df = self.spark.createDataFrame(data)
            #df.coalesce(1).write.mode("append").json(target_path)
            df.coalesce(1).write.mode("append").format("json").save(target_path)
            print(f"☁️ [DATABRICKS] Seeded {num_records} records to {target_path}")
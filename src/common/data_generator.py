# src/utils/generator.py
import os
import sys
import random
from datetime import datetime
from pyspark.sql import functions as F

class NexusDataGenerator:
    def __init__(self, spark):
        self.spark = spark
        self.regions = ["Auckland", "Wellington", "Christchurch", "Hamilton", "Tauranga"]
        # CRITICAL WINDOWS FIX:
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    def _generate_base_records(self, num_records):
        data = []
        for i in range(num_records):
            data.append({
                "tx_id": f"TXN-{1000 + i}",
                "amount": round(random.uniform(10.0, 500.0), 2),
                "region": random.choice(self.regions),
                "tx_time": datetime.now().isoformat()
            })
        return data

    def _apply_corruption(self, data, corruption_level):
        if corruption_level <= 0: return data
        for record in data:
            if random.random() < corruption_level:
                choice = random.choice(["null_id", "neg_amount", "bad_region", "malformed_json"])
                if choice == "null_id": record["tx_id"] = None
                elif choice == "neg_amount": record["amount"] = -99.99
                elif choice == "bad_region": record["region"] = None
                elif choice == "malformed_json": record["amount"] = "CORRUPT_STRING"
        return data

    def _nest_data(self, data):
        nested_data = []
        for i in range(0, len(data), 5):
            chunk = data[i:i+5]
            parent = {
                "store_id": f"STORE-{random.randint(1, 50)}",
                "tx_date": datetime.now().isoformat(),
                "items": chunk 
            }
            nested_data.append(parent)
        return nested_data

    def write_scenario(self, destination_path, format="json", nested=False, corruption=0.1, num_records=100):
        # Convert path to local relative path if it starts with /
        if destination_path.startswith("/"):
            destination_path = "." + destination_path
        destination_path = destination_path.replace("\\", "/")

        raw_data = self._generate_base_records(num_records)
        corrupted_data = self._apply_corruption(raw_data, corruption)
        
        if nested:
            corrupted_data = self._nest_data(corrupted_data)

        # Create DataFrame
        df = self.spark.createDataFrame(raw_data)
        
        # Force all to string for the write phase to avoid schema mismatch crashes
        if format in ["json", "csv"]:
            for col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast("string"))

        if format == "json":
            df.write.mode("overwrite").json(destination_path)
        elif format == "csv":
            if nested:
                df = df.selectExpr("store_id", "tx_date", "inline(items)")
            df.write.mode("overwrite").option("header", "true").csv(destination_path)
        elif format == "parquet":
            # Parquet stays binary/typed
            df.write.mode("overwrite").parquet(destination_path)

        print(f"✅ Success! Data seeded to {os.path.abspath(destination_path)}")
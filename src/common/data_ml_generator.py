import os
import random
import uuid
from datetime import datetime
from pyspark.sql import functions as F

class NexusMLDataGenerator:
    def __init__(self, spark):
        self.spark = spark
        self.tiers = ["Bronze", "Silver", "Gold", "Platinum"]
        # Maintain a list of IDs to ensure referential integrity for Joins
        self.customer_ids = [f"CUST-{i:04d}" for i in range(1, 1001)] 

    def generate_ml_customers(self, destination_path):
        """Generates static customer features + The Target Label."""
        data = []
        for c_id in self.customer_ids:
            # Logic: We determine "is_churned" based on hidden factors
            # (e.g., higher support tickets and low tenure increases churn probability)
            support_tickets = random.randint(0, 10)
            tenure_months = random.randint(1, 60)
            
            # HIDDEN SIGNAL: Target logic
            is_churned = 1 if (support_tickets > 7 or tenure_months < 3) else 0

            data.append({
                "customer_id": c_id,
                "tenure_months": tenure_months,
                "support_tickets": support_tickets,
                "tier": random.choice(self.tiers),
                "is_churned": is_churned # The Target for Classification
            })
            
        # Inject ML Noise (Outliers)
        data[0]["support_tickets"] = 999  # Outlier to be cleaned in Silver
        
        self._write(data, f"{destination_path}/customers_ml", "parquet")

    def generate_ml_transactions(self, destination_path, num_records=5000):
        """Generates behavioral transaction data for the same customers."""
        data = []
        for _ in range(num_records):
            data.append({
                "tx_id": f"TXN-{uuid.uuid4().hex[:6].upper()}",
                "customer_id": random.choice(self.customer_ids), # <--- The Join Key
                "amount": round(random.uniform(10.0, 500.0), 2),
                "tx_time": datetime.now().isoformat()
            })
            
        # Inject ML Noise (Missing values)
        data[0]["customer_id"] = None # To test join failures
        
        self._write(data, f"{destination_path}/transactions_ml", "json")

    def _write(self, data, path, format):
        clean_path = ("." + path if path.startswith("/") else path).replace("\\", "/")
        df = self.spark.createDataFrame(data)
        if "tenure_months" in df.columns:
            df = df.withColumn("tenure_months", F.col("tenure_months").cast("int")) # Casts to 32-bit INT
        if "support_tickets" in df.columns:
            df = df.withColumn("support_tickets", F.col("support_tickets").cast("int"))
        if "is_churned" in df.columns:
            df = df.withColumn("is_churned", F.col("is_churned").cast("int"))

        if format == "json":
            # Cast everything to string for Bronze 'payload' logic
            for col in df.columns:
                df = df.withColumn(col, F.col(col).cast("string"))
            df.write.mode("overwrite").json(clean_path)
        else:
            df.write.mode("overwrite").parquet(clean_path)
            
        print(f"📊 Seeded {format.upper()} to {os.path.abspath(clean_path)}")
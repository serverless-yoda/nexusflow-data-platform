# src/notebooks/00_seed_data.py
from src.common.data_generator import NexusDataGenerator
import os

# Define the landing path (usually mounted ADLS Gen2)
LANDING_PATH = "/mnt/nexus_landing/raw_transactions/"
os.makedirs(LANDING_PATH, exist_ok=True)

gen = NexusDataGenerator()

# Generate a batch of 5,000 records with a 2% corruption rate
# This simulates a typical "Daily" ingestion file
file_name = f"tx_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
full_path = os.path.join(LANDING_PATH, file_name)

gen.write_to_landing(full_path, num_records=5000, corruption_rate=0.02)

print(f"Successfully seeded {full_path} for ingestion testing.")
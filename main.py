import yaml
import os
from src.common.spark_session import NexusSpark
from src.common.data_generator import NexusDataGenerator

from src.layers.processors import BronzeProcessor, SilverProcessor, GoldProcessor

def main():
    with open("conf/pipeline_manifest.yml", "r") as f:
        config = yaml.safe_load(f)

    run_mode = config['settings']['run_mode']
    base_path = config['settings']['local_base_path'] if run_mode == "local" else config['settings']['cloud_base_path']
    
    # Initialize Session
    spark = NexusSpark.get_session(run_mode)

    if run_mode == "local":
        print("🛠️  Running Local Data Generator...")
        try:
            landing_path = os.path.join(base_path, "landing/raw")
            #print(f"📂 Target Landing Path: {landing_path}")
            generator = NexusDataGenerator(spark)
            generator.write_to_landing(landing_path, num_records=10, run_mode=run_mode)
            print(f"✅ Mock data ready in: {landing_path}")
        except Exception as e:
            print(f"⚠️  Data Generator skipped or failed: {e}")

    # Processor Map
    processor_map = {
        "bronze": BronzeProcessor,
        "silver": SilverProcessor,
        "gold": GoldProcessor
    }

    for table_cfg in config['tables']:
        print(f"🚀 Processing {table_cfg['name']} ({table_cfg['type']})")
        processor_class = processor_map[table_cfg['type']]
        
        # Instantiate and Run
        instance = processor_class(spark, table_cfg, run_mode, base_path)
        instance.process()

if __name__ == "__main__":
    main()
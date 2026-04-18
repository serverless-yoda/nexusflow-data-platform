# main.py
import shutil
import yaml
import os
import sys

from src.common.spark_session import NexusSpark
from src.common.data_generator import NexusDataGenerator
from src.layers.processors import BronzeProcessor, SilverProcessor, GoldProcessor


def main():
    with open("conf/pipeline_manifest.yml", "r") as f:
        config = yaml.safe_load(f)

    run_mode = config['settings']['run_mode']
    base_path = config['settings']['local_base_path'] if run_mode == "local" else config['settings']['cloud_base_path']
    
    
    if run_mode == "local":
        # This tells Spark to use the exact same Python that is running this script
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        # Clean landing area before seeding
        shutil.rmtree("./data", ignore_errors=True)  
        shutil.rmtree("./spark_temp", ignore_errors=True)  
        shutil.rmtree("./spark_warehouse", ignore_errors=True)  

    # Initialize Session
    spark = NexusSpark.get_session(run_mode)

    # Set the active catalog so all 2-part table names resolve correctly
    catalog = config['settings'].get('catalog')
    if catalog and run_mode != "local":
        spark.sql(f"USE CATALOG {catalog}")
        print(f"📦 Active catalog set to: {catalog}")

    # Skip files deleted between Auto Loader notification and read (re-seeded landing data)
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

    print("🛠️  Running Local Data Generator...")        
    try:            
        generator = NexusDataGenerator(spark)            
        # 1. Seed Clean Parquet (The 'Fast Path')
        generator.write_scenario(f"{base_path}/landing/sensors", format="parquet", corruption=0.0)

        # 2. Seed Dirty CSV (The 'Legacy Path')
        generator.write_scenario(f"{base_path}/landing/legacy_sales", format="csv", corruption=0.0)

        # 3. Seed Nested JSON (The 'Complex Path')
        generator.write_scenario(f"{base_path}/landing/store_streams", format="json", nested=False, corruption=0.0)

            
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
        processor_class = processor_map.get(table_cfg['type'], None)
        
        if processor_class is None:
            print(f"⚠️  No processor found for type '{table_cfg['type']}' in table '{table_cfg['name']}'")
            continue

        # Instantiate and Run
        instance = processor_class(spark, table_cfg, run_mode, base_path)
        instance.process()

if __name__ == "__main__":
    main()
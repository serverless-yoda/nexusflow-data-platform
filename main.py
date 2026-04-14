# main.py
import yaml
import os
import shutil
from datetime import datetime
from src.orchestration.engine import NexusEngine
from src.common.data_generator import NexusDataGenerator


# Define the paths to clear
paths_to_reset = [
    "./data/silver",
    "./data/gold",  
    "./data/quarantine",
    "./checkpoints"  # CRITICAL: Always clear checkpoints when clearing data!
]
def main():
    with open("conf/pipeline_manifest.yml", "r") as f:
        config = yaml.safe_load(f)

    run_mode = os.getenv("NEXUS_RUN_MODE", config['settings'].get('run_mode', 'local'))
    local_root = os.path.abspath(config['settings'].get('local_base_path', './data'))
    
    if run_mode == "local":
        for path in paths_to_reset:
            if os.path.exists(path):
                print(f"🧹 Clearing {path}...")
                shutil.rmtree(path)
                
    engine = NexusEngine(config=config,run_mode=run_mode, local_root=local_root)
    generator = NexusDataGenerator(engine.spark)

    storage_root = config['settings'].get('storage_root', 'dbfs:/nexusflow')
    catalog = config['settings'].get('catalog', 'nff_catalog')
    for table in config['tables']:
        # --- SEED DATA FOR TESTING ---
        if config['settings'].get('seed_data', False) and 'source_path' in table:
            number_of_records = config['settings'].get('number_of_records', 5)
            landing_path = engine.resolve_path(table['source_path'])
            
            # Create a unique filename using microseconds to avoid collisions
            timestamp = datetime.now().strftime('%H%M%S_%f')
            file_name = f"batch_{timestamp}.json"
            
            target = os.path.join(landing_path, file_name) if run_mode == "local" else f"{storage_root}" + landing_path
            print(target)
            generator.write_to_landing(target, num_records=number_of_records, run_mode=run_mode)

        # --- RUN PRODUCTION PIPELINE ---
        if table['type'] == "silver":
            print(f"🥈 Starting Silver Pipeline: {table['name']}")
            engine.run_silver(table, storage_root=storage_root, catalog=catalog)
        elif table['type'] == "gold":
            print(f"🏆 Starting Gold Pipeline: {table['name']}")
            engine.run_gold(table, storage_root=storage_root,catalog=catalog)
        else:
            print(f"🥈 Starting Bronze Pipeline: {table['name']}")
            engine.run_bronze(table, storage_root=storage_root, catalog=catalog)

            
            
    print("🏁 NexusFlow Orchestration Complete.")

if __name__ == "__main__":
    main()
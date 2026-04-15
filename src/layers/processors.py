import os
import pyspark.sql.functions as F
from src.interfaces.processor import IProcessor
from src.core.transformer_factory import TransformerFactory

class BaseProcessor:
    def __init__(self, spark, table_cfg, run_mode, base_path):
        self.spark = spark
        self.cfg = table_cfg
        self.run_mode = run_mode
        self.base_path = base_path
        
        # Path Resolution
        raw_path = table_cfg.get('external_location', f"/{table_cfg['name']}")
        #self.target_path = os.path.abspath(os.path.join(base_path, raw_path.lstrip("/")))
        #self.checkpoint = os.path.abspath(os.path.join(base_path, "checkpoints", table_cfg['name']))
        
        self.checkpoint = os.path.abspath(
            os.path.join(base_path, "checkpoints", self.cfg['type'], self.cfg['name'])
        )
        ext_loc = table_cfg.get('external_location', f"/{table_cfg['name']}")
        self.target_path = os.path.abspath(os.path.join(base_path, ext_loc.lstrip("/")))

        # --- NEW: Schema Provisioning ---
        self._ensure_schema_exists()

    def _ensure_schema_exists(self):
        """
        Extracts the schema name from 'bronze.universal_raw' 
        and creates it if it doesn't exist.
        """
        table_name = self.cfg.get('target_table')
        if "." in table_name:
            schema_name = table_name.split(".")[0]
            print(f"🛠️ Ensuring schema '{schema_name}' exists...")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

class BronzeProcessor(BaseProcessor, IProcessor):
    def process(self):
        # 1. Robust Path Resolution
        # Ensure we remove leading slashes to prevent os.path.join issues
        clean_source = self.cfg['source_path'].lstrip("/")
        source = os.path.abspath(os.path.join(self.base_path, clean_source))
        
        print(f"🔍 Checking source path: {source}")
        
        # 2. Defensive Check: Auto-create landing zone if it doesn't exist
        # Streaming will fail immediately if the source path is missing
        if not os.path.exists(source):
            print(f"⚠️ Warning: Source path {source} not found. Creating it...")
            os.makedirs(source, exist_ok=True)

        # 3. Read Raw Payloads
        # We use 'text' format for the Universal Bronze pattern
        df = (self.spark.readStream
              .format("text")
              .load(source)
              .select(
                  F.col("value").alias("payload"),
                  F.current_timestamp().alias("ingested_at"),
                  F.input_file_name().alias("source_file"),
                  # Extract extension (json, csv, xml)
                  F.element_at(F.split(F.input_file_name(), "\."), -1).alias("file_format")
              ))

        # 4. Write to Delta (Universal Bronze Table)
        print(f"📥 Streaming to {self.cfg['target_table']}...")
        query = (df.writeStream
         .format("delta")
         .option("checkpointLocation", self.checkpoint)
         .option("path", self.target_path) 
         .trigger(availableNow=True)
         .toTable(self.cfg['target_table']))

        query.awaitTermination()

class SilverProcessor(BaseProcessor, IProcessor):
    def process(self):
        transformer = TransformerFactory.get_transformer(self.cfg)
        source_table = self.cfg['source_table'] # "bronze.universal_raw"

        if not self.spark.catalog.tableExists(source_table):
            print(f"❌ Silver Error: Source {source_table} not found in catalog.")
            return

        stream = self.spark.readStream.table(source_table)

        def micro_batch(batch_df, batch_id):
            processed = transformer.transform(batch_df).cache()
            
            # 1. Atomic split: Valid vs Quarantine
            valid_df = processed.filter("is_valid").drop("is_valid")
            quarantine_df = processed.filter("!is_valid")

            # 2. Save and Register Valid Data
            (valid_df.write.format("delta")
                .mode("append")
                .option("path", self.target_path) # Directs physical storage
                .saveAsTable(self.cfg['target_table'])) # Links to 'silver.transactions'
            
            # 3. Save Quarantine (usually as a separate path/table)
            (quarantine_df.write.format("delta")
                .mode("append")
                .save(self.target_path + "_quarantine"))

            processed.unpersist()

        (stream.writeStream.foreachBatch(micro_batch).option("checkpointLocation", self.checkpoint)
         .trigger(availableNow=True).start().awaitTermination())

class GoldProcessor(BaseProcessor, IProcessor):
    def process(self):
        source_table = self.cfg['source_table']
        
        # 1. Defensive Check: Does the source exist?
        if not self.spark.catalog.tableExists(source_table):
            print(f"⚠️ Gold Layer Skip: Source table {source_table} not found yet. "
                  "This is normal if Silver hasn't processed data yet.")
            return

        # 2. Proceed with transformation
        transformer = TransformerFactory.get_transformer(self.cfg)
        
        print(f"🥇 Generating Gold Layer: {self.cfg['target_table']}...")
        df = self.spark.read.table(source_table)
        
        gold_df = transformer.transform(df)
        
        # 3. Write to Gold
        (gold_df.write
         .format("delta")
         .mode("overwrite")
         .option("path", self.target_path)
         .saveAsTable(self.cfg['target_table'])
         )
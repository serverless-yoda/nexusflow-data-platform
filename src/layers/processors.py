# src/layers/processors.py
import os
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, LongType, TimestampType
from src.interfaces.processor import IProcessor
from src.core.transformer_factory import TransformerFactory
from src.common.path_resolver import PathResolver

# Explicit schema for binaryFile format to override stale Auto Loader schema caches
_BINARY_FILE_SCHEMA = StructType([
    StructField("path", StringType()),
    StructField("modificationTime", TimestampType()),
    StructField("length", LongType()),
    StructField("content", BinaryType()),
])

class BaseProcessor:
    def __init__(self, spark, table_cfg, run_mode, base_path):
        self.spark = spark
        self.cfg = table_cfg
        self.run_mode = run_mode
        self.base_path = base_path
        
        # Resolve the Target Path (Where the Delta data lives)
        target_suffix = table_cfg.get('external_location', f"/{table_cfg['name']}")
        self.target_path = PathResolver.resolve(base_path, target_suffix, run_mode)
        
        # Resolve the Checkpoint Path
        checkpoint_suffix = os.path.join("checkpoints", self.cfg['type'], self.cfg['name'])
        self.checkpoint = PathResolver.resolve(base_path, checkpoint_suffix, run_mode)

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
        source = PathResolver.resolve(self.base_path, self.cfg['source_path'], self.run_mode)
        fmt = self.cfg.get('format', 'json').lower()

        if self.run_mode != "local":
            # --- CLOUD: Use Auto-Loader in binary mode for the Universal Raw pattern ---
            df = (
                    self.spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "binaryFile")
                        .option("cloudFiles.schemaLocation", f"{self.checkpoint}/_schema")
                        .schema(_BINARY_FILE_SCHEMA)
                        .load(source)
                        .select(
                            F.col("content").cast("string").alias("payload"),
                            F.element_at(F.split(F.col("path"), r"\."), -1).alias("file_format")
                        )
                  )
        else:
            # --- LOCAL: Use Standard Spark Stream ---
            print(f'✅ Processing in LOCAL mode with format: {fmt}')
            if fmt == "parquet":
                # Direct structured read
                df = self.spark.readStream.format("parquet").load(source)
            else:
                # Binary read for JSON/CSV to maintain the "Universal" payload pattern
                df = (
                        self.spark.readStream
                            .format("binaryFile")
                            .option('recursiveFileLookup', 'true')
                            .load(source)
                            .select(
                                F.col("content").cast("string").alias("payload"),
                                F.element_at(F.split(F.col("path"), r"\."), -1).alias("file_format")
                            )
                      )
                
        # Common Metadata for both modes
        df = df.select("*", 
                       F.current_timestamp().alias("ingested_at"),
                       F.input_file_name().alias("source_file"))

        # # 2. Unified Write
        query = (
                    df.writeStream
                        .format("delta")
                        .option("mergeSchema", "true") 
                        .option("checkpointLocation", self.checkpoint)
                        .trigger(availableNow=True)
                        .toTable(self.cfg['target_table'])
                )

        query.awaitTermination()
        print(f"✅ Bronze processing completed for {self.cfg['name']}. Table name is {self.cfg['target_table']}")

class SilverProcessor(BaseProcessor, IProcessor):

    def process(self):
        #print(f"DEBUG: Config for transformer: {self.cfg}")
        transformer = TransformerFactory.get_transformer(self.spark, self.cfg)
        source_table = self.cfg['source_table']

        # 1. Protective check for source existence
        if not self.spark.catalog.tableExists(source_table):
            print(f"❌ Silver Error: Source {source_table} not found.")
            return

        stream = self.spark.readStream.table(source_table)

        def micro_batch(batch_df, batch_id):
            # Apply the Strategy (JSON parsing, Exploding, Quality Tagging)
            processed = transformer.transform(batch_df).cache()
            
            # --- ROUTING LOGIC ---
            
            # 1. Valid Records: Ready for business
            valid_df = processed.filter("is_valid == true").drop("is_valid")
            
            # 2. Quarantine Records: Failed rules (e.g., amount < 0)
            quarantine_df = processed.filter("is_valid == false")

            # Save Valid Data
            (
                valid_df
                    .write
                    .format("delta")
                        .mode("append")
                        #.option("path", self.target_path) 
                        .saveAsTable(self.cfg['target_table'])
            )

            # Save Quarantine Data 
            # We append a suffix to the path to keep it isolated
            (
                quarantine_df
                    .write.format("delta")
                        .mode("append")
                        #.option("path", f"{self.target_path}_quarantine")
                        .saveAsTable(f"{self.cfg['target_table']}_quarantine")
            )
            
            print(f'✅ Silver Batch {batch_id} processed: {valid_df.count()} valid records for {self.cfg["target_table"]}, {quarantine_df.count()} quarantined records for {self.cfg["target_table"]}_quarantine.')
            
            # Clear cache for the next batch
            processed.unpersist()

        # Start the streaming query
        (
            stream.writeStream
            .foreachBatch(micro_batch)
            .option("checkpointLocation", self.checkpoint)
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )

class GoldProcessor(BaseProcessor, IProcessor):
    def process(self):
        source_table = self.cfg['source_table'] # e.g., "silver.transactions"
        
        if not self.spark.catalog.tableExists(source_table):
            print(f"⚠️ Gold Skip: {source_table} not available.")
            return

        print(f"🥇 Generating Regional Gold Report: {self.cfg['target_table']}...")
        
        # 1. Read validated data
        #print(f"DEBUG: Config for transformer: {self.cfg}")
        transformer = TransformerFactory.get_transformer(self.spark, self.cfg)
        silver_df = self.spark.read.table(source_table)
        
        # 2. Transform into KPIs
        gold_df = transformer.transform(silver_df)
        
        # 3. Write as an optimized Gold Table
        (
            gold_df
                .write
                .format("delta")
                .mode("overwrite")  # Full refresh for reporting accuracy
                #.option("path", self.target_path)
                .saveAsTable(self.cfg['target_table'])
        )
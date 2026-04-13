# src/orchestration/engine.py
import os
from pyspark.sql import functions as F
from src.common.spark_session import NexusSpark
from src.transformation.quality_rules import QualityRules
from src.transformation.silver_transformer import SilverTransformer
from src.transformation.gold_transformer import GoldTransformer

class NexusEngine:
    def __init__(self, config, run_mode="local", local_root=""):
        self.config = config
        self.run_mode = run_mode
        self.local_root = local_root
        if run_mode == "local":
            print(f"Local Mode: Using local_root={local_root}")
            self.spark = NexusSpark(run_mode).get_session()
        else:
            self.spark = NexusSpark(run_mode).getActiveSession()
    def resolve_path(self, path):
        if self.run_mode == "local" and not path.startswith("abfss"):
            return os.path.join(self.local_root, path.lstrip("/"))
        return path
    
    def provision_layer(self, layer_name: str):
        if self.run_mode == "local":
            print(f"Local Mode: Skipping DDL provisioning for {layer_name}.")
            return
    
        settings = self.config['settings']
        layer_cfg = self.config['layers'][layer_name]
        
        mapping = {
            "${catalog}": settings['catalog'],
            "${storage_root}": settings['storage_root'],
            f"${{{layer_name}_schema}}": layer_cfg['schema'],
            f"${{{layer_name}_path}}": layer_cfg['path']
        }

        sql_path = f"src/sql/ddl/create_{layer_name}_txt.sql"
        with open(sql_path, "r") as f:
            sql_template = f.read()
        
        for placeholder, value in mapping.items():
            sql_template = sql_template.replace(placeholder, value)

        print(f"🏗️  Provisioning {layer_name} layer in {settings['catalog']}...")
        for statement in sql_template.split(";"):
            if statement.strip():
                self.spark.sql(statement)

    def resolve_table_name(self, full_table_name: str) -> str:
        if self.run_mode == "local":
            parts = full_table_name.split(".")
            if len(parts) >= 2:
                return f"{parts[-2]}_{parts[-1]}"
            return full_table_name
        return full_table_name

    def run_silver(self, table_cfg):
        #print(table_cfg)
        source = self.resolve_path(table_cfg['source_path'])
        _checkpoint = self.resolve_path(f"Volumes/checkpoints/{table_cfg['name']}")
        checkpoint =  self.resolve_path(f"{table_cfg['check_point']}")
        #print(checkpoint)
        
        target_table = self.resolve_table_name(table_cfg['target_table'])
        quarantine_table = self.resolve_table_name(table_cfg['target_quarantine'])
        
        transformer = SilverTransformer(self.spark)

        if self.run_mode == "local":
            parts = target_table.split(".")
            if len(parts) > 1:
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {parts[0]}")
            
            data_schema = self.spark.read.json(source).schema
            raw_stream = (self.spark.readStream
                .format("json")
                .schema(data_schema)
                .load(source))
        else:
            raw_stream = (self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
                .load(source))

        def micro_batch_sink(batch_df, batch_id):
            if batch_df.isEmpty(): return
            
            processed_df = transformer.clean_transactions(batch_df, table_cfg['rules_method'])
            
            target_path = self.resolve_path(table_cfg['target_path'])
            quarantine_path = self.resolve_path(table_cfg.get('target_quarantine_path', f"/quarantine/{table_cfg['name']}"))

            (processed_df.filter("is_valid = true").drop("is_valid") 
                .write.format("delta").mode("append") 
                .option("path", target_path).saveAsTable(target_table))
                
            # (processed_df.filter("is_valid = false") 
            #     .write.format("delta").mode("append") 
            #     .option("path", quarantine_path).saveAsTable(quarantine_table))

        query = (raw_stream.writeStream
            .foreachBatch(micro_batch_sink)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start())
        
        query.awaitTermination()
        self._apply_storage_optimization(target_table, table_cfg['cluster_by'])

    def run_gold(self, table_cfg):
        print(f"🏆 Processing Gold Layer: {table_cfg['name']}")
        
        source_table = self.resolve_table_name(table_cfg['source_table'])
        target_table = self.resolve_table_name(table_cfg['target_table'])
        target_path = self.resolve_path(table_cfg['target_path'])
        target_table = self.resolve_table_name(table_cfg['target_table'])

        silver_df = self.spark.read.table(source_table)
        
        gold_tool = GoldTransformer(self.spark)
        gold_df = gold_tool.calculate_regional_kpis(silver_df)
        
        gold_df.write.format("delta") \
            .mode("overwrite") \
            .option("path", target_path) \
            .saveAsTable(target_table)
        
        self._apply_storage_optimization(target_table, table_cfg['cluster_by'])

    def _apply_storage_optimization(self, table_name, cluster_cols):
        resolved_name = self.resolve_table_name(table_name)
        cols = ", ".join(cluster_cols)
        
        if self.run_mode == "local":
            print(f"⏩ Skipping 'ALTER CLUSTER BY' for {resolved_name} (Local limitations).")
            print(f"🪄 Running OPTIMIZE on {resolved_name}...")
            self.spark.sql(f"OPTIMIZE {resolved_name}")
        else:
            print(f"🪄 Applying Liquid Clustering to {resolved_name}...")
            self.spark.sql(f"ALTER TABLE {resolved_name} CLUSTER BY ({cols})")
            self.spark.sql(f"OPTIMIZE {resolved_name}")
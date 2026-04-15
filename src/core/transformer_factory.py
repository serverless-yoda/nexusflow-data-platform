# src/core/transformer_factory.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.interfaces.processor import ITransformer

class SilverTransformerStrategy(ITransformer):
    def __init__(self, spark, cfg):
        # This line is critical!
        self.spark = spark 
        self.target_schema = cfg.get('schema')
        if self.target_schema is None:
            print(f'⚠️ WARNING: No schema defined for {cfg.get("name")}.Table name is {cfg.get("target_table")}. This may lead to processing errors.')
            raise ValueError(f"Schema is missing in configuration for table: {cfg.get('table_name')}")

        self.fmt = cfg.get('format', 'json')
        self.rules = cfg.get('rules', [])
        self.explode_col = cfg.get('explode_col')


    def transform(self, df: DataFrame) -> DataFrame:
        
        if self.target_schema is None:
            # Fallback to avoid PySparkTypeError
            return df
    
        # 1. Identify what we are dealing with
        has_payload = "payload" in df.columns
        
        # Filter for the specific format we want to process in this instance
        df_filtered = df.filter(F.col("file_format") == self.fmt)

        # 2. Extract the data into a structured format (processed_df)
        if self.fmt == "parquet":
            if has_payload:
                # LOCAL RE-READ LOGIC
                # We collect the distinct file paths that were ingested in this micro-batch
                rows = df_filtered.select("source_file").distinct().collect()                
                paths = [row.source_file for row in rows]                
                if not paths:
                    # Just return the current empty batch but with the schema applied
                    return self.spark.createDataFrame([], self.target_schema).withColumn("is_valid", F.lit(True))
                clean_paths = [
                    p.replace("file:///", "").replace("%20", " ")   
                    for p in paths
                ]  
                             
                processed_df = self.spark.read.schema(self.target_schema).parquet(*clean_paths)
            else:
                # CLOUD LOGIC: Auto-loader already unpacked the columns
                processed_df = df_filtered
                
        elif self.fmt == "json":
            if has_payload:
                processed_df = (df_filtered.withColumn("data", F.from_json(F.col("payload"), self.target_schema))
                                .select("data.*", "ingested_at", "source_file"))
            else:
                processed_df = df_filtered

        elif self.fmt == "csv":
            if has_payload:
                processed_df = (df_filtered.withColumn("data", F.from_csv(F.col("payload"), self.target_schema))
                                .select("data.*", "ingested_at", "source_file"))
            else:
                processed_df = df_filtered
        else:
            raise ValueError(f"Unsupported format: {self.fmt}")

        # 3. Handle Nesting
        if self.explode_col and self.explode_col in processed_df.columns:
            processed_df = (processed_df
                            .withColumn("exploded", F.explode_outer(F.col(self.explode_col)))
                            .select("*", "exploded.*")
                            .drop(self.explode_col, "exploded"))

        # 4. Apply Quality Rules
        # Now that processed_df is structured, 'amount' will be resolved!
        quality_expr = " AND ".join(self.rules) if self.rules else "1=1"
        
        # Final safety: if for some reason 'amount' is still missing, 
        # this will give a clearer error than the AnalysisException
        return processed_df.withColumn("is_valid", F.expr(quality_expr))
            
class GoldTransformerStrategy(ITransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Aggregates Silver transactions into Regional KPI insights.
        """
        return (df.groupBy("region")
                .agg(
                    F.sum("amount").alias("total_revenue"),
                    F.count("tx_id").alias("transaction_count"),
                    F.avg("amount").alias("avg_ticket_size"),
                    F.max("tx_time").alias("last_transaction_at")
                )
                # Add a reporting metadata column
                .withColumn("report_generated_at", F.current_timestamp())
                .orderBy(F.desc("total_revenue")))
    
class TransformerFactory:
    @staticmethod
    def get_transformer(spark, table_cfg):
        t_type = table_cfg['type']
        
        if t_type == "silver":
            return SilverTransformerStrategy(spark, table_cfg)
            
        if t_type == "gold":
            return GoldTransformerStrategy()
            
        raise ValueError(f"No transformer found for layer type: {t_type}")
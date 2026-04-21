# src/core/transformer_factory.py
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.interfaces.processor import ITransformer
from pyspark.sql.types import StructType, StructField, StringType

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


    def process_dynamic_payload(self,df):
        # 1. Split into lines
        split_regex = r"\\r\\n|[\r\n]+"
        df_lines = df.withColumn("raw_line", F.explode(F.split(F.col("payload"), split_regex))) \
                    .withColumn("raw_line", F.trim(F.col("raw_line"))) \
                    .filter(F.col("raw_line") != "")

        # 2. Extract the header from the first row of the first file 
        # (Note: In a production stream, you'd usually peek at the first record)
        header_raw = df_lines.filter(F.col("raw_line").contains(",")).first()["raw_line"]
        column_names = header_raw.split(",")

        # 3. Dynamically build the schema based on those names
        # We use StringType by default; you can add logic to cast types later
        dynamic_schema = StructType([StructField(name, StringType(), True) for name in column_names])

        # 4. Process the data, skipping the header line
        return (
            df_lines
            .filter(~F.col("raw_line").startswith(header_raw))
            .withColumn("data", F.from_csv(F.col("raw_line"), dynamic_schema.simpleString(), {"sep": ","}))
            .select("data.*", "ingested_at", "source_file")
        )

    def transform(self, df: DataFrame) -> DataFrame:
        
        if self.target_schema is None:
            # Fallback to avoid PySparkTypeError
            return df
        #df.printSchema()  # Debug: Check incoming schema
        # 1. Identify what we are dealing with
        has_payload = "payload" in df.columns
        
        # Filter for the specific format we want to process in this instance
        print(f"🔍 Detected format: {self.fmt}. Has payload: {has_payload}")
        df_filtered = df.filter(F.col("file_format") == self.fmt)
        #df_filtered.show(5, truncate=False)  # Debug: Check filtered data
        # 2. Extract the data into a structured format (processed_df)
        if self.fmt == "parquet":
            if has_payload:
                # LOCAL RE-READ LOGIC
                # We collect the distinct file paths that were ingested in this micro-batch
                rows = df_filtered.select("source_file").distinct().collect()   
                #print(rows)             
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
                processed_df = self.process_dynamic_payload(df_filtered)
            else:
                print(f'⚠️ Processing CSV without payload. This is unexpected for CSV format. ')
                processed_df = df_filtered
        else:
            raise ValueError(f"Unsupported format: {self.fmt}")

        # 3. Handle Nesting
        if self.explode_col and self.explode_col in processed_df.columns:
            processed_df = (processed_df
                            .withColumn("exploded", F.explode_outer(F.col(self.explode_col)))
                            .select("*", "exploded.*")
                            .drop(self.explode_col, "exploded"))
            processed_df.show(5, truncate=False)

        # 4. Apply Quality Rules
        # Now that processed_df is structured, 'amount' will be resolved!
        quality_expr = " AND ".join(self.rules) if self.rules else "1=1"
        
        # Final safety: if for some reason 'amount' is still missing, 
        # this will give a clearer error than the AnalysisException
        new_df_processed = processed_df.withColumn("is_valid", F.expr(quality_expr))
        return new_df_processed
            
class GoldTransformerStrategy(ITransformer):
    def __init__(self, table_cfg, spark):
        self.spark = spark
        self.group_by = table_cfg.get('group_by', [])
        self.aggs = table_cfg.get('aggregations', {})
        self.joins = table_cfg.get('joins', [])
        self.sort_by = table_cfg.get('sort_by')

    def transform(self, df: DataFrame) -> DataFrame:
        output_df = df

        for j in self.joins:
            target_join_table = j.get('table')
            join_key = j.get('on')
            join_type = j.get('type', 'inner')

            if self.spark.catalog.tableExists(target_join_table):
                # 1. Read the right-hand table
                right_df = self.spark.read.table(target_join_table)
                
                # 2. THE SLEDGEHAMMER: Find overlapping columns (except the join key)
                # This prevents [AMBIGUOUS_REFERENCE] because only one version of 
                # 'customer_id' (or any other shared col) will exist after this.
                duplicate_cols = [c for c in right_df.columns if c in output_df.columns and c != join_key]
                right_df = right_df.drop(*duplicate_cols)

                print(f"🔗 Joining {target_join_table} on {join_key} (Dropped duplicates: {duplicate_cols})")

                # 3. Perform the join using the string-based 'on' parameter
                # We pass the string directly, NOT in a list [].
                output_df = output_df.join(right_df, on=join_key, how=join_type)
            else:
                print(f"⚠️ Table {target_join_table} not found.")

        # --- AGGREGATION ---
        if self.group_by:
            # Wrap strings in F.col to be safe
            group_cols = [F.col(c) for c in self.group_by]
            
            # Create aggregation expressions from YAML strings
            agg_exprs = [F.expr(expr).alias(alias) for alias, expr in self.aggs.items()]
            
            output_df = output_df.groupBy(*group_cols).agg(*agg_exprs)

        if self.sort_by:
            output_df = output_df.orderBy(F.desc(self.sort_by))

        return output_df
                    
class TransformerFactory:
    @staticmethod
    def get_transformer(spark, table_cfg):
        t_type = table_cfg['type']
        
        if t_type == "silver":
            return SilverTransformerStrategy(spark, table_cfg)
            
        if t_type == "gold":
            return GoldTransformerStrategy(table_cfg, spark)
            
        raise ValueError(f"No transformer found for layer type: {t_type}")
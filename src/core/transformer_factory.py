from pyspark.sql import functions as F
from src.interfaces.processor import ITransformer
from pyspark.sql import DataFrame

class SilverTransformerStrategy(ITransformer):
    def __init__(self, fmt, rules):
        self.fmt = fmt
        self.rules = rules

    def transform(self, df: DataFrame) -> DataFrame:
        # Filter Universal Bronze for this specific table's format
        df = df.filter(F.col("file_format") == self.fmt)
        
        # Dynamic Schema Parsing (Simplified for demo)
        if self.fmt == "json":
            df = df.withColumn("data", F.from_json("payload", "tx_id STRING, amount DOUBLE, region STRING, tx_time TIMESTAMP"))
        
        quality_expr = " AND ".join(self.rules) if self.rules else "1=1"
        return df.select("data.*").withColumn("is_valid", F.expr(quality_expr))

class GoldTransformerStrategy(ITransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return (df.groupBy("region")
                .agg(F.sum("amount").alias("total_revenue"), 
                     F.current_date().alias("report_date")))

class TransformerFactory:
    @staticmethod
    def get_transformer(table_cfg):
        t_type = table_cfg['type']
        if t_type == "silver":
            return SilverTransformerStrategy(table_cfg['format'], table_cfg['rules'])
        if t_type == "gold":
            return GoldTransformerStrategy()
        raise ValueError(f"No transformer for {t_type}")
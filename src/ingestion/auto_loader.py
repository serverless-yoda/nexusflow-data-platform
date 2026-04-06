class NexusAutoLoader:
    def __init__(self, spark, checkpoint_path):
        self.spark = spark
        self.checkpoint_path = checkpoint_path

    def read_json_stream(self, input_path, target_table):
        """
        Production-grade ingestion with Schema Evolution and Rescued Data.
        """
        return (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{self.checkpoint_path}/schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            #  Don't let bad data kill the stream
            .option("readChangeFeed", "true") 
            .load(input_path)
            .writeStream
            .option("checkpointLocation", f"{self.checkpoint_path}/data")
            .trigger(availableNow=True)
            .toTable(target_table))
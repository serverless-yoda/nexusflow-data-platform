# src/ingestion/autoloader.py
from src.common.spark_session import NexusSpark
from src.common.secrets import NexusSecrets
from src.common.table_factory import NexusTableFactory

class NexusAutoLoader:
    def __init__(self, source_path: str, target_table: str):
        self.spark = NexusSpark()
        self.secrets = NexusSecrets(self.spark)
        self.factory = NexusTableFactory(self.spark)
        self.source_path = source_path
        self.target_table = target_table

    def run_ingestion(self):
        """
        Executes an incremental 'Trigger Once' load. 
        Perfect for scheduled jobs in Phase 7.
        """
        # 1. Configure Storage Access using our Secrets Manager
        creds = self.secrets.get_adls_config()
        self.spark.conf.set(f"fs.azure.account.auth.type", "OAuth")
        self.spark.conf.set(f"fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set(f"fs.azure.account.oauth2.client.id", creds['client_id'])
        self.spark.conf.set(f"fs.azure.account.oauth2.client.secret", creds['client_secret'])
        self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{creds['tenant_id']}/oauth2/token")

        # 2. Build the Auto Loader Stream
        df_stream = (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"dbfs:/nexus/checkpoints/{self.target_table}/schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(self.source_path))

        # 3. Write via Micro-Batch (Integrates with TableFactory)
        def process_batch(batch_df, batch_id):
            if not batch_df.isEmpty():
                # We use the factory to ensure Liquid Clustering & Governance
                schema, table = self.target_table.split(".")
                self.factory.create_managed_table(batch_df, schema, table, cluster_by=["region"])

        query = (df_stream.writeStream
            .trigger(availableNow=True) # 2026 best practice for cost-efficiency
            .option("checkpointLocation", f"dbfs:/nexus/checkpoints/{self.target_table}/data")
            .foreachBatch(process_batch)
            .start())
        
        query.awaitTermination()
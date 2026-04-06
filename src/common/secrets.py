class SecretProvider:
    def __init__(self, dbutils, spark, scope='nexus-scope', key = 'storage-primary-key'):
        self.dbutils = dbutils
        self.scope = scope
        self.key = key
        self.spark = spark

    def get_secret(self):
        return self.dbutils.secrets.get(scope=self.scope, key=self.key)
    
    def apply_storage_configuration(self, account_name):
        key = self.get_secret()
        self.spark.conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", key)
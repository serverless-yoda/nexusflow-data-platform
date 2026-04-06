class DeltaTableFactory:
    def __init__(self, spark):
        self.spark = spark

    def create_high_performance_table(self, table_name, schema, cluster_keys):
        """
        Create a delta table with 2026-standard optimization
        """  
        (
            self.spark
                .catalog
                .createTable(table_name, schema=schema,source="delta")
                .executeCommand(
                    f"""
                    ALTER TABLE {table_name}
                    SET TBLPROPERTIES (
                        'delta.enableDeletionVectors' = 'true',
                        'delta.feature.allowColumnDefaults' = 'supported'
                    )
                    """
                )
        )

        # apply liquid clustering
        self.spark.sql(
            f"""
            ALTER TABLE {table_name}
            CLUSTER BY ({','.join(cluster_keys)})
            """
        )
        print(f"✅ Table {table_name} created with Liquid Clustering on {cluster_keys}")

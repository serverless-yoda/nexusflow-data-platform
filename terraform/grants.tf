# terraform/grants.tf

/* NEXUSFLOW RBAC MODEL
  Standard: Developers have 'USE' on Prod, 'ALL' on Dev.
  Standard: Service Principals own the Data (CRUD).
*/

# 1. Granting Catalog Usage to the Engineering Group
resource "databricks_grant" "catalog_usage" {
  catalog = databricks_catalog.nff_catalog.name

  principal  = "nff_engineering_team" # Group synced from Entra ID
  privileges = ["USE_CATALOG", "USE_SCHEMA"]
}

# 2. Bronze Layer: Read-Only for Humans, Write for Service Principal
resource "databricks_grant" "bronze_access" {
  schema = databricks_schema.medallion_layers["bronze"].id

  grant {
    principal  = "nff_engineering_team"
    privileges = ["SELECT", "READ_METADATA"]
  }

  grant {
    principal  = var.service_principal_name # The 'Pipeline' identity
    privileges = ["ALL_PRIVILEGES"] 
  }
}

# 3. Governance Layer: Restricted to Data Stewards
resource "databricks_grant" "governance_access" {
  schema = databricks_schema.medallion_layers["governance"].id

  grant {
    principal  = "nff_data_stewards"
    privileges = ["SELECT", "EXECUTE"] # Required to run Masking Functions
  }
}
# 1. Register the 'Keycard' (Storage Credential)
# This tells Databricks to use the Managed Identity we built in main.tf
resource "databricks_storage_credential" "external" {
  name = "nexus-storage-credential-${random_id.suffix.hex}"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.unity_connector.id
  }
}

# 2. Register the 'Parking Space' (External Location)
# This validates that Databricks is allowed to talk to your specific Azure Container
resource "databricks_external_location" "this" {
  name            = "nexus_external_location-${random_id.suffix.hex}"
  url             = "abfss://${azurerm_storage_container.data_container.name}@${azurerm_storage_account.nexus_storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external.name
  comment         = "Storage for NexusFlow Medallion layers"
}

# 3. Create the Catalog (The 'Library')
resource "databricks_catalog" "nff_catalog" {
  name           = "nff_catalog_${var.env}" 
  comment        = "Main catalog for NexusFlow Medallion Architecture"
  
  # Now that the External Location exists, this URL will be accepted!
  storage_root   = databricks_external_location.this.url
  
  force_destroy  = true
}

# 4. Create the Schemas (Bronze, Silver, Gold)
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.nff_catalog.id
  name         = "bronze_raw"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.nff_catalog.id
  name         = "silver_refined"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.nff_catalog.id
  name         = "gold_analytics"
}

# 5. Create the Secret Scope
resource "databricks_secret_scope" "nexus_scope" {
  name = "nexusflow-scope"
  
  keyvault_metadata {
    resource_id = azurerm_key_vault.nexus_kv.id
    dns_name    = azurerm_key_vault.nexus_kv.vault_uri
  }
}
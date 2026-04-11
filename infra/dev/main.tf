# This creates a 4-character random hex string (e.g., "a1b2")
resource "random_id" "suffix" {
  byte_length = 2
}

# 1. Create a Resource Group (The 'Project Folder')
resource "azurerm_resource_group" "nexus_rg" {
  # This will become 'rg-nexusflow-dev' or 'rg-nexusflow-prod'
  name     = "rg-nexusflow-${var.env}" 
  location = "westus2"
}

resource "azurerm_storage_account" "nexus_storage" {
  # Storage names can't have dashes, so we just squash it together
  # Result: 'nffstoragedev'
  name                     = "nexus${random_id.suffix.hex}storage${var.env}"
  resource_group_name      = azurerm_resource_group.nexus_rg.name
  location                 = azurerm_resource_group.nexus_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

# 3. Create the 'data' container (The 'Drawer' inside the Hard Drive)
resource "azurerm_storage_container" "data_container" {
  name                  = "data"
  storage_account_name  = azurerm_storage_account.nexus_storage.name
  container_access_type = "private"
}

# 4. The Digital Safe (Key Vault) from your secret_map.yml
resource "azurerm_key_vault" "nexus_kv" {
  name                = "nexus-flow-${random_id.suffix.hex}-kv-${var.env}"
  resource_group_name = azurerm_resource_group.nexus_rg.name
  location            = azurerm_resource_group.nexus_rg.location
  tenant_id           = var.azure_tenant_id
  sku_name            = "standard"
  enable_rbac_authorization = true
}


# Create the Databricks Workspace (The Clubhouse)
resource "azurerm_databricks_workspace" "nexus_workspace" {
  name                = "dbx-nexus-${random_id.suffix.hex}-${var.env}"
  resource_group_name = azurerm_resource_group.nexus_rg.name
  location            = azurerm_resource_group.nexus_rg.location
  sku                 = "premium" # Premium is needed for Unity Catalog!

  tags = {
    Environment = "Development"
  }
}

# The 'Keycard' for Databricks
resource "azurerm_databricks_access_connector" "unity_connector" {
  name                = "ext-access-connector-${random_id.suffix.hex}-${var.env}"
  resource_group_name = azurerm_resource_group.nexus_rg.name
  location            = azurerm_resource_group.nexus_rg.location

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "storage_data_contributor" {
  scope                = azurerm_storage_account.nexus_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity_connector.identity[0].principal_id
}

# Get the details of the person currently running Terraform (You!)
data "azurerm_client_config" "current" {}

# Give YOU permission to see the secrets
resource "azurerm_role_assignment" "vault_admin" {
  scope                = azurerm_key_vault.nexus_kv.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

# This gives Databricks permission to READ secrets from your Vault
resource "azurerm_role_assignment" "databricks_vault_access" {
  scope                = azurerm_key_vault.nexus_kv.id
  role_definition_name = "Key Vault Secrets User"
  
  # This is the 'Keycard' you built for Databricks
  principal_id         = azurerm_databricks_access_connector.unity_connector.identity[0].principal_id
}
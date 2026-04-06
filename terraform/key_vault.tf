# terraform/key_vault.tf

resource "azurerm_key_vault" "nff_kv" {
  name                = "nexuskv-${var.env}"
  location            = azurerm_resource_group.nff_rg.location
  resource_group_name = azurerm_resource_group.nff_rg.name
  sku_name            = "standard"
  tenant_id           = data.azurerm_client_config.current.tenant_id
}

# Granting the Databricks Resource Provider access to read secrets
resource "azurerm_key_vault_access_policy" "databricks_access" {
  key_vault_id = azurerm_key_vault.nff_kv.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = var.databricks_service_principal_id

  secret_permissions = ["Get", "List"]
}
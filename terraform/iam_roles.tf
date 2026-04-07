# 1. Get the current Azure Client Config (to fetch Tenant/Subscription IDs)
data "azurerm_client_config" "current" {}

# 2. Assign 'Storage Blob Data Contributor' to the Service Principal
# This allows reading, writing, and deleting BLOBs (data), but not the account itself.
resource "azurerm_role_assignment" "nff_storage_contributor" {
  scope                = azurerm_storage_account.nff_lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.service_principal_object_id 

  # Ensures the assignment doesn't interfere with existing owner roles
  skip_service_principal_aad_check = true
}

# 3. Assign 'Key Vault Secrets User' 
# This allows the Service Principal to fetch keys for the Spark Session.
resource "azurerm_role_assignment" "nff_kv_user" {
  scope                = azurerm_key_vault.nff_kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.service_principal_object_id
}
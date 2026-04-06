# terraform/main.tf

# 1. Create the Resource Group
resource "azurerm_resource_group" "nff_rg" {
  name     = "rg-nexus-medallion-${var.env}"
  location = var.location
}

# 2. Create the Data Lake (HNS Enabled)
resource "azurerm_storage_account" "nff_lake" {
  name                     = "nexusadls${var.env}"
  resource_group_name      = azurerm_resource_group.nff_rg.name
  location                 = azurerm_resource_group.nff_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true # Essential for Delta Lake performance
}

# 3. Create the Medallion Containers
resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(["landing", "bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.nff_lake.id
}
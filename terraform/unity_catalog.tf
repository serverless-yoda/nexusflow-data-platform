# terraform/unity_catalog.tf

resource "databricks_catalog" "nff_catalog" {
  name           = "nff_${var.env}"
  storage_root   = "abfss://catalog@stnexusflow${var.env}.dfs.core.windows.net/"
  comment        = "Main Catalog for NexusFlow Financial ${var.env}"
  force_destroy  = var.env == "dev" ? true : false
}

resource "databricks_schema" "medallion_layers" {
  for_each     = toset(["bronze", "silver", "gold", "governance"])
  catalog_name = databricks_catalog.nff_catalog.name
  name         = each.key
}
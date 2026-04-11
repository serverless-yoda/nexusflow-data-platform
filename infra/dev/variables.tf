variable "azure_tenant_id" {
  description = "The Tenant ID for my Azure account"
  type        = string
}

variable "databricks_host" {
  description = "The URL of the Databricks workspace (e.g., https://adb-xxx.azuredatabricks.net)"
  type        = string
  default     = "" # We leave this empty for now
}

variable "env" {
  description = "The environment name (dev or prod)"
  type        = string
}
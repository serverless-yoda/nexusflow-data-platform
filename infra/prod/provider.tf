terraform {
  required_providers {
    # The Azure Toolkit
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    # The Databricks Toolkit
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "azurerm" {
  features {} # Required: turns on all Azure features
}

# This tells Terraform how to log into your Databricks workspace
provider "databricks" {
  host = azurerm_databricks_workspace.nexus_workspace.workspace_url
}
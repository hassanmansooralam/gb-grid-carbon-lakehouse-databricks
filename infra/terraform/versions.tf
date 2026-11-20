terraform {
  required_version = ">= 1.6"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.60"
    }
  }
}

# Authentication via standard Databricks env vars or a config profile.
provider "databricks" {}

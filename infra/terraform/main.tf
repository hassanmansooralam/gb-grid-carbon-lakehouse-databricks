locals {
  schemas = {
    raw    = "Landing zone for raw API files."
    bronze = "Raw API envelopes as append-only streaming tables."
    silver = "Flattened, validated, de-duplicated tables."
    gold   = "Business-ready materialized views."
    ml     = "Registered models and batch predictions."
  }

  env_schemas = {
    for pair in setproduct(var.environments, keys(local.schemas)) :
    "${pair[0]}.${pair[1]}" => { env = pair[0], schema = pair[1] }
  }
}

# Catalogs are created with workspace default storage and imported (see imports.tf).
resource "databricks_catalog" "this" {
  for_each = toset(var.environments)

  name    = "${var.catalog_prefix}_${each.key}"
  comment = "GB Grid Carbon Lakehouse - ${each.key}. Source: NESO Carbon Intensity API (CC BY 4.0)."
  properties = {
    project     = "gb-grid-carbon-lakehouse"
    environment = each.key
  }

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [storage_root, storage_location, connection_name, provider_name, share_name, options]
  }
}

resource "databricks_schema" "this" {
  for_each = local.env_schemas

  catalog_name  = databricks_catalog.this[each.value.env].name
  name          = each.value.schema
  comment       = local.schemas[each.value.schema]
  force_destroy = true
}

resource "databricks_volume" "landing" {
  for_each = toset(var.environments)

  catalog_name = databricks_catalog.this[each.key].name
  schema_name  = databricks_schema.this["${each.key}.raw"].name
  name         = "landing"
  volume_type  = "MANAGED"
  comment      = "Raw JSON from the extractor: <endpoint>/ingest_date=YYYY-MM-DD/*.json"
}

resource "databricks_grant" "catalog_usage" {
  for_each = var.reader_principal == "" ? toset([]) : toset(var.environments)

  catalog    = databricks_catalog.this[each.key].name
  principal  = var.reader_principal
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "gold_read" {
  for_each = var.reader_principal == "" ? toset([]) : toset(var.environments)

  schema     = databricks_schema.this["${each.key}.gold"].id
  principal  = var.reader_principal
  privileges = ["USE_SCHEMA", "SELECT"]
}

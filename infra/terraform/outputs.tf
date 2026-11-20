output "catalogs" {
  value = { for env, c in databricks_catalog.this : env => c.name }
}

output "landing_volume_paths" {
  description = "Use these as --volume-root for the extractor"
  value = {
    for env, v in databricks_volume.landing :
    env => "/Volumes/${v.catalog_name}/${v.schema_name}/${v.name}"
  }
}

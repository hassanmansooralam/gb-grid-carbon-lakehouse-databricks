variable "environments" {
  description = "One catalog per environment, named <catalog_prefix>_<env>"
  type        = list(string)
  default     = ["dev", "prod"]
}

variable "catalog_prefix" {
  description = "Catalog name prefix"
  type        = string
  default     = "carbon"
}

variable "reader_principal" {
  description = "Optional group/user to grant read access on gold (e.g. \"account users\"). Empty = skip."
  type        = string
  default     = ""
}

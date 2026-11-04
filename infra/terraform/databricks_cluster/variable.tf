variable "workspace_url" {
  type        = string
  description = "Workspace URL, passed through the TF_VAR_workspace_url environment variable."
}

variable "node_type_id" {
  type        = string
  default     = null
  description = "Override the node type. Null picks the smallest 4-core type available."
}

variable "autotermination_minutes" {
  type    = number
  default = 10
}

variable "access_connector_id" {
  type        = string
  description = "Access connector resource ID, passed through TF_VAR_access_connector_id (it contains the subscription ID, so it never goes in a file)."
}

variable "storage_account_name" {
  type        = string
  description = "ADLS Gen2 account name, passed through TF_VAR_storage_account_name."
}

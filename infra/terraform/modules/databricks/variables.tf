variable "resource_group_name" {
  type        = string
  description = "Existing resource group for the workspace and access connector."
}

variable "location" {
  type        = string
  description = "Azure region."
}

variable "workspace_name" {
  type        = string
  description = "Databricks workspace name."
}

variable "access_connector_name" {
  type        = string
  description = "Databricks access connector name."
}

variable "storage_account_id" {
  type        = string
  description = "Resource ID of the ADLS Gen2 account the connector may access."
}

variable "tags" {
  type        = map(string)
  description = "Tags applied to every resource."
}

variable "name" {
  description = "Storage account name (3-24 lowercase letters and digits, globally unique)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.name))
    error_message = "Storage account name must be 3-24 lowercase letters and digits."
  }
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "containers" {
  description = "ADLS Gen2 containers (lakehouse layers)."
  type        = list(string)
  default     = ["bronze", "silver", "gold"]
}

variable "tags" {
  type    = map(string)
  default = {}
}

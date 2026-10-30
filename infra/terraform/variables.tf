variable "subscription_id" {
  description = "Azure subscription ID. Leave null to use the ARM_SUBSCRIPTION_ID environment variable."
  type        = string
  default     = null
}

variable "location" {
  description = "Azure region (ADR-001: Central India)."
  type        = string
  default     = "centralindia"
}

variable "project" {
  type    = string
  default = "urbanpulse"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "name_suffix" {
  description = "3-6 lowercase letters or digits that make global names unique."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,6}$", var.name_suffix))
    error_message = "name_suffix must be 3-6 lowercase letters or digits."
  }
}

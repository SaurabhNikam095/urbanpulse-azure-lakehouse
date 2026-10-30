variable "namespace_name" {
  description = "Event Hubs namespace name (6-50 characters)."
  type        = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "capacity" {
  description = "Throughput units (ADR-001: Standard tier, 2 TUs)."
  type        = number
  default     = 2

  validation {
    condition     = var.capacity >= 1 && var.capacity <= 20
    error_message = "Capacity must be between 1 and 20 throughput units."
  }
}

variable "event_hubs" {
  description = "Event hubs to create, with partition count and retention in days."
  type = map(object({
    partition_count   = number
    message_retention = number
  }))
  default = {
    trips    = { partition_count = 4, message_retention = 1 }
    gps      = { partition_count = 4, message_retention = 1 }
    payments = { partition_count = 2, message_retention = 1 }
  }
}

variable "tags" {
  type    = map(string)
  default = {}
}

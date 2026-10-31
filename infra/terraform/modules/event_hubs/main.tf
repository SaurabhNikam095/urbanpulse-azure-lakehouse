resource "azurerm_eventhub_namespace" "this" {
  name                = var.namespace_name
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "Standard"
  capacity            = var.capacity
  tags                = var.tags
}

resource "azurerm_eventhub" "topics" {
  for_each          = var.event_hubs
  name              = each.key
  namespace_id      = azurerm_eventhub_namespace.this.id
  partition_count   = each.value.partition_count
  message_retention = each.value.message_retention
}

resource "azurerm_eventhub_namespace_schema_group" "this" {
  name                 = "urbanpulse"
  namespace_id         = azurerm_eventhub_namespace.this.id
  schema_compatibility = "Backward"
  schema_type          = "Avro"
}

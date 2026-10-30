output "namespace_id" {
  value = azurerm_eventhub_namespace.this.id
}

output "namespace_name" {
  value = azurerm_eventhub_namespace.this.name
}

output "event_hub_names" {
  value = [for hub in azurerm_eventhub.topics : hub.name]
}

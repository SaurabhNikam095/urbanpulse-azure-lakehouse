output "storage_account_name" {
  value = azurerm_storage_account.this.name
}

output "storage_account_id" {
  value = azurerm_storage_account.this.id
}

output "container_names" {
  value = [for fs in azurerm_storage_data_lake_gen2_filesystem.layers : fs.name]
}

output "resource_group_name" {
  value = module.resource_group.name
}

output "storage_account_name" {
  value = module.adls.storage_account_name
}

output "key_vault_uri" {
  value = module.key_vault.vault_uri
}

output "event_hubs_namespace" {
  value = module.event_hubs.namespace_name
}

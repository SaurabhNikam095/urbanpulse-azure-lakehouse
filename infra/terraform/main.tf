locals {
  base = "${var.project}-${var.environment}"

  tags = {
    project     = var.project
    owner       = "saurabhnikam095"
    phase       = "baseline"
    environment = var.environment
    managed_by  = "terraform"
  }
}

module "resource_group" {
  source   = "./modules/resource_group"
  name     = "rg-${local.base}"
  location = var.location
  tags     = local.tags
}

module "adls" {
  source              = "./modules/adls"
  name                = "${var.project}${var.environment}${var.name_suffix}"
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.tags
}

module "key_vault" {
  source              = "./modules/key_vault"
  name                = "kv-${local.base}-${var.name_suffix}"
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.tags
}

module "event_hubs" {
  source              = "./modules/event_hubs"
  namespace_name      = "evhns-${local.base}-${var.name_suffix}"
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.tags
}

module "databricks" {
  source                = "./modules/databricks"
  workspace_name        = "dbw-${local.base}-${var.name_suffix}"
  access_connector_name = "dbac-${local.base}-${var.name_suffix}"
  resource_group_name   = module.resource_group.name
  location              = module.resource_group.location
  storage_account_id    = module.adls.storage_account_id
  tags                  = merge(local.tags, { phase = "streaming-bronze" })
}

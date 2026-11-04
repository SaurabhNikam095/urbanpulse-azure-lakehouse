resource "databricks_storage_credential" "adls" {
  name    = "urbanpulse-adls-credential"
  comment = "Access connector managed identity for the UrbanPulse lakehouse"

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

resource "databricks_external_location" "bronze" {
  name            = "urbanpulse-bronze"
  url             = "abfss://bronze@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls.name
  comment         = "Bronze Delta tables and streaming checkpoints"
}

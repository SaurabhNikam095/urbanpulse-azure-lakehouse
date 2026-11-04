data "databricks_current_user" "me" {}

data "databricks_spark_version" "lts" {
  long_term_support = true
}

data "databricks_node_type" "smallest" {
  min_cores  = 4
  local_disk = true
  category   = "General Purpose"
}

locals {
  node_type = coalesce(var.node_type_id, data.databricks_node_type.smallest.id)
}

resource "databricks_cluster" "single_node" {
  cluster_name            = "urbanpulse-single-node"
  spark_version           = data.databricks_spark_version.lts.id
  node_type_id            = local.node_type
  num_workers             = 0
  autotermination_minutes = var.autotermination_minutes
  data_security_mode      = "SINGLE_USER"
  single_user_name        = data.databricks_current_user.me.user_name

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  custom_tags = {
    ResourceClass = "SingleNode"
    project       = "urbanpulse"
    owner         = "saurabhnikam095"
    phase         = "streaming-bronze"
    managed_by    = "terraform"
  }
}

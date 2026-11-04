output "cluster_id" {
  value = databricks_cluster.single_node.id
}

output "node_type_used" {
  value = local.node_type
}

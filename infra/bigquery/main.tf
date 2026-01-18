provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "instagram_ds" {
  dataset_id = var.dataset_id
  location   = "US"
}

locals {
  tabelas = [
    "dim_instagram_account",
    "dim_instagram_post",
    "dim_instagram_comment",
    "dim_date",
    "fact_instagram_account_snapshot",
    "fact_instagram_post_metrics",
    "fact_instagram_comment_metrics"
  ]
}

resource "google_bigquery_table" "instagram_tables" {
  for_each   = toset(local.tabelas)
  dataset_id = google_bigquery_dataset.instagram_ds.dataset_id
  table_id   = each.value
  schema     = file("${path.module}/schemas/${each.value}.json")
}
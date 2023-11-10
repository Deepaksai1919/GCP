resource "google_bigquery_table" "default" {
  dataset_id = "window_dataset"
  table_id   = "vegetables"
  deletion_protection = false
  view {
    query = file("${path.module}/view_ddl.sql")
    use_legacy_sql = false
  }
}
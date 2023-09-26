resource "google_bigquery_dataset" "bq_ds" {
  dataset_id = "ds_from_tf"
}

resource "google_bigquery_table" "bq_table" {
  dataset_id = google_bigquery_dataset.bq_ds.dataset_id
  table_id = "table_from_tf"
  deletion_protection = false
}
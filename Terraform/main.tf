resource "google_bigquery_table" "default" {
  dataset_id = "window_dataset"
  table_id   = "vegetables"
  deletion_protection = false
  view {
    query = file("${path.module}/view_ddl.sql")
    use_legacy_sql = false
  }
  schema = file("${path.module}/col_desc.json")
}


# # For object table
data "google_project" "gcp_project" {}

# This creates a connection in the US region named "my-connection-id".
# This connection is used to access the bucket.
resource "google_bigquery_connection" "bq_connection" {
  connection_id = "gcs_connection"
  location      = "US"
  cloud_resource {}
}

# # This grants the previous connection IAM role access to the bucket.
resource "google_project_iam_member" "connection_iam" {
  role    = "roles/storage.objectViewer"
  project = data.google_project.gcp_project.project_id
  member  = "serviceAccount:${google_bigquery_connection.bq_connection.cloud_resource[0].service_account_id}"
}

# # This defines a Google BigQuery dataset.
resource "google_bigquery_dataset" "object_dataset" {
  dataset_id = "object_dataset"
}

resource "random_id" "bucket_name_suffix" {
  byte_length = 8
}
# # This creates a bucket in the US region named "my-bucket" with a pseudorandom suffix.

resource "google_storage_bucket" "gcs_bucket" {
  name                        = "object_bucket_${random_id.bucket_name_suffix.hex}"
  location                    = "US"
  force_destroy               = true
  uniform_bucket_level_access = true
}

# This defines a BigQuery object table with manual metadata caching.
resource "google_bigquery_table" "bq_table" {
  deletion_protection = false
  table_id            = "object_table"
  dataset_id          = google_bigquery_dataset.object_dataset.dataset_id
  external_data_configuration {
    connection_id = google_bigquery_connection.bq_connection.name
    autodetect    = false
    # `object_metadata is` required for object tables. For more information, see
    # https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_table#object_metadata
    object_metadata = "SIMPLE"
    # This defines the source for the prior object table.
    source_uris = [
      "gs://${google_storage_bucket.gcs_bucket.name}/*",
    ]
    # metadata_cache_mode = "AUTOMATIC"
  }
  # max_staleness = "0-0 0 4:0:0"
  depends_on = [
    google_project_iam_member.connection_iam
  ]
}

resource "google_bigquery_table" "bq_content_table" {
  deletion_protection = false
  table_id            = "object_content_table"
  dataset_id          = google_bigquery_dataset.object_dataset.dataset_id
  external_data_configuration {
    # connection_id = google_bigquery_connection.bq_connection.name
    autodetect    = false
    source_format = "NEWLINE_DELIMITED_JSON"
    # This defines the source for the prior object table.
    source_uris = [
      "gs://${google_storage_bucket.gcs_bucket.name}/*.json",
    ]
    # metadata_cache_mode = "AUTOMATIC"
  }
  schema = file("${path.module}/content_schema.json")
  # max_staleness = "0-0 0 4:0:0"
  depends_on = [
    google_project_iam_member.connection_iam
  ]
}
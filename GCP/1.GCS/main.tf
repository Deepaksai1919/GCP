resource "google_storage_bucket" "gcs_bucket" {
  name = "tf-course-bucket-from-terraform-deepak"
  location = "US-CENTRAL1"
  storage_class = "NEARLINE"
  labels = {
    "env" = "tf_env"
    "dep" = "compliance"
  }
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 5
    }
    action {
      type = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  retention_policy {
    is_locked = true
    retention_period = 864000 # 10 days
  }
}

resource "google_storage_bucket_object" "gcs_object1" {
  name = "some_logo"
  bucket = google_storage_bucket.gcs_bucket.name
  source = "applelogo.png"
}
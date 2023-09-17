resource "google_cloud_run_v2_service" "run-app-from-tf" {
  name = "run-app-from-tf"
  location = "asia-southeast1"
  template {
    containers {
      # image = "gcr.io/google-samples/hello-app:1.0"
      image = "gcr.io/google-samples/hello-app:2.0"
    }
    service_account = "svc-terraform@terraform-399209.iam.gserviceaccount.com"
  }
}

# For No Auth / Public Access
data "google_iam_policy" "no_auth" {
  binding {
    role = "roles/run.invoker"
    members = ["allUsers"]
  }
}

resource "google_cloud_run_v2_service_iam_policy" "no_auth" {
  name = google_cloud_run_v2_service.run-app-from-tf.name
  location = google_cloud_run_v2_service.run-app-from-tf.location
  policy_data = data.google_iam_policy.no_auth.policy_data
}


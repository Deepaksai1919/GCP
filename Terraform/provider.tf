terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.82.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = "bigquery-392209"
  region = "us-central1"
  zone = "us-centra1-a"
  credentials = "../bigquery-svc.json"
}
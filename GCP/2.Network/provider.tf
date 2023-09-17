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
  project = "terraform-399209"
  region = "us-central1"
  zone = "us-centra1-a"
  credentials = "../sa.json"
}
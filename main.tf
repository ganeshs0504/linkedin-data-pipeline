terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.33.0"
    }
  }
}

provider "google" {
  project = "gcs-linkedin-pipeline"
  region  = "europe-west2"
}

resource "google_storage_bucket" "linkedin-data-bucket" {
  name          = "gcs-linkedin-pipeline-data-bucket"
  location      = "EU"
  force_destroy = true
}
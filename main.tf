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

# resource "google_storage_bucket" "linkedin-data-bucket" {
#   name          = "gcs-linkedin-data-bucket"
#   location      = "EU"
#   force_destroy = true
# }
resource "google_storage_bucket" "dataproc_staging_bucket" {
  name          = "dataproc-staging-bucket-gcs-linkedin-pipeline"
  location      = "EU"
  force_destroy = true
}
resource "google_storage_bucket" "dataproc_temp_bucket" {
  name          = "dataproc-temp-bucket-gcs-linkedin-pipeline"
  location      = "EU"
  force_destroy = true
}

resource "google_storage_bucket" "dataproc_jobs" {
  name          = "gcs-linkedin-dataproc-jobs"
  location      = "EU"
  force_destroy = true
}

resource "google_dataproc_cluster" "dataproc_cluster" {
  name   = "dataproc-cluster"
  region = "europe-west2"

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_staging_bucket.name
    temp_bucket    = google_storage_bucket.dataproc_temp_bucket.name
    master_config {
      num_instances = 1
      machine_type  = "e2-standard-4"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 200
      }
    }

    worker_config {
      num_instances = 0
    }

    gce_cluster_config {
      internal_ip_only = false
    }
    software_config {
      image_version = "2.0.35-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }
  }
}

# resource "google_bigquery_dataset" "linkedin_bq_dataset" {
#   dataset_id  = "linkedin_bq_dataset"
#   description = "Default dataset for the linkedin data pipeline project"
#   location    = "EU"
#   delete_contents_on_destroy = true
# }
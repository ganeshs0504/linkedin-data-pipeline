terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.33.0"
    }
  }
}

provider "google" {
  credentials = file(local.config["credentials"])
  project     = local.config["project_id"]
  region      = local.config["region"]
}

resource "google_storage_bucket" "linkedin-data-bucket" {
  name          = local.config["linkedin_data_bucket_name"]
  location      = local.config["location"]
  force_destroy = true
}
resource "google_storage_bucket" "dataproc_staging_bucket" {
  name          = local.config["dataproc_staging_bucket_name"]
  location      = local.config["location"]
  force_destroy = true
}
resource "google_storage_bucket" "dataproc_temp_bucket" {
  name          = local.config["dataproc_temp_bucket_name"]
  location      = local.config["location"]
  force_destroy = true
}

resource "google_storage_bucket" "dataproc_jobs" {
  name          = local.config["dataproc_jobs_bucket_name"]
  location      = local.config["location"]
  force_destroy = true
}

resource "google_dataproc_cluster" "dataproc_cluster" {
  name   = local.config["dataproc_cluster_name"]
  region = local.config["region"]

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

resource "google_bigquery_dataset" "linkedin_bq_dataset" {
  dataset_id                 = local.config["bigquery_dataset_id"]
  description                = "Default dataset for the linkedin data pipeline project"
  location                   = local.config["location"]
  delete_contents_on_destroy = true
}
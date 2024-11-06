locals {
  config = jsondecode(file("./config.json"))
}

# variable "config" {
#   description = "Configuration loaded from config.json"
#   default = locals.config
# }

# variable "credentials" {
#     description = "GCP creds"
#     default = locals.config["credentials"]
# }

# variable "project" {
#     description = "Project Name"
#     default = "gcs-linkedin-pipeline"
# }

# variable "region" {
#     description = "Project Region"
#     default = "europe-west2"
# }

# variable "location" {
#     description = "Location of resources"
#     default = "EU"
# }

# # Storage buckets
# variable "linkedin_data_bucket_name" {
#   description = "The name of the GCS bucket for LinkedIn data storage."
#   type        = string
#   default     = "gcs-linkedin-data-bucket"
# }

# variable "dataproc_staging_bucket_name" {
#   description = "The name of the GCS staging bucket for Dataproc jobs."
#   type        = string
#   default     = "dataproc-staging-bucket-gcs-linkedin-pipeline"
# }

# variable "dataproc_temp_bucket_name" {
#   description = "The name of the GCS temporary bucket for Dataproc."
#   type        = string
#   default     = "dataproc-temp-bucket-gcs-linkedin-pipeline"
# }

# variable "dataproc_jobs_bucket_name" {
#   description = "The name of the GCS bucket for Dataproc job storage."
#   type        = string
#   default     = "gcs-linkedin-dataproc-jobs"
# }

# # Dataproc Cluster
# variable "dataproc_cluster_name" {
#   description = "The name of the Dataproc cluster."
#   type        = string
#   default     = "dataproc-cluster"
# }

# # BigQuery Dataset
# variable "bigquery_dataset_id" {
#   description = "The ID of the BigQuery dataset for LinkedIn data."
#   type        = string
#   default     = "linkedin_bq_dataset"
# }

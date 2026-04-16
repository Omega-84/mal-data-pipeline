variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "credentials_file" {
  description = "Path to GCP service account JSON key"
  type        = string
}

variable "gcs_bucket_name" {
  description = "GCS bucket for raw Jikan JSON files"
  type        = string
  default     = "jikan_anime_data_bucket"
}

variable "bq_dataset_id" {
  description = "BigQuery dataset for staging and mart tables"
  type        = string
  default     = "mal_pipeline"
}

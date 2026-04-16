output "gcs_bucket_name" {
  description = "GCS bucket for raw Jikan JSON"
  value       = google_storage_bucket.raw_data.name
}

output "bq_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.mal_pipeline.dataset_id
}

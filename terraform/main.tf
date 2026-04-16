terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# ── GCS bucket for raw JSON ──────────────────────────────────────────────────

resource "google_storage_bucket" "raw_data" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# ── BigQuery dataset ─────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "mal_pipeline" {
  dataset_id  = var.bq_dataset_id
  location    = var.region
  description = "MAL anime pipeline — staging and mart tables"

  delete_contents_on_destroy = false
}

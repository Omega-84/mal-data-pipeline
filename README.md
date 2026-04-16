# mal-data-pipeline

An end-to-end data engineering project that ingests anime data from the [MyAnimeList](https://myanimelist.net/) unofficial API ([Jikan](https://jikan.moe/)), transforms it using modern ELT tooling, and serves insights through an interactive analytics dashboard — complete with anime recommendations powered by vector embeddings.

---

## Overview

This project was built as the capstone for the [DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). It covers the full data engineering lifecycle: ingestion, storage, transformation, and visualization — with an ML layer on top for semantic recommendations.

---

## Architecture

```
Jikan API (MyAnimeList)
        │
        ▼
Google Cloud Storage (Raw JSON)
        │
        ▼
Bruin (ELT — Ingest → Load → Staging → Mart)
        │
        ▼
BigQuery (Data Warehouse)
        │
        ├──▶ Streamlit Dashboard (Analytics)
        └──▶ Vector Embeddings → Recommendation Engine
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Source | Jikan API (MyAnimeList) |
| Raw Storage | Google Cloud Storage |
| ELT Orchestration | Bruin |
| Data Warehouse | BigQuery |
| Transformation | SQL (via Bruin) |
| Embeddings | sentence-transformers (HuggingFace) |
| Dashboard | Streamlit |
| Infrastructure | Terraform (GCP) |
| Dependency Management | uv |

---

## Pipeline Layers

### Ingest
Python assets fetch from Jikan API and upload raw JSON to GCS:
- `fetch_descriptions.py` — full anime metadata for top 250 anime
- `fetch_characters.py` — main characters per anime
- `fetch_episodes.py` — all episodes per anime (paginated)

Uploads are idempotent — existing GCS blobs are skipped.

### Load
Python assets read raw JSON from GCS and load into BigQuery raw tables:
- `load_descriptions.py` → `raw_descriptions`
- `load_characters.py` → `raw_characters` (exploded: one row per character)
- `load_episodes.py` → `raw_episodes` (exploded: one row per episode)

All use `WRITE_TRUNCATE` with schema autodetect.

### Staging (Views)
SQL assets cast and clean raw tables — materialized as BigQuery views:
- `stg_descriptions` — typed columns, parsed timestamps, flattened studios/genres
- `stg_characters` — typed IDs, main characters only
- `stg_episodes` — typed IDs, filler flag

### Mart (Tables) — in progress
Business-logic SQL assets materialized as partitioned and clustered BigQuery tables.

---

## Dataset

Data sourced from [MyAnimeList](https://myanimelist.net/) via [Jikan API v4](https://docs.api.jikan.moe/) for the top 250 anime by popularity. Fields include:

- Core metadata: title, type, status, score, rank, popularity
- Airing dates, year, rating (age rating)
- Genre, studio, demographic associations
- Synopsis and image URLs
- Episode-level data (title, score, filler flag)
- Character data (main characters with images)

---

## Project Structure

```
mal-data-pipeline/
├── modules.py                  # Jikan API fetch functions
├── pipeline/
│   ├── pipeline.yml            # Bruin pipeline config
│   └── assets/
│       ├── seeds/              # dim_anime seed (top 250 anime list)
│       ├── ingest/             # GCS upload assets (Python)
│       ├── load/               # BQ load assets (Python)
│       └── staging/            # Typed view assets (SQL)
├── terraform/                  # GCS bucket + BQ dataset provisioning
├── dashboard/                  # Streamlit app (in progress)
└── pyproject.toml              # uv dependencies
```

---

## Getting Started

### Prerequisites
- GCP project with BigQuery and GCS enabled
- Service account JSON with BQ + GCS permissions
- `uv` installed

### Setup

```bash
# Install dependencies
uv sync
source .venv/bin/activate

# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Provision infrastructure
cd terraform
terraform init
terraform apply

# Configure Bruin connection in .bruin.yml (see CLAUDE.md)

# Run the full pipeline
bruin run pipeline/
```

---

## Acknowledgements

- [Jikan](https://jikan.moe/) — Unofficial MyAnimeList REST API
- [DataTalks.Club DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [Bruin](https://bruin-data.github.io/bruin/)

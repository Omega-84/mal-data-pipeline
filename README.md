# mal-data-pipeline 🎌

An end-to-end data platform for anime analytics and discovery, powered by the [MyAnimeList](https://myanimelist.net/) dataset via the [Jikan API](https://jikan.moe/).

---

## Overview

**mal-data-pipeline** ingests anime data from the Jikan API, processes and stores it in a cloud data warehouse, and surfaces insights through an interactive dashboard — with a built-in recommendation engine that suggests similar anime using vector embeddings.

---

## Features

- 🔄 **ELT Pipeline** — Extracts raw anime data from Jikan API, loads to GCS, and transforms using Bruin
- 🏗️ **Data Warehouse** — Structured, query-ready tables in BigQuery/Snowflake
- 📊 **Analytics Dashboard** — Interactive Streamlit app for exploring anime trends, scores, genres, and studios
- 🤖 **Anime Recommendations** — Vector embedding-based similarity search to discover new anime
- ⚡ **On-Demand Ingestion** — Users can trigger the pipeline for any anime not yet in the dataset

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Source | Jikan REST API (MyAnimeList) |
| Ingestion & Transformation | Bruin |
| Raw Storage | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Embeddings | Sentence Transformers (HuggingFace) |
| Dashboard | Streamlit |
| Infrastructure | Terraform, GCP |

---

## Architecture

```
Jikan API
    │
    ▼
Google Cloud Storage (raw JSON)
    │
    ▼
Bruin (ELT)
    │
    ├──▶ BigQuery (dim_anime, dim_genre, fact_scores, fact_episodes)
    │
    └──▶ Embedding Pipeline
              │
              └──▶ Vector Store (BigQuery Vector Search)

Streamlit App
    ├── 📊 Dashboard — rankings, trends, genre breakdowns
    └── 🔍 Recommendations — find similar anime
```

---

## Dashboard Preview

> Coming soon

---

## Getting Started

> Setup instructions coming soon

---

## Data Sources

This project uses data from [MyAnimeList](https://myanimelist.net/) via the unofficial [Jikan API v4](https://docs.api.jikan.moe/). Jikan is a free, open-source REST API — no API key required.

---

## License

MIT

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
Bruin (ELT — Staging → Mart)
        │
        ▼
BigQuery / Snowflake (Data Warehouse)
        │
        ├──▶ Streamlit Dashboard (Analytics)
        └──▶ Vector Embeddings → Recommendation Engine
```

---

## Features

- **Batch ingestion** of anime data from the Jikan REST API
- **Raw storage** in Google Cloud Storage as JSON
- **ELT transformations** using [Bruin](https://bruin-data.github.io/bruin/) into a structured warehouse schema
- **Interactive dashboard** built with Streamlit for exploring anime trends, scores, genres, and studios
- **Anime recommendations** using vector embeddings (sentence-transformers) and similarity search

---

## Dashboard

The Streamlit dashboard includes:

- Top anime by score, popularity, and members
- Genre and studio breakdowns
- Score distribution and rating trends
- Episode count analysis
- Semantic anime recommendations — pick an anime, get similar ones

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Source | Jikan API (MyAnimeList) |
| Raw Storage | Google Cloud Storage |
| ELT | Bruin |
| Data Warehouse | BigQuery / Snowflake |
| Transformation | SQL (via Bruin) |
| Embeddings | sentence-transformers (HuggingFace) |
| Vector Search | BigQuery Vector Search / Pinecone |
| Dashboard | Streamlit |
| Infrastructure | Terraform, GCP |

---

## Dataset

Data is sourced from [MyAnimeList](https://myanimelist.net/) via the unofficial [Jikan API v4](https://docs.api.jikan.moe/). The pipeline ingests metadata for the top anime by popularity, including:

- Core metadata (title, type, status, episodes, score, rank)
- Genre and studio associations
- Score distributions and user statistics
- Episode-level data
- Streaming platform availability

---

## Project Structure

```
mal-data-pipeline/
├── ingestion/          # Jikan API fetch scripts
├── pipeline/           # Bruin ELT assets
├── dashboard/          # Streamlit app
├── embeddings/         # Vector embedding generation
├── terraform/          # GCP infrastructure
├── .env.example        # Environment variable template
└── README.md
```

---

## Getting Started

> Setup instructions coming soon.

---

## Acknowledgements

- [Jikan](https://jikan.moe/) — Unofficial MyAnimeList REST API
- [DataTalks.Club DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [Bruin](https://bruin-data.github.io/bruin/)

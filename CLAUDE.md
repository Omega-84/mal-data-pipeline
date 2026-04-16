# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

End-to-end data engineering capstone for DataTalks.Club DE Zoomcamp. Ingests anime metadata from the Jikan API (MyAnimeList), transforms it via Bruin ELT, loads into BigQuery, and serves a Streamlit dashboard with vector-based recommendations.

**Pipeline:**
```
Jikan API → GCS (raw JSON) → Bruin (staging → mart) → BigQuery → Streamlit + Embeddings
```

## Environment

Uses `uv` for dependency management.

```bash
uv sync                  # install dependencies into .venv
source .venv/bin/activate
uv add <package>         # add a dependency
```

Python version is pinned in `.python-version`. Dependencies are in `pyproject.toml`; lockfile is `uv.lock`.

## Bruin

Bruin handles ingestion, transformation, and orchestration. The CLI is installed at `~/.local/bin/bruin`.

```bash
bruin run pipeline/                        # run full pipeline
bruin run pipeline/assets/some_asset.sql   # run a single asset
bruin validate pipeline/                   # validate all assets
bruin lint pipeline/                       # lint assets
bruin connections list                     # list configured connections
bruin connections test --name <name>       # test a connection
```

**Pipeline structure:**
```
pipeline/
├── pipeline.yml          # schedule, default_connections, notifications
└── assets/
    ├── seeds/            # dim_anime.csv + dim_anime.asset.yml (top 250 anime list)
    ├── ingest/           # fetch_*.py — Jikan API → GCS JSON (idempotent)
    ├── load/             # load_*.py — GCS JSON → BQ raw tables (WRITE_TRUNCATE)
    ├── staging/          # stg_*.sql — raw → typed views (materialization: view)
    └── mart/             # business-logic tables (materialization: table, partitioned + clustered)
```

Asset files: `.sql` for SQL transforms, `.py` for Python logic, `.asset.yml` for YAML-defined assets (must end in `.asset.yml`). Connection for an asset defaults to `default_connections` in `pipeline.yml` if not set in the asset itself.

## Streamlit dashboard

```bash
streamlit run dashboard/app.py
```

## Jikan API

Base URL: `https://api.jikan.moe/v4`. No auth required. Rate-limited to ~3 req/s — add delays between paginated calls. Key endpoints:

- `/anime?order_by=popularity&limit=25&page=N` — paginated anime list
- `/anime/{id}/full` — full metadata for a single anime
- `/anime/{id}/characters` — character list (filtered to `role == "Main"`)
- `/anime/{id}/episodes?page=N` — paginated episode list

All fetch functions live in `modules.py`. Ingest assets use `ThreadPoolExecutor` (max 3 workers for descriptions/characters, 2 for episodes) and `time.sleep(0.4)` per request to respect rate limits.

## GCS / BigQuery

Credentials via service account JSON. Path set in `.env` as `GOOGLE_APPLICATION_CREDENTIALS`. Project and bucket names also come from `.env`. Never hardcode credentials or project IDs.

GCS bucket: `jikan_anime_data_bucket`. Prefixes: `descriptions/`, `characters/`, `episodes/`.

BigQuery dataset: `mal_pipeline`. All layers (raw, staging, mart) live in this single dataset. Raw tables: `raw_descriptions`, `raw_characters`, `raw_episodes`. Staging views: `stg_descriptions`, `stg_characters`, `stg_episodes`.

## Zoomcamp rubric notes

To score full marks, BigQuery tables must use **partitioning** (e.g. by `aired_from` date) and **clustering** (e.g. by `genre`). Bruin must be used for ingestion, transformation, orchestration, AND analysis (AI analyst feature) to qualify for the Bruin competition prize.

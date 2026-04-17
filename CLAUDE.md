# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

End-to-end data engineering capstone for DataTalks.Club DE Zoomcamp and the Bruin Data Engineering Competition. Ingests anime metadata from the Jikan API (MyAnimeList), transforms it via Bruin ELT, loads into BigQuery, and serves a Streamlit dashboard (OtakuLens) with vector-based recommendations.

**Pipeline:**
```
Jikan API → GCS (raw JSON) → Bruin (staging → intermediate → mart) → BigQuery → Streamlit
                                                                    ↘ DuckDB (local fallback)
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

Bruin handles ingestion, transformation, orchestration, and AI analysis. The CLI is installed at `~/.local/bin/bruin`.

```bash
bruin run pipeline/                        # run full pipeline
bruin run pipeline/assets/some_asset.sql   # run a single asset
bruin validate pipeline/                   # validate all assets
bruin lint pipeline/                       # lint assets
bruin connections list                     # list configured connections
bruin connections test --name gcp-default  # test the GCP connection
bruin ai enhance pipeline/                 # AI-generate descriptions + quality checks
```

**IMPORTANT — `bruin ai enhance` side effects:**
`bruin ai enhance` frequently breaks working assets by:
- Changing `connection: gcp-default` → `connection: google_cloud_platform` (wrong — use `gcp-default`)
- Changing `secrets: key: gcp-default` → `key: google_cloud_platform` (wrong — use `gcp-default`)
- Adding `materialization: type: table/file` to Python ingest/load assets (wrong — Python assets must NOT have `materialization` blocks unless they implement `materialize()`)

After running `bruin ai enhance`, always verify these fields in modified assets.

**Pipeline structure:**
```
pipeline/
├── pipeline.yml          # schedule (@daily), default_connections, notifications
└── assets/
    ├── seeds/            # dim_anime.csv + dim_anime.asset.yml (top 500 anime list)
    ├── ingest/           # fetch_*.py — Jikan API → GCS JSON (idempotent)
    ├── load/             # load_*.py — GCS JSON → BQ raw tables (WRITE_TRUNCATE)
    ├── staging/          # stg_*.sql — raw → typed views (materialization: view)
    ├── intermediate/     # int_*.sql — joined/aggregated tables (materialization: table)
    └── mart/             # final consumption tables (partitioned + clustered)
```

**Asset run order (DAG):**
seeds → ingest → load → staging → intermediate → mart

Asset files: `.sql` for SQL transforms, `.py` for Python logic, `.asset.yml` for YAML-defined assets (must end in `.asset.yml`). Connection defaults to `default_connections` in `pipeline.yml` if not set in the asset itself.

All mart/intermediate assets have AI-generated descriptions and column-level quality checks.

## Streamlit dashboard

Dashboard is **OtakuLens** — dark gold theme, anime selector with poster, scorecards, airing status, character grid, charts, and semantic recommendations.

```bash
streamlit run dashboard/app.py
```

Key implementation notes:
- `GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")` at module top — no hardcoding
- `get_backend()` tries BigQuery first, falls back to DuckDB. Supports `st.secrets["gcp_service_account"]` for Streamlit Cloud deployment
- All BQ query strings use f-strings with `{GCP_PROJECT_ID}` interpolation
- `@st.cache_resource` for connection + embedding model — requires app restart to pick up code changes
- Recommendation engine uses `sentence-transformers` (`all-MiniLM-L6-v2`) with genre/theme features weighted 3x vs synopsis
- `get_base_title()` strips season/part suffixes to exclude same-series sequels from recommendations
- Same media type filtering in recommendations (`anime_type` must match)
- `is_episodic = row.get("anime_type") in ("TV", "ONA")` gates episode-specific charts
- For `pd.NA` boolean checks: use `v is not None and pd.notna(v)`, NOT `v and not pd.isna(v)` (crashes)

## DuckDB fallback

A local DuckDB snapshot of all mart tables lives at `data/mal.duckdb`. The dashboard auto-detects BQ availability and falls back to DuckDB if BQ is unreachable.

To refresh the snapshot before tearing down GCP infra:
```bash
GCP_PROJECT_ID=your-project-id python scripts/export_to_duckdb.py
```

## Jikan API

Base URL: `https://api.jikan.moe/v4`. No auth required. Rate-limited to ~3 req/s — add delays between paginated calls. Key endpoints:

- `/anime?order_by=popularity&limit=25&page=N` — paginated anime list
- `/anime/{id}/full` — full metadata for a single anime
- `/anime/{id}/characters` — character list (filtered to `role == "Main"`)
- `/anime/{id}/episodes?page=N` — paginated episode list
- `/anime/{id}/statistics` — user engagement counts + per-score vote breakdown

All fetch functions live in `modules.py`. Ingest assets use `ThreadPoolExecutor` (max 3 workers for descriptions/characters/statistics, 2 for episodes) and `time.sleep(0.4)` per request to respect rate limits.

## GCS / BigQuery

Credentials via service account JSON. Path configured in `.bruin.yml` (gitignored). Never hardcode credentials or project IDs.

GCS bucket: `jikan_anime_data_bucket`. Prefixes: `descriptions/`, `characters/`, `episodes/`, `statistics/`.

BigQuery dataset: `mal_pipeline`. All SQL assets use 2-part table names (`` `mal_pipeline.table` ``) — project is resolved from the Bruin connection.

BigQuery dataset location: **US** (multi-region). Must match Terraform config (`location = "US"` in `main.tf`).

Table inventory:
- Raw: `raw_descriptions`, `raw_characters`, `raw_episodes`, `raw_statistics`
- Staging views: `stg_descriptions`, `stg_characters`, `stg_episodes`, `stg_statistics`
- Intermediate: `int_episode_agg`, `int_anime_base`
- Mart: `mart_anime` (partitioned by `TIMESTAMP_TRUNC(airing_start, YEAR)`, clustered by `genre_1`), `mart_episodes`, `mart_characters`

## Credentials & secrets (never commit)

Gitignored: `.bruin.yml`, `terraform/terraform.tfvars`, `.env`, `*.json`, `.streamlit/secrets.toml`

Templates committed for reproducibility: `.env.example`, `terraform/terraform.tfvars.example`, `.streamlit/secrets.toml.example`

## Bruin competition notes

Bruin is used for all four required categories: ingestion, transformation, orchestration, and analysis.
- Analysis = Bruin AI data analyst (`ai_data_analyst/prompt.txt` + MCP + Claude Code)
- Screenshots of AI analyst queries are in `ai_data_analyst/Q1.png`, `Q2.png`, `Q3.png`
- Deadline: June 1, 2026

## DE Zoomcamp rubric notes

BigQuery tables use **partitioning** (`mart_anime` by airing year) and **clustering** (by `genre_1`). Dashboard has 6+ tiles including categorical distributions and temporal trends.

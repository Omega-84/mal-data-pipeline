<p align="center">
  <img src="img/logo.png" alt="OtakuLens Logo" width="180"/>
</p>

<h1 align="center">OtakuLens — Anime Analytics Pipeline</h1>

<p align="center">
  Real-time anime intelligence — ingesting metadata for <strong>500+ titles</strong> from MyAnimeList, transforming it through a production-grade ELT pipeline, and serving interactive analytics with <strong>semantic recommendations</strong> powered by sentence embeddings.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Bruin-000000?style=for-the-badge&logoColor=white"/>
  <img src="https://img.shields.io/badge/Google%20Cloud-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white"/>
  <img src="https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white"/>
  <img src="https://img.shields.io/badge/Cloud%20Storage-AECBFA?style=for-the-badge&logo=googlecloudstorage&logoColor=black"/>
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white"/>
  <img src="https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black"/>
  <img src="https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white"/>
  <img src="https://img.shields.io/badge/uv-DE5FE9?style=for-the-badge&logo=astral&logoColor=white"/>
</p>

<p align="center">
  <a href="https://mal-data-pipeline-zprefllqfdr4htgqq6ggdq.streamlit.app/"><img src="https://img.shields.io/badge/🚀 Live Demo-FF4B4B?style=for-the-badge&logoColor=white"/></a>
  &nbsp;
  <a href="https://github.com/DataTalksClub/data-engineering-zoomcamp"><img src="https://img.shields.io/badge/DE Zoomcamp-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white"/></a>
  &nbsp;
  <a href="https://getbruin.com/competition/"><img src="https://img.shields.io/badge/Bruin Competition-000000?style=for-the-badge&logoColor=white"/></a>
</p>

---

## 💡 About the Project

**OtakuLens** is an end-to-end data engineering project built as a capstone for the [DataTalks.Club DE Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) and submitted to the [Bruin Data Engineering Competition](https://getbruin.com/competition/).

**Objective:** Build a fully reproducible, cloud-native pipeline that collects anime metadata from the [Jikan API](https://jikan.moe/) (the unofficial MyAnimeList REST API), transforms it through a multi-layer ELT process using Bruin, and surfaces actionable insights through an interactive Streamlit dashboard — including content-based anime recommendations driven by semantic similarity.

The project demonstrates:
- **Cloud infrastructure as code** — GCS + BigQuery provisioned via Terraform
- **Orchestrated ELT** — Bruin pipeline with a `@daily` schedule and full DAG dependency management
- **Layered transformations** — staging → intermediate → mart with partitioning and clustering
- **AI-powered analysis** — Bruin AI data analyst (MCP + Claude Code) querying real pipeline data
- **Semantic recommendations** — `sentence-transformers` embeddings over synopsis, genres, themes, and studio
- **Offline resilience** — automatic DuckDB fallback when BigQuery is unavailable

---

### 🔗 Project Links

| Resource | URL |
|---|---|
| Live Dashboard | [streamlit.app](https://mal-data-pipeline-zprefllqfdr4htgqq6ggdq.streamlit.app/) |
| GitHub | [Omega-84/mal-data-pipeline](https://github.com/Omega-84/mal-data-pipeline) |

---

![Infographic](img/infographic.png)

---

## 🏗️ Architecture

```
Jikan API (MyAnimeList)
        │
        ▼
Google Cloud Storage  ◄── Bruin ingest assets (idempotent, rate-limited)
   Raw JSON files
        │
        ▼
  Bruin ELT Pipeline
  ├── Load         GCS JSON → BigQuery raw tables (WRITE_TRUNCATE)
  ├── Staging      raw → typed views
  ├── Intermediate joins + aggregations
  └── Mart         partitioned + clustered final tables
        │
        ├──▶ BigQuery (primary)
        │         └──▶ OtakuLens Dashboard (Streamlit)
        └──▶ DuckDB (local fallback — auto-detected)
```

### Pipeline Lineage

![Pipeline Lineage](img/lineage.png)

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Source | [Jikan API v4](https://docs.api.jikan.moe/) — MyAnimeList, no auth |
| Raw Storage | Google Cloud Storage |
| ELT + Orchestration | [Bruin](https://getbruin.com/) (`@daily` schedule) |
| Data Warehouse | BigQuery — partitioned + clustered |
| AI Analysis | Bruin AI data analyst (MCP + Claude Code) |
| Recommendations | `sentence-transformers` `all-MiniLM-L6-v2` |
| Dashboard | Streamlit |
| Local Fallback | DuckDB |
| Infrastructure | Terraform |
| Package Management | `uv` |

---

## 📊 Dataset

Top **500 anime** by popularity from MyAnimeList via Jikan API:

| Data | Details |
|---|---|
| Core metadata | title, type, status, score, rank, popularity, rating |
| Classification | genres (up to 3), themes (up to 2), demographics, source |
| Temporal | airing start/end, year, season |
| Episodes | title, score, filler flag — per episode |
| Engagement | watching, completed, on-hold, dropped, plan-to-watch |
| Score dist. | vote counts + percentages for scores 1–10 |
| Characters | main character names and images |

**~500 anime · ~10,000+ episodes · ~2,000+ characters**

---

## 🔄 Pipeline Structure

```
pipeline/
├── pipeline.yml                  # @daily schedule, gcp-default connection
└── assets/
    ├── seeds/         dim_anime.csv — top 500 anime IDs
    ├── ingest/        fetch_*.py — Jikan API → GCS (idempotent)
    ├── load/          load_*.py — GCS → BQ raw tables (WRITE_TRUNCATE)
    ├── staging/       stg_*.sql — typed views
    ├── intermediate/  int_*.sql — joins + aggregations
    └── mart/          mart_*.sql — partitioned + clustered
```

**Run order:** `seeds → ingest → load → staging → intermediate → mart`

### Transformation layers

**Staging** — SQL views that cast raw columns to correct types, parse timestamps, and extract structured fields. No data movement.

**Intermediate**
- `int_episode_agg` — per-anime: episode count, avg score, filler %, best episode title/score
- `int_anime_base` — wide join of descriptions + statistics + episode aggregates; splits genres (max 3) and themes (max 2)

**Mart**
- `mart_anime` — partitioned by `TIMESTAMP_TRUNC(airing_start, YEAR)`, clustered by `genre_1`
- `mart_episodes` — episodes with anime title
- `mart_characters` — characters with anime title

All intermediate and mart assets have AI-generated descriptions and column-level quality checks via `bruin ai enhance`.

---

## 🎌 OtakuLens Dashboard

**[→ Open Live App](https://mal-data-pipeline-zprefllqfdr4htgqq6ggdq.streamlit.app/)**

Dark-gold themed Streamlit dashboard with:

- **Dynamic filters** — genre, theme, studio, media type (all cross-reactive)
- **Scorecards** — MAL score, popularity rank, episodes, completion rate, best episode
- **Airing status badge** — live / finished indicator
- **Character grid** — top 12 main characters with images
- **Viewer engagement** — watching / completed / on-hold / dropped / plan-to-watch
- **Episode timeline** *(TV/ONA)* — score per episode, canon vs filler
- **Filler breakdown** *(TV/ONA)* — donut chart with percentages
- **Score distribution** — histogram with average trend line
- **Semantic recommendations** — 6 similar anime via sentence embeddings (synopsis + genres/themes/studio, same media type, same-series filtered)

Automatically falls back to local DuckDB when BigQuery is unavailable.

---

## 🤖 Bruin AI Data Analyst

This project uses the **Bruin AI data analyst** (Bruin MCP + Claude Code) to answer business questions directly against the pipeline data — no guessing schemas, always executing against real data.

System prompt in `ai_data_analyst/prompt.txt`.

**Sample analyses:**

**Q1 — Give me the 10 most popular sports anime TV series.**
![Most popular sports anime TV series](ai_data_analyst/Q1.png)

**Q2 — What are the 3 highest rated One Piece episodes?**
![Highest rated One Piece episodes](ai_data_analyst/Q2.png)

**Q3 — What studio has the most productions in the top 50 most popular anime?**
![Studio with most productions in top 50](ai_data_analyst/Q3.png)

---

## 🚀 Getting Started

### Prerequisites

- Python 3.13+
- GCP project with BigQuery + GCS APIs enabled
- Service account JSON with **BigQuery Data Editor** + **GCS Object Admin** roles
- [`uv`](https://github.com/astral-sh/uv): `pip install uv`
- [Bruin CLI](https://getbruin.com/docs/getting-started/introduction/): `curl -LO https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh && bash install.sh`
- [Terraform](https://developer.hashicorp.com/terraform/install)

### 1. Clone & install

```bash
git clone https://github.com/Omega-84/mal-data-pipeline.git
cd mal-data-pipeline
uv sync
source .venv/bin/activate
```

### 2. Environment variables

```bash
cp .env.example .env
# set GCP_PROJECT_ID=your-gcp-project-id
export $(cat .env | xargs)
```

### 3. Configure Bruin

Create `.bruin.yml` (gitignored) in the project root:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp-default
          service_account_file: /path/to/your-service-account.json
          project_id: your-gcp-project-id
          location: US
```

```bash
bruin connections test --name gcp-default
```

### 4. Provision infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in project_id + credentials_file
terraform init && terraform apply
cd ..
```

Creates: GCS bucket + BigQuery dataset `mal_pipeline` (US multi-region).

### 5. Run the pipeline

```bash
bruin run pipeline/
```

Or layer by layer:

```bash
bruin run pipeline/assets/seeds/
bruin run pipeline/assets/ingest/
bruin run pipeline/assets/load/
bruin run pipeline/assets/staging/
bruin run pipeline/assets/intermediate/
bruin run pipeline/assets/mart/
```

### 6. Launch dashboard

```bash
streamlit run dashboard/app.py
```

### Local-only (no GCP)

The dashboard falls back to `data/mal.duckdb` automatically — no setup needed. To refresh the snapshot:

```bash
python scripts/export_to_duckdb.py
```

---

## ☁️ Streamlit Cloud Deployment

1. Push repo to GitHub
2. Create app at [streamlit.io/cloud](https://streamlit.io/cloud) pointing to `dashboard/app.py`
3. Under **App Settings → Secrets**, add (see `.streamlit/secrets.toml.example` for full template):

```toml
GCP_PROJECT_ID = "your-gcp-project-id"

[gcp_service_account]
type = "service_account"
project_id = "your-gcp-project-id"
private_key_id = "..."
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "your-sa@your-project.iam.gserviceaccount.com"
# ... (see .streamlit/secrets.toml.example)
```

No credential files are committed — credentials are read via `st.secrets["gcp_service_account"]`.

---

## 🔐 Reproducibility

- No credentials or project IDs hardcoded anywhere in source
- `GCP_PROJECT_ID` read from env var — set once, works everywhere
- BigQuery clients use `bigquery.Client()` — project inferred from service account
- SQL assets use 2-part table names (`` `mal_pipeline.table` ``) — project from Bruin connection
- Gitignored: `.bruin.yml`, `terraform.tfvars`, `.env`, `*.json`, `.streamlit/secrets.toml`
- Templates committed: `.env.example`, `terraform.tfvars.example`, `secrets.toml.example`

---

## 📁 Project Structure

```
mal-data-pipeline/
├── modules.py                         # Jikan API fetch helpers
├── pipeline/
│   ├── pipeline.yml                   # @daily schedule, connections
│   └── assets/                        # seeds → ingest → load → staging → intermediate → mart
├── dashboard/
│   ├── app.py                         # OtakuLens Streamlit app
│   └── .streamlit/config.toml         # dark theme, file watcher config
├── ai_data_analyst/
│   ├── prompt.txt                     # system prompt for Bruin AI analyst
│   └── Q1.png / Q2.png / Q3.png       # analysis screenshots
├── scripts/
│   ├── generate_seed.py               # fetch top 500 → dim_anime.csv
│   └── export_to_duckdb.py            # snapshot BQ → data/mal.duckdb
├── data/mal.duckdb                    # committed DuckDB snapshot (BQ fallback)
├── terraform/                         # GCS + BigQuery provisioning
├── infographic.png                    # project overview infographic
├── .env.example                       # env var template
├── .streamlit/secrets.toml.example    # Streamlit secrets template
└── terraform/terraform.tfvars.example # Terraform vars template
```

---

## 🏆 Competition & Course

### Bruin Data Engineering Competition
Submitted to [getbruin.com/competition](https://getbruin.com/competition/) — Bruin used for all four required categories:

| Category | How |
|---|---|
| Ingestion | 4 Python assets — Jikan API → GCS |
| Transformation | Staging → Intermediate → Mart SQL layers |
| Orchestration | `@daily` schedule, DAG-driven execution |
| Analysis | Bruin AI data analyst — MCP + Claude Code |

### DataTalks.Club DE Zoomcamp
Capstone project covering: cloud infrastructure, data lake, warehouse (partitioned + clustered), transformations, dashboard, and reproducibility.

---

## 🙏 Acknowledgements

- [Jikan](https://jikan.moe/) — Unofficial MyAnimeList REST API
- [DataTalks.Club](https://datatalks.club/) — DE Zoomcamp
- [Bruin](https://getbruin.com/) — ELT orchestration + AI analysis
- [sentence-transformers](https://www.sbert.net/) — Semantic similarity

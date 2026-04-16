"""@bruin

name: load.load_episodes
description: |
  Loads episode-level data from GCS JSON files into BigQuery raw episodes table.

  Reads JSON files uploaded by fetch_episodes (format: episodes/anime_{anime_id}.json)
  and explodes the episodes array so each episode becomes a separate row in the raw_episodes
  table. This transformation enables episode-level analytics and simplifies downstream
  SQL transformations in the staging and mart layers.

  The load operation uses WRITE_TRUNCATE with schema autodetect to fully refresh the
  raw_episodes table on each run. This approach ensures data consistency and handles
  schema evolution from the upstream Jikan API.

  Each source JSON contains an anime_id and an episodes array. The exploding process
  combines the anime_id with each episode object, creating denormalized rows ready
  for BigQuery analytics workloads.

depends:
  - ingest.fetch_episodes

tags:
  - domain:entertainment
  - data_type:fact_table
  - pipeline_role:raw
  - sensitivity:public
  - update_pattern:full_refresh
  - source:jikan_api

columns:
  - name: anime_id
    type: integer
    description: MyAnimeList anime identifier, foreign key to dim_anime seed and staging tables
    checks:
      - name: not_null
  - name: episode_id
    type: integer
    description: MyAnimeList episode identifier (mal_id), unique identifier for individual episodes
    checks:
      - name: not_null
  - name: title
    type: string
    description: Episode title as provided by MyAnimeList, may be in Japanese, English, or romaji depending on anime
  - name: score
    type: float
    description: Community rating score for this specific episode on a 0.0-10.0 scale, null for episodes without ratings
  - name: filler
    type: boolean
    description: True if episode is filler content (not canon to main storyline), false for canon episodes, may be null for unknown

@bruin"""

import json, os, sys
from google.cloud import storage, bigquery

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

PROJECT_ID  = "de-zoomcamp-485104"
DATASET_ID  = "mal_pipeline"
BUCKET_NAME = "jikan_anime_data_bucket"
TABLE_ID    = "raw_episodes"


def main():
    gcs = storage.Client()
    bq  = bigquery.Client(project=PROJECT_ID)

    rows = []
    for blob in gcs.bucket(BUCKET_NAME).list_blobs(prefix="episodes/"):
        data = json.loads(blob.download_as_text())
        anime_id = data["anime_id"]
        for ep in data.get("episodes", []):
            rows.append({"anime_id": anime_id, **ep})

    job = bq.load_table_from_json(
        rows,
        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        ),
    )
    job.result()
    print(f"Loaded {len(rows)} rows into {DATASET_ID}.{TABLE_ID}")


main()

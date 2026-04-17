"""@bruin

name: load.load_statistics
description: |
  Loads anime viewer engagement statistics and score distributions from GCS JSON files into the raw_statistics
  BigQuery table. This asset processes user interaction metrics and detailed rating distributions (1-10 scale)
  sourced from the MyAnimeList platform via the Jikan API.

  The data provides comprehensive insights into anime popularity and user engagement patterns, including viewing
  status counts (watching, completed, dropped, on_hold, plan_to_watch), total user interactions, and granular
  score distributions with both vote counts and percentages for each rating tier.

  This raw table serves as the foundation for downstream analytics and recommendation systems, enabling analysis
  of anime popularity trends, user behavior patterns, and rating distributions across the curated anime catalog.

  Uses BigQuery WRITE_TRUNCATE mode to replace data on each run, ensuring consistency with the source JSON files.

  Operational Notes:
  - Runs daily as part of the mal-pipeline schedule
  - Full table replacement (WRITE_TRUNCATE) for data consistency
  - Auto-detects schema from JSON source files
  - Expected load: ~250 rows (one per anime) with ~30 columns each
  - Typical runtime: <30 seconds for full dataset load
connection: gcp-default
tags:
  - domain:entertainment
  - data_type:fact_table
  - pipeline_role:load
  - sensitivity:public
  - update_pattern:full_refresh
  - source:gcs_json
  - refresh_cadence:daily
  - volume:small
  - performance:io_bound
  - format:json_to_bq
  - anime
  - statistics
  - engagement
  - ratings
  - user_behavior
  - raw_data
  - myanimelist
  - load
  - gcs_to_bq
  - daily_refresh
  - bigquery
  - user_metrics
  - truncate_load

depends:
  - ingest.fetch_statistics

secrets:
  - key: gcp-default
    inject_as: gcp-default

columns:
  - name: anime_id
    type: integer
    description: Unique identifier for the anime title from MyAnimeList database
    checks:
      - name: not_null
      - name: unique
  - name: watching
    type: integer
    description: Number of users currently watching this anime (active viewers)
    checks:
      - name: not_null
  - name: completed
    type: integer
    description: Number of users who have completed watching this anime (finished series)
    checks:
      - name: not_null
  - name: on_hold
    type: integer
    description: Number of users who have paused watching and may resume later (temporarily inactive)
    checks:
      - name: not_null
  - name: dropped
    type: integer
    description: Number of users who stopped watching and don't plan to resume (abandoned)
    checks:
      - name: not_null
  - name: plan_to_watch
    type: integer
    description: Number of users who have added this anime to their watchlist but haven't started (pipeline)
    checks:
      - name: not_null
  - name: total
    type: integer
    description: Total number of users who have interacted with this anime across all viewing statuses
    checks:
      - name: not_null
  - name: score_1_votes
    type: integer
    description: Number of users who rated this anime 1/10 (lowest possible rating)
  - name: score_2_votes
    type: integer
    description: Number of users who rated this anime 2/10
  - name: score_3_votes
    type: integer
    description: Number of users who rated this anime 3/10
  - name: score_4_votes
    type: integer
    description: Number of users who rated this anime 4/10
  - name: score_5_votes
    type: integer
    description: Number of users who rated this anime 5/10 (average/neutral rating)
  - name: score_6_votes
    type: integer
    description: Number of users who rated this anime 6/10
  - name: score_7_votes
    type: integer
    description: Number of users who rated this anime 7/10
  - name: score_8_votes
    type: integer
    description: Number of users who rated this anime 8/10
  - name: score_9_votes
    type: integer
    description: Number of users who rated this anime 9/10
  - name: score_10_votes
    type: integer
    description: Number of users who rated this anime 10/10 (highest possible rating)
  - name: score_1_pct
    type: float
    description: Percentage of total ratings that are 1/10 scores (as decimal, e.g., 0.15 = 15%)
  - name: score_2_pct
    type: float
    description: Percentage of total ratings that are 2/10 scores (as decimal)
  - name: score_3_pct
    type: float
    description: Percentage of total ratings that are 3/10 scores (as decimal)
  - name: score_4_pct
    type: float
    description: Percentage of total ratings that are 4/10 scores (as decimal)
  - name: score_5_pct
    type: float
    description: Percentage of total ratings that are 5/10 scores (as decimal)
  - name: score_6_pct
    type: float
    description: Percentage of total ratings that are 6/10 scores (as decimal)
  - name: score_7_pct
    type: float
    description: Percentage of total ratings that are 7/10 scores (as decimal)
  - name: score_8_pct
    type: float
    description: Percentage of total ratings that are 8/10 scores (as decimal)
  - name: score_9_pct
    type: float
    description: Percentage of total ratings that are 9/10 scores (as decimal)
  - name: score_10_pct
    type: float
    description: Percentage of total ratings that are 10/10 scores (as decimal)

@bruin"""

import json, os, sys
from google.cloud import storage, bigquery

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

DATASET_ID  = "mal_pipeline"
BUCKET_NAME = "jikan_anime_data_bucket"
TABLE_ID    = "raw_statistics"


def main():
    gcs = storage.Client()
    bq  = bigquery.Client()

    rows = []
    for blob in gcs.bucket(BUCKET_NAME).list_blobs(prefix="statistics/"):
        rows.append(json.loads(blob.download_as_text()))

    job = bq.load_table_from_json(
        rows,
        f"{DATASET_ID}.{TABLE_ID}",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        ),
    )
    job.result()
    print(f"Loaded {len(rows)} rows into {DATASET_ID}.{TABLE_ID}")


main()

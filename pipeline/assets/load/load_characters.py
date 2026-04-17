"""@bruin

name: load.load_characters
description: |
  Loads main character data from Google Cloud Storage JSON files into BigQuery raw table.
  Transforms nested character arrays into individual rows for downstream processing in the
  anime recommendation pipeline.

  This asset processes JSON files stored in the GCS characters/ prefix, where each file
  contains an anime_id and a characters array. The transformation explodes the nested
  character arrays so that each character becomes a separate row in the raw_characters
  table, enabling efficient joins and character-based analytics downstream.

  **Data Transformation:** Nested JSON arrays → Flattened relational rows
  **Load Pattern:** Full refresh (WRITE_TRUNCATE) with autodetect schema
  **Data Volume:** ~500-2000 character records (2-8 main characters per anime, ~250 anime)
  **Refresh Cadence:** Daily batch processing, dependent on upstream fetch_characters asset
  **Data Characteristics:** All characters filtered to role="Main" during ingestion, ensuring
  focus on primary protagonists/antagonists only. Character names vary in format (Japanese
  romanization vs localized names) and image URLs are stable MAL CDN references.
  **Performance:** Lightweight transformation suitable for BigQuery autodetect schema.
  Typical load time <30 seconds for full dataset refresh.
  **Downstream Usage:** Feeds stg_characters view for type casting and mart_characters table
  for character-based recommendations, search functionality, and visual displays in OtakuLens
connection: gcp-default
tags:
  - domain:entertainment
  - data_type:fact_table
  - pipeline_role:raw_load
  - sensitivity:public
  - update_pattern:full_refresh
  - source_system:gcs_json
  - content_type:character_data
  - anime_data
  - mal_dataset
  - extraction_method:api_batch
  - data_freshness:daily
  - processing_time:lightweight

depends:
  - ingest.fetch_characters
owner: DE-Zoomcamp-Student

secrets:
  - key: gcp-default
    inject_as: gcp-default

columns:
  - name: anime_id
    type: INT64
    description: MyAnimeList anime identifier (foreign key), linking character data back to the source anime series for joins and lineage tracking. References dim_anime.csv seed data with ~250 expected unique values. Used extensively in joins with mart_anime and other dimensional tables for cross-cutting anime analytics. Values are positive integers typically in the 1-50000 range for popular anime.
    checks:
      - name: not_null
  - name: character_id
    type: INT64
    description: Unique MyAnimeList character identifier (primary key) from mal_id field in Jikan API response. Used as the primary key for character-based joins and deduplication. MAL character IDs are positive integers in the hundreds of thousands to millions range, serving as stable identifiers across the MyAnimeList platform ecosystem.
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: STRING
    description: Full name of the main character as provided by MyAnimeList, used for character-based search and analytics. Expected to be non-empty for all main characters. Names typically follow "Given Family" or "Family, Given" format for Japanese characters, or localized formats for international characters. Critical for dashboard search functionality and character identification in the OtakuLens interface.
    checks:
      - name: not_null
  - name: image_url
    type: STRING
    description: Direct HTTPS URL to character's profile image (JPG format from MyAnimeList CDN), enables visual character displays in dashboards. URLs follow stable CDN pattern "https://cdn.myanimelist.net/images/characters/..." and are permanent for the character's lifetime. Essential for OtakuLens character grid displays and visual browsing features. All URLs are HTTPS and point to MyAnimeList's official CDN infrastructure.

@bruin"""

import json, os, sys
from google.cloud import storage, bigquery

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

DATASET_ID  = "mal_pipeline"
BUCKET_NAME = "jikan_anime_data_bucket"
TABLE_ID    = "raw_characters"


def main():
    gcs = storage.Client()
    bq  = bigquery.Client()

    rows = []
    for blob in gcs.bucket(BUCKET_NAME).list_blobs(prefix="characters/"):
        data = json.loads(blob.download_as_text())
        anime_id = data["anime_id"]
        for char in data.get("characters", []):
            rows.append({"anime_id": anime_id, **char})

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

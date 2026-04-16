"""@bruin

name: load.load_characters
type: python
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
  **Downstream Usage:** Feeds stg_characters view for type casting and potential mart tables
  for character-based recommendations and analytics

connection: gcp-default

depends:
  - ingest.fetch_characters

columns:
  - name: anime_id
    description: MyAnimeList anime identifier (foreign key), linking character data back to the source anime series for joins and lineage tracking
  - name: character_id
    description: Unique MyAnimeList character identifier (primary key) from mal_id field in Jikan API response
  - name: name
    description: Full name of the main character as provided by MyAnimeList, used for character-based search and analytics
  - name: image_url
    description: URL to character's profile image (JPG format from MyAnimeList CDN), enables visual character displays in dashboards

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

owner: DE-Zoomcamp-Student

@bruin"""

import json, os, sys
from google.cloud import storage, bigquery

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

PROJECT_ID  = "de-zoomcamp-485104"
DATASET_ID  = "mal_pipeline"
BUCKET_NAME = "jikan_anime_data_bucket"
TABLE_ID    = "raw_characters"


def main():
    gcs = storage.Client()
    bq  = bigquery.Client(project=PROJECT_ID)

    rows = []
    for blob in gcs.bucket(BUCKET_NAME).list_blobs(prefix="characters/"):
        data = json.loads(blob.download_as_text())
        anime_id = data["anime_id"]
        for char in data.get("characters", []):
            rows.append({"anime_id": anime_id, **char})

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

""" @bruin
name: load.load_characters
type: python
description: "Load character JSONs from GCS into BigQuery raw table (exploded to one row per character)"
depends:
  - ingest.fetch_characters
@bruin """

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

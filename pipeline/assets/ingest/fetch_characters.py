"""@bruin

name: ingest.fetch_characters
description: |
  Fetches main character data from the Jikan API (MyAnimeList) for popular anime titles
  and stores as JSON files in Google Cloud Storage. Part of the anime data pipeline that
  enables character-based analytics and recommendation features.

  The asset processes a curated list of ~250 top anime titles from the seed file (dim_anime.csv),
  calling the Jikan API's /anime/{id}/characters endpoint to retrieve main character information.
  Only characters with role="Main" are extracted to focus on the most significant characters
  per anime. The data includes character names, MAL character IDs, and profile image URLs.

  **Data Flow:** Jikan API → GCS JSON files (characters/ prefix) → BigQuery raw table → staging view

  **Rate Limiting:** Implements 0.4s delays between requests and uses ThreadPoolExecutor with
  max 3 workers to respect Jikan API's ~3 req/s rate limit. Includes retry logic for 429/5xx errors.

  **Idempotency:** Skips existing GCS blobs to avoid redundant API calls and ensure safe re-runs.
  Each JSON file is named characters/anime_{id}.json containing the anime_id and characters array.
connection: gcp-default
tags:
  - domain:entertainment
  - data_type:external_source
  - pipeline_role:raw_ingestion
  - sensitivity:public
  - update_pattern:batch_daily
  - source_system:jikan_api
  - content_type:character_data
  - anime_data
owner: DE-Zoomcamp-Student

columns:
  - name: anime_id
    description: MyAnimeList anime identifier linking back to the source anime
  - name: characters
    description: Array of main character objects for this anime

@bruin"""

import csv, json, os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

from modules import get_character_data

BUCKET_NAME = "jikan_anime_data_bucket"
SEED_CSV    = os.path.join(PROJECT_ROOT, "pipeline", "assets", "seeds", "dim_anime.csv")


def load_anime_ids():
    with open(SEED_CSV, encoding="utf-8") as f:
        return [int(row["anime_id"]) for row in csv.DictReader(f)]


def upload_one(client: storage.Client, anime_id: int):
    bucket    = client.bucket(BUCKET_NAME)
    blob_name = f"characters/anime_{anime_id}.json"
    blob      = bucket.blob(blob_name)

    if blob.exists():
        return anime_id, "skipped"

    data = get_character_data(anime_id)
    if not data:
        return anime_id, "failed"

    blob.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")
    time.sleep(0.4)
    return anime_id, "uploaded"


def main():
    client    = storage.Client()
    anime_ids = load_anime_ids()
    uploaded = skipped = failed = 0

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(upload_one, client, aid): aid for aid in anime_ids}
        for future in as_completed(futures):
            aid, status = future.result()
            if status == "uploaded":
                uploaded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1
                print(f"  FAILED: anime_{aid}")

    print(f"Characters — uploaded: {uploaded}, skipped: {skipped}, failed: {failed}")


main()

"""@bruin

name: ingest.fetch_episodes
description: |
  Ingests episode-level metadata from MyAnimeList via Jikan API for the top 250 anime.

  Fetches comprehensive episode data including title, score, and filler episode flags
  from the Jikan API (MyAnimeList's public API). Data is paginated per anime and
  uploaded as JSON objects to GCS with idempotent behavior (skips existing files).

  This asset serves as the first stage in the episode data pipeline, creating raw
  JSON files that are later loaded into BigQuery and transformed into structured
  episode analytics tables.

  Rate-limited to 2 concurrent workers with 0.4s delays between paginated requests
  to respect Jikan API limits. Episodes are fetched for all anime IDs from the
  dim_anime seed file (top 250 by popularity).

  Output files: episodes/anime_{anime_id}.json in GCS bucket
tags:
  - domain:entertainment
  - data_type:external_source
  - pipeline_role:raw
  - sensitivity:public
  - update_pattern:idempotent
  - source:jikan_api

columns:
  - name: anime_id
    type: integer
    description: MyAnimeList anime identifier, links to dim_anime seed
    checks:
      - name: not_null
  - name: episodes
    type: array
    description: Array of episode objects for this anime
    checks:
      - name: not_null
  - name: episodes.episode_id
    type: integer
    description: MyAnimeList episode identifier (mal_id)
  - name: episodes.title
    type: string
    description: Episode title, may be in Japanese or English
  - name: episodes.score
    type: float
    description: Community rating score for this specific episode (0.0-10.0)
  - name: episodes.filler
    type: boolean
    description: True if episode is filler content (not canon to main story)

@bruin"""

import csv, json, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

from modules import get_episode_data

BUCKET_NAME = "jikan_anime_data_bucket"
SEED_CSV    = os.path.join(PROJECT_ROOT, "pipeline", "assets", "seeds", "dim_anime.csv")


def load_anime_ids():
    with open(SEED_CSV, encoding="utf-8") as f:
        return [int(row["anime_id"]) for row in csv.DictReader(f)]


def upload_one(client: storage.Client, anime_id: int):
    bucket    = client.bucket(BUCKET_NAME)
    blob_name = f"episodes/anime_{anime_id}.json"
    blob      = bucket.blob(blob_name)

    if blob.exists():
        return anime_id, "skipped"

    data = get_episode_data(anime_id)
    if not data or not data.get("episodes"):
        return anime_id, "failed"

    blob.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")
    return anime_id, "uploaded"


def main():
    client    = storage.Client()
    anime_ids = load_anime_ids()
    uploaded = skipped = failed = 0

    # Episodes are paginated — keep workers low to avoid hammering the API
    with ThreadPoolExecutor(max_workers=2) as executor:
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

    print(f"Episodes — uploaded: {uploaded}, skipped: {skipped}, failed: {failed}")


main()

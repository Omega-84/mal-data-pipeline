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
  to respect Jikan API limits (~3 req/s max). Episodes are fetched for all anime IDs
  from the dim_anime seed file (top 250 by popularity). Long-running anime series
  (500+ episodes like Naruto, One Piece) will have multiple API pages and longer
  processing times.

  Each anime generates one JSON file containing an array of all episodes. Episode
  volumes vary significantly: movies have 1 episode, standard series have 12-24
  episodes, long-running series can have 500+ episodes. Total expected output:
  ~250 JSON files ranging from 1KB (movies) to 100KB+ (long series).

  Output files: episodes/anime_{anime_id}.json in GCS bucket
  Refresh pattern: Daily idempotent execution (skips existing files)
  Failure handling: Individual anime failures logged but don't stop the batch
  Typical execution time: 15-25 minutes for full refresh (highly variable based on API response times)
  SLA compliance: Designed to complete within 30-minute window for daily batch processing
tags:
  - domain:entertainment
  - data_type:external_source
  - pipeline_role:raw
  - sensitivity:public
  - update_pattern:idempotent
  - source:jikan_api
  - refresh_cadence:daily
  - data_format:json
  - api_paginated:true
  - rate_limited:true
  - volume:medium
  - api_dependency:jikan
  - execution_time:long
  - failure_resilient:true
  - concurrency:limited

columns:
  - name: anime_id
    type: integer
    description: MyAnimeList anime identifier, foreign key to dim_anime seed file. Ranges from low values (e.g., 20 for Naruto) to high values (38000+ for recent anime)
    checks:
      - name: not_null
  - name: episodes
    type: array
    description: Array of episode objects for this anime. Length varies dramatically by series type - movies have 1 episode, seasonal anime have 12-24, long-running series have 500+. Empty arrays possible for unreleased anime. Nested structure flattened in downstream load_episodes asset for episode-level analytics
    checks:
      - name: not_null
  - name: episodes.episode_id
    type: integer
    description: MyAnimeList episode identifier (mal_id). Unique across all episodes in MAL database. Used for deep-linking to episode pages on myanimelist.net
    checks:
      - name: not_null
  - name: episodes.title
    type: string
    description: Episode title as provided by MAL community. Language varies - may be Japanese (kanji/hiragana), English, or romanized Japanese. Some episodes have generic titles like "Episode 1" for unaired content
  - name: episodes.score
    type: float
    description: Community rating score for individual episode on 0.0-10.0 scale. Null for episodes with insufficient votes (<5-10 ratings). Derived from user votes, typically lower variance than anime-level scores. Approximately 30-40% of episodes lack scores due to low engagement
  - name: episodes.filler
    type: boolean
    description: Canon status flag - true if episode is filler content (anime-original, not adapted from source manga/novel), false for canon episodes. Particularly relevant for long-running shonen series. May be null for series where filler status is disputed or unknown

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

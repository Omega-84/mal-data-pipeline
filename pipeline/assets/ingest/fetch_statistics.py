"""@bruin

name: ingest.fetch_statistics
description: |
  Ingests anime viewer engagement statistics and score distributions from the Jikan API (MyAnimeList proxy)
  for a curated list of popular anime titles. Fetches comprehensive user interaction metrics including viewing
  status counts, completion rates, and detailed score distributions (1-10 scale with vote counts and percentages).

  Data is sourced from the /anime/{id}/statistics endpoint and stored as individual JSON files in GCS under
  the statistics/ prefix. The process is fully idempotent, skipping existing files and enabling safe re-runs.

  This asset serves as the foundation for understanding anime popularity trends, user engagement patterns,
  and rating distributions - critical for recommendation systems and content analysis downstream.
tags:
  - anime
  - statistics
  - engagement
  - ratings
  - external_api
  - jikan
  - myanimelist
  - user_behavior
  - ingest
  - raw
  - json

materialization:
  type: file

depends:
  - mal_pipeline.dim_anime

columns:
  - name: anime_id
    type: integer
    description: Unique identifier for the anime title from MyAnimeList database
    checks:
      - name: not_null
      - name: unique
  - name: watching
    type: integer
    description: Number of users currently watching this anime
    checks:
      - name: not_null
  - name: completed
    type: integer
    description: Number of users who have completed watching this anime
    checks:
      - name: not_null
  - name: on_hold
    type: integer
    description: Number of users who have paused watching and may resume later
    checks:
      - name: not_null
  - name: dropped
    type: integer
    description: Number of users who stopped watching and don't plan to resume
    checks:
      - name: not_null
  - name: plan_to_watch
    type: integer
    description: Number of users who have added this anime to their watchlist but haven't started
    checks:
      - name: not_null
  - name: total
    type: integer
    description: Total number of users who have interacted with this anime across all statuses
    checks:
      - name: not_null
  - name: score_1_votes
    type: integer
    description: Number of users who rated this anime 1/10 (lowest score)
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
    description: Number of users who rated this anime 5/10 (average)
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
    description: Number of users who rated this anime 10/10 (highest score)
  - name: score_1_pct
    type: float
    description: Percentage of total ratings that are 1/10 scores
  - name: score_2_pct
    type: float
    description: Percentage of total ratings that are 2/10 scores
  - name: score_3_pct
    type: float
    description: Percentage of total ratings that are 3/10 scores
  - name: score_4_pct
    type: float
    description: Percentage of total ratings that are 4/10 scores
  - name: score_5_pct
    type: float
    description: Percentage of total ratings that are 5/10 scores
  - name: score_6_pct
    type: float
    description: Percentage of total ratings that are 6/10 scores
  - name: score_7_pct
    type: float
    description: Percentage of total ratings that are 7/10 scores
  - name: score_8_pct
    type: float
    description: Percentage of total ratings that are 8/10 scores
  - name: score_9_pct
    type: float
    description: Percentage of total ratings that are 9/10 scores
  - name: score_10_pct
    type: float
    description: Percentage of total ratings that are 10/10 scores

@bruin"""

import csv, json, os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

from modules import get_anime_statistics

BUCKET_NAME = "jikan_anime_data_bucket"
SEED_CSV    = os.path.join(PROJECT_ROOT, "pipeline", "assets", "seeds", "dim_anime.csv")


def load_anime_ids():
    with open(SEED_CSV, encoding="utf-8") as f:
        return [int(row["anime_id"]) for row in csv.DictReader(f)]


def upload_one(client: storage.Client, anime_id: int):
    bucket    = client.bucket(BUCKET_NAME)
    blob_name = f"statistics/anime_{anime_id}.json"
    blob      = bucket.blob(blob_name)

    if blob.exists():
        return anime_id, "skipped"

    data = get_anime_statistics(anime_id)
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

    print(f"Statistics — uploaded: {uploaded}, skipped: {skipped}, failed: {failed}")


main()

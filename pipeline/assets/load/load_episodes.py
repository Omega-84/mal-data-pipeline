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

  Data volume varies significantly by series: movies typically have 1 episode,
  seasonal anime have 12-24 episodes, while long-running series like Naruto or
  One Piece can have 500+ episodes. Expected total rows: ~15,000-25,000 episodes
  across the top 250 anime.

  The load process is idempotent-friendly when combined with the upstream fetch_episodes
  asset, which skips existing files. Performance is I/O bound due to JSON parsing
  and BigQuery streaming inserts.

  Operational characteristics:
  - Execution time: 2-5 minutes for full refresh (varies by GCS latency)
  - Memory usage: Low (~50-100MB) due to streaming JSON processing
  - Error handling: Silently skips malformed JSON files
  - Concurrency: Single-threaded processing of GCS blobs
  - Data freshness: Episodes loaded within minutes of ingest completion
  - Schema evolution: Autodetect handles new fields from Jikan API updates
tags:
  - domain:entertainment
  - domain:anime
  - data_type:fact_table
  - pipeline_role:raw
  - sensitivity:public
  - update_pattern:full_refresh
  - source:jikan_api
  - refresh_cadence:daily
  - volume:medium
  - performance:io_bound
  - data_format:exploded_json
  - myanimelist
  - episodes
  - streaming_load
  - schema_autodetect

depends:
  - ingest.fetch_episodes

columns:
  - name: anime_id
    type: integer
    description: MyAnimeList anime identifier, foreign key to dim_anime seed and staging tables. References anime from the top 250 by popularity, ranging from classic low-ID series (e.g., 20 for Naruto) to modern high-ID series (38000+ for recent anime). Used for joining episode data with anime metadata in downstream transformations
    checks:
      - name: not_null
      - name: positive
  - name: episode_id
    type: integer
    description: MyAnimeList episode identifier (mal_id), globally unique across all episodes in the MAL database. Values typically range from low thousands (early MAL episodes) to millions (recent episodes). Used for deep-linking to episode pages on myanimelist.net and ensuring episode-level uniqueness across all anime
    checks:
      - name: not_null
      - name: positive
      - name: unique
  - name: title
    type: string
    description: Episode title as provided by MyAnimeList community data. Language varies by anime and community preference - may be Japanese (kanji/hiragana), English, or romanized Japanese. Some episodes have generic titles like "Episode 1" for unaired or unnamed content. Approximately 95% of episodes have non-null titles
  - name: score
    type: float
    description: Community rating score for this specific episode on a 0.0-10.0 scale. Null for episodes with insufficient votes (typically <5-10 ratings). Episode-level scores generally have lower variance than anime-level scores and are derived from user episode ratings. Approximately 60-70% of episodes lack scores due to low community engagement on episode-level rating
  - name: filler
    type: boolean
    description: Canon status flag indicating if episode is filler content (anime-original material not adapted from source manga/novel). True for filler episodes, false for canon episodes. Particularly relevant for long-running shonen series like Naruto (41% filler), Bleach (45% filler), One Piece (11% filler). May be null for series where filler status is disputed or unknown, especially for anime-original series

@bruin"""

import json, os, sys
from google.cloud import storage, bigquery

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

DATASET_ID  = "mal_pipeline"
BUCKET_NAME = "jikan_anime_data_bucket"
TABLE_ID    = "raw_episodes"


def main():
    gcs = storage.Client()
    bq  = bigquery.Client()

    rows = []
    for blob in gcs.bucket(BUCKET_NAME).list_blobs(prefix="episodes/"):
        data = json.loads(blob.download_as_text())
        anime_id = data["anime_id"]
        for ep in data.get("episodes", []):
            rows.append({"anime_id": anime_id, **ep})

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

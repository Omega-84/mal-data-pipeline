"""@bruin

name: ingest.fetch_descriptions
description: |
  Primary ingestion point that fetches comprehensive anime metadata from the Jikan API (MyAnimeList)
  and uploads structured JSON files to Google Cloud Storage. This asset processes a curated set of
  ~250 popular anime from the dim_anime.csv seed file and extracts rich metadata including titles,
  ratings, genres, studios, synopsis, and production details.

  Technical implementation uses concurrent API calls with ThreadPoolExecutor (3 workers) to the
  Jikan v4 /anime/{id}/full endpoint, respecting the API rate limit with 0.4s delays between requests.
  Each successful response is normalized into a consistent JSON schema and stored in GCS under the
  "descriptions/" prefix. The process is idempotent - skips already-uploaded files to support
  incremental pipeline runs and recovery scenarios.

  This data feeds downstream into the load_descriptions → stg_descriptions → intermediate/mart
  layers where it powers the anime recommendation system with vector embeddings and analytics.
  Expected output size: ~250 JSON files totaling ~2-3MB. Refresh cadence: daily via pipeline schedule.
connection: gcp-default
tags:
  - domain:entertainment
  - domain:anime
  - data_type:external_source
  - data_type:api_ingestion
  - pipeline_role:raw
  - pipeline_role:ingestion
  - sensitivity:public
  - update_pattern:snapshot
  - source_system:jikan_api
  - source_system:myanimelist
  - freshness:daily
  - format:json
  - storage_layer:gcs
  - api_version:v4
  - concurrency:threaded
  - execution_time:medium
  - dependency:seeds
  - monitoring:required
  - sla:daily_completion

depends:
  - mal_pipeline.dim_anime
owner: Omega-84

secrets:
  - key: gcp-default
    inject_as: gcp-default

columns:
  - name: anime_id
    type: INTEGER
    description: |
      Unique MyAnimeList identifier for the anime. Primary key for joining with characters,
      episodes, and statistics data. Range typically 1-50000+ for active anime entries.
    checks:
      - name: not_null
      - name: unique
  - name: title
    type: STRING
    description: |
      Primary title of the anime, typically in Japanese romanized form (romaji). This is the
      canonical identifier used across the MAL platform and rarely null.
    checks:
      - name: not_null
  - name: title_english
    type: STRING
    description: |
      Official English localized title when available. Null for anime without official English
      releases or where English title matches the primary title.
  - name: title_japanese
    type: STRING
    description: |
      Original Japanese title in native characters (kanji/hiragana/katakana). Provides authentic
      representation for cultural context and Japanese-language search.
  - name: status
    type: STRING
    description: |
      Current airing/release status. Common values: "Finished Airing", "Currently Airing",
      "Not yet aired". Used for filtering active vs completed series in analytics.
    checks:
      - name: not_null
  - name: airing
    type: BOOLEAN
    description: |
      Boolean flag indicating if anime is actively broadcasting new episodes. Derived from status
      but provides direct boolean logic for active series filtering.
  - name: score
    type: FLOAT
    description: |
      Community rating score on MyAnimeList scale 0.0-10.0. Null for new/unrated anime with
      insufficient vote counts. Higher scores indicate better community reception.
  - name: rank
    type: INTEGER
    description: |
      Current ranking position on MyAnimeList based on weighted scoring algorithm. Lower numbers
      indicate higher rank. Null for unranked anime or those with insufficient data.
  - name: popularity
    type: INTEGER
    description: |
      Popularity ranking based on number of members who added anime to their lists. Differs from
      score-based rank - measures engagement breadth rather than quality perception.
  - name: synopsis
    type: STRING
    description: |
      Plot summary and description text. May contain spoiler warnings for major plot reveals.
      Critical field for content-based recommendations and search functionality.
  - name: year
    type: INTEGER
    description: |
      Year the anime first aired or was released. Extracted from airing_start timestamp for
      temporal analysis and decade-based filtering.
  - name: rating
    type: STRING
    description: |
      Content rating/age classification following international standards. Common values: "G",
      "PG", "PG-13", "R". Used for age-appropriate content filtering.
  - name: image_url
    type: STRING
    description: |
      URL to large cover image from MyAnimeList CDN. Typically jpg format, ~225x350px resolution.
      Essential for visual presentation in dashboard and recommendation displays.
    checks:
      - name: not_null
  - name: airing_start
    type: STRING
    description: |
      ISO 8601 timestamp when anime first aired/was released. Stored as string from API response,
      converted to TIMESTAMP in staging layer for temporal queries and partitioning.
  - name: airing_end
    type: STRING
    description: |
      ISO 8601 timestamp when anime finished airing. Null for ongoing series or TBA releases.
      Used to calculate series duration and identify completed vs active content.
  - name: studios
    type: STRING
    description: |
      Comma-separated list of animation studios involved in production. Key dimension for studio
      analysis, recommendations by production house, and industry network analysis.
  - name: genres
    type: STRING
    description: |
      Comma-separated genre classifications (Action, Comedy, Drama, Romance, etc.). Primary
      dimension for content-based filtering and recommendation algorithms. Critical for genre
      distribution analysis.
  - name: themes
    type: STRING
    description: |
      Comma-separated thematic tags that complement genres (School, Military, Supernatural, Isekai).
      Provides more granular content categorization beyond standard genre classifications.
  - name: anime_type
    type: STRING
    description: |
      Format type classification. Common values: "TV", "Movie", "OVA", "Special", "ONA". Used
      for consumption pattern analysis and format-specific recommendations.
    checks:
      - name: not_null
  - name: source
    type: STRING
    description: |
      Original source material the anime was adapted from. Values include: "Manga", "Light novel",
      "Original", "Game", "Novel". Important for adaptation analysis and source-based discovery.
  - name: demographics
    type: STRING
    description: |-
      Comma-separated target demographic classifications (Shounen, Seinen, Josei, Shoujo, Kids).
      Critical for audience targeting and age-appropriate recommendation filtering.

@bruin"""

import csv, json, os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

from modules import get_anime_data

BUCKET_NAME = "jikan_anime_data_bucket"
SEED_CSV    = os.path.join(PROJECT_ROOT, "pipeline", "assets", "seeds", "dim_anime.csv")


def load_anime_ids():
    with open(SEED_CSV, encoding="utf-8") as f:
        return [int(row["anime_id"]) for row in csv.DictReader(f)]


def upload_one(client: storage.Client, anime_id: int):
    bucket    = client.bucket(BUCKET_NAME)
    blob_name = f"descriptions/anime_{anime_id}.json"
    blob      = bucket.blob(blob_name)

    if blob.exists():
        return anime_id, "skipped"

    data = get_anime_data(anime_id)
    if not data:
        return anime_id, "failed"

    blob.upload_from_string(json.dumps(data, ensure_ascii=False), content_type="application/json")
    time.sleep(0.4)  # stay under Jikan rate limit
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

    print(f"Descriptions — uploaded: {uploaded}, skipped: {skipped}, failed: {failed}")


main()

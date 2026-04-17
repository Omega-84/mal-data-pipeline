"""@bruin

name: load.load_descriptions
description: |
  Loads anime metadata JSON files from GCS into BigQuery raw table for the MAL data pipeline.

  This asset transfers structured anime data previously fetched from the Jikan API (MyAnimeList)
  from Google Cloud Storage into a raw BigQuery table. Each JSON file contains comprehensive
  metadata for a single anime including titles, ratings, synopsis, production details, and
  temporal information. The load operation uses WRITE_TRUNCATE to ensure data freshness and
  BigQuery's autodetect to infer the schema from the JSON structure.

  Data flows from Jikan API → GCS (via fetch_descriptions) → BigQuery raw table →
  staging views → mart tables for analytics and ML recommendations.

  **Operational characteristics:**
  - Expected volume: ~250 anime records from dim_anime.csv seed file
  - Data size: ~2-3MB total JSON payload when fully loaded
  - Refresh pattern: Daily via @daily pipeline schedule with full snapshot refresh
  - Load strategy: WRITE_TRUNCATE ensures clean state, autodetect handles schema evolution
  - Dependencies: Requires fetch_descriptions to complete and populate GCS bucket
  - Downstream impact: Feeds stg_descriptions view and all mart-layer analytics tables
  - Performance: Typically completes in <30 seconds for full 250-record load
connection: gcp-default
tags:
  - domain:entertainment
  - domain:anime
  - data_type:external_source
  - data_type:api_ingestion
  - pipeline_role:raw
  - pipeline_role:load_layer
  - update_pattern:snapshot
  - sensitivity:public
  - source:jikan_api
  - source_system:myanimelist
  - content:anime_metadata
  - freshness:daily
  - format:json
  - storage_layer:bigquery
  - load_strategy:truncate_reload
  - volume:small
  - api_version:v4

depends:
  - ingest.fetch_descriptions
owner: Omega-84

secrets:
  - key: gcp-default
    inject_as: gcp-default

columns:
  - name: anime_id
    type: INTEGER
    description: |
      Primary identifier from MyAnimeList, used for API calls and joins with characters,
      episodes, and statistics datasets. Sourced from dim_anime.csv seed file containing
      ~250 popular anime. Range typically 1-50000+ for active MAL entries.
    checks:
      - name: not_null
      - name: unique
  - name: title
    type: STRING
    description: |
      Primary anime title, typically in Japanese romanized form (romaji). This is the
      canonical identifier used across the MAL platform and serves as the default display name.
    checks:
      - name: not_null
  - name: title_english
    type: STRING
    description: |
      Official English localized title when available. Null for anime without official English
      releases or where the English title matches the primary title. Used for English-language
      search and display in internationalized interfaces.
  - name: title_japanese
    type: STRING
    description: |
      Original Japanese title in native characters (kanji/hiragana/katakana). Provides authentic
      representation for cultural context and Japanese-language search functionality.
  - name: status
    type: STRING
    description: |
      Current airing/release status from MyAnimeList. Standard values include 'Finished Airing',
      'Currently Airing', 'Not yet aired'. Critical for filtering active vs completed series
      in analytics and recommendations.
    checks:
      - name: not_null
  - name: airing
    type: BOOLEAN
    description: |
      Boolean flag indicating if anime is actively broadcasting new episodes. Derived from status
      field but provides direct boolean logic for filtering currently airing series.
  - name: score
    type: FLOAT
    description: |
      Community average rating on MyAnimeList 0.0-10.0 scale. Null for new/unrated anime with
      insufficient vote counts. Higher scores indicate better community reception. Used in
      ranking algorithms and quality-based filtering.
  - name: rank
    type: INTEGER
    description: |
      Current ranking position on MyAnimeList based on weighted scoring algorithm. Lower numbers
      indicate higher rank (1 = best). Null for unranked anime with insufficient rating data.
      Used for top-N recommendations and popularity analysis.
  - name: popularity
    type: INTEGER
    description: |
      Popularity ranking based on number of members who added anime to their MAL lists. Differs
      from score-based rank - measures engagement breadth rather than quality perception.
      Lower numbers indicate higher popularity.
    checks:
      - name: not_null
  - name: synopsis
    type: STRING
    description: |
      Plot summary and description text from MyAnimeList. May contain spoiler warnings for
      major plot reveals. Critical field for content-based recommendations, search functionality,
      and vector embedding generation in the recommendation system.
  - name: year
    type: INTEGER
    description: |
      Year the anime first aired or was released, extracted from airing_start timestamp.
      Used for temporal analysis, decade-based filtering, and historical trend analysis.
  - name: rating
    type: STRING
    description: |
      Content rating/age classification following international standards. Common values include
      'G' (General), 'PG' (Parental Guidance), 'PG-13' (13+), 'R' (Restricted). Used for
      age-appropriate content filtering in family-safe recommendation modes.
  - name: image_url
    type: STRING
    description: |
      URL to large cover image from MyAnimeList CDN, typically JPG format at ~225x350px resolution.
      Essential for visual presentation in dashboard and recommendation displays. Images hosted
      on cdn.myanimelist.net domain.
    checks:
      - name: not_null
  - name: airing_start
    type: STRING
    description: |
      ISO 8601 timestamp string when anime first aired/was released. Stored as string from Jikan
      API response, converted to TIMESTAMP in staging layer for temporal queries and date-based
      partitioning in mart tables.
  - name: airing_end
    type: STRING
    description: |
      ISO 8601 timestamp string when anime finished airing. Null for ongoing series, upcoming
      releases, or single-episode formats. Used to calculate series duration and identify
      completed vs active content for binge-watching recommendations.
  - name: studios
    type: STRING
    description: |
      Comma-separated list of animation studios involved in production (e.g., 'Studio Ghibli',
      'Mappa', 'Wit Studio'). Key dimension for studio-based analysis, recommendations by
      production house, and industry network analysis.
  - name: genres
    type: STRING
    description: |
      Comma-separated genre classifications from MyAnimeList taxonomy (Action, Comedy, Drama,
      Romance, Slice of Life, etc.). Primary dimension for content-based filtering and
      recommendation algorithms. Essential for genre distribution analysis.
  - name: themes
    type: STRING
    description: |
      Comma-separated thematic tags that complement standard genres (School, Military, Supernatural,
      Isekai, Time Travel). Provides more granular content categorization for specialized
      recommendation queries and thematic analysis.
  - name: anime_type
    type: STRING
    description: |
      Format/medium classification from MyAnimeList. Standard values: 'TV' (television series),
      'Movie' (theatrical film), 'OVA' (original video animation), 'Special' (specials/extras),
      'ONA' (original net animation). Used for format-specific recommendations and consumption analysis.
    checks:
      - name: not_null
  - name: source
    type: STRING
    description: |
      Original source material the anime was adapted from. Common values include 'Manga',
      'Light novel', 'Original' (anime-original), 'Game', 'Novel', 'Web manga'. Important for
      adaptation analysis and source-based discovery patterns.
  - name: demographics
    type: STRING
    description: |-
      Comma-separated target demographic classifications (Shounen, Seinen, Josei, Shoujo, Kids).
      Shounen (young male), Seinen (adult male), Shoujo (young female), Josei (adult female).
      Critical for audience targeting and demographically-appropriate recommendation filtering.

@bruin"""

import json, os, sys
from google.cloud import storage, bigquery

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

DATASET_ID  = "mal_pipeline"
BUCKET_NAME = "jikan_anime_data_bucket"
TABLE_ID    = "raw_descriptions"


def main():
    gcs = storage.Client()
    bq  = bigquery.Client()

    rows = []
    for blob in gcs.bucket(BUCKET_NAME).list_blobs(prefix="descriptions/"):
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

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

type: python
connection: google_cloud_platform
materialization:
  type: table

depends:
  - ingest.fetch_descriptions

columns:
  - name: anime_id
    description: "Primary identifier from MyAnimeList, used for API calls and joins"

  - name: title
    description: "Primary anime title, typically in romanized form"

  - name: title_english
    description: "Official English title, may be null for anime without English releases"

  - name: title_japanese
    description: "Original Japanese title in native characters"

  - name: status
    description: "Current airing status (e.g., 'Currently Airing', 'Finished Airing', 'Not yet aired')"

  - name: airing
    description: "Boolean indicating if the anime is currently airing"

  - name: score
    description: "Average user rating on MyAnimeList (0-10 scale), null for unrated anime"

  - name: rank
    description: "Ranking position based on score among all anime, null for unranked"

  - name: popularity
    description: "Popularity rank based on number of users who added it to their lists"

  - name: synopsis
    description: "Plot summary and description, may contain spoiler warnings"

  - name: year
    description: "Year of initial airing, extracted from aired.from date"

  - name: rating
    description: "Content rating (e.g., 'PG-13', 'R', 'G') for audience appropriateness"

  - name: image_url
    description: "URL to large cover image from MyAnimeList CDN"

  - name: airing_start
    description: "Timestamp when anime first aired, ISO format string from Jikan API"

  - name: airing_end
    description: "Timestamp when anime finished airing, null for ongoing or unaired series"

  - name: studios
    description: "Comma-separated list of animation studios involved in production"

  - name: genres
    description: "Comma-separated genre classifications (e.g., 'Action', 'Drama', 'Romance')"

  - name: themes
    description: "Comma-separated thematic tags that don't fit standard genres"

  - name: anime_type
    description: "Format type such as 'TV', 'Movie', 'OVA', 'Special', 'ONA'"

  - name: source
    description: "Original source material (e.g., 'Manga', 'Light novel', 'Original', 'Game')"

  - name: demographics
    description: "Target demographic classifications like 'Shounen', 'Seinen', 'Josei'"

tags:
  - domain:entertainment
  - data_type:external_source
  - pipeline_role:raw
  - update_pattern:snapshot
  - sensitivity:public
  - source:jikan_api
  - content:anime_metadata

owner: "Omega-84"

@bruin"""

import json, os, sys
from google.cloud import storage, bigquery

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

PROJECT_ID  = "de-zoomcamp-485104"
DATASET_ID  = "mal_pipeline"
BUCKET_NAME = "jikan_anime_data_bucket"
TABLE_ID    = "raw_descriptions"


def main():
    gcs = storage.Client()
    bq  = bigquery.Client(project=PROJECT_ID)

    rows = []
    for blob in gcs.bucket(BUCKET_NAME).list_blobs(prefix="descriptions/"):
        rows.append(json.loads(blob.download_as_text()))

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

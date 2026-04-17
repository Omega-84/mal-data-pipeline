/* @bruin

name: mal_pipeline.stg_characters
type: bq.sql
description: |
  Staging view that standardizes character data types and provides a clean interface
  for main anime character information sourced from MyAnimeList. This view transforms
  the raw character data loaded from GCS JSON files into properly typed columns
  suitable for downstream analytics and dashboard consumption.

  The view performs essential type casting from the autodetected raw schema to
  explicit INT64 types for identifiers, ensuring consistent data types across
  the pipeline. It serves as the standardized staging layer between raw ingestion
  and mart consumption, focusing exclusively on main characters (role="Main")
  from approximately 250 popular anime titles.

  **Data Lineage:** Jikan API → GCS JSON → raw_characters → stg_characters → mart_characters
  **Data Volume:** ~500-2000 character records (2-8 main characters per anime)
  **Refresh Pattern:** Daily refresh via dependent load asset, view auto-refreshes on query
  **Downstream Usage:** Primary input for mart_characters table that powers character
  browsing and recommendation features in the OtakuLens dashboard
tags:
  - domain:entertainment
  - data_type:dimension_table
  - pipeline_role:staging
  - sensitivity:public
  - update_pattern:dependent_refresh
  - content_type:character_data
  - anime_data
  - mal_dataset

materialization:
  type: view

depends:
  - load.load_characters
owner: DE-Zoomcamp-Student

columns:
  - name: anime_id
    type: INTEGER
    description: |
      MyAnimeList anime identifier (foreign key) linking character records back to their
      source anime series. References the curated anime list from dim_anime.csv seed data
      containing ~250 popular titles. Used for joins with anime metadata tables and
      enables character-to-anime relationship queries in downstream analytics.
    checks:
      - name: not_null
  - name: character_id
    type: INTEGER
    description: |
      Unique MyAnimeList character identifier serving as the natural primary key.
      Derived from the mal_id field in Jikan API responses and used for character
      deduplication, primary key constraints in downstream marts, and as the anchor
      for character-based recommendation algorithms. Each character should appear
      only once across all anime.
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: STRING
    description: |
      Full character name as catalogued by the MyAnimeList community, typically
      in "Family Name Given Name" format for Japanese characters or localized
      name format for non-Japanese characters. Serves as the primary display label
      for character search and browsing functionality in dashboard interfaces.
      Expected to be populated for all main characters.
    checks:
      - name: not_null
  - name: image_url
    type: STRING
    description: |-
      Direct HTTPS URL to the character's official profile image hosted on MyAnimeList's
      CDN infrastructure, typically in JPG format. Enables visual character displays,
      grids, and profile pages in the OtakuLens dashboard without requiring separate
      image hosting. URLs follow the pattern "https://cdn.myanimelist.net/images/characters/..."
      and are stable for the lifetime of the character record.
    checks:
      - name: not_null

@bruin */

SELECT
    CAST(anime_id      AS INT64)  AS anime_id,
    CAST(character_id  AS INT64)  AS character_id,
    name,
    image_url
FROM `mal_pipeline.raw_characters`

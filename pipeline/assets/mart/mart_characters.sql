/* @bruin

name: mal_pipeline.mart_characters
type: bq.sql
description: |
  Final consumption table containing main characters enriched with anime context for the
  OtakuLens dashboard. Combines character data from MyAnimeList with anime titles to enable
  character-based recommendations and browsing features.

  This mart table serves as the primary source for character-related analytics and visualizations,
  providing a denormalized view that joins character profiles with their source anime context.
  Each row represents a main character from an anime series, making it optimized for queries
  that need both character details and anime metadata.

  **Business Use Cases:** Character grid displays, character-based recommendations, anime-character
  relationship analysis, dashboard search functionality
  **Data Lineage:** Jikan API → GCS → raw_characters → stg_characters + stg_descriptions → mart_characters
  **Refresh Pattern:** Daily full refresh, rebuilds from staging views
  **Data Volume:** ~500-2000 character records (2-8 main characters per ~250 anime)
  **Performance:** Sub-second queries for character browsing, optimized for dashboard pagination
  **Typical Queries:** Character browsing by anime, character image galleries, recommendation algorithms
tags:
  - domain:entertainment
  - data_type:fact_table
  - pipeline_role:mart
  - sensitivity:public
  - update_pattern:full_refresh
  - content_type:character_data
  - anime_data
  - mal_dataset
  - dashboard_ready
  - visual_content

materialization:
  type: table

depends:
  - mal_pipeline.stg_characters
  - mal_pipeline.stg_descriptions
owner: DE-Zoomcamp-Student

columns:
  - name: anime_id
    type: INTEGER
    description: MyAnimeList anime identifier linking characters to their source anime series. Foreign key enabling joins with mart_anime and other anime-related tables for cross-dimensional analysis.
    checks:
      - name: not_null
  - name: anime_title
    type: STRING
    description: Primary title of the anime series (typically Japanese romanized title). Enriched from stg_descriptions to provide anime context without requiring additional joins for character-based queries.
    checks:
      - name: not_null
  - name: character_id
    type: INTEGER
    description: Unique MyAnimeList character identifier serving as the primary key. Used for character-based deduplication, joins, and as the anchor for character recommendation algorithms.
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: STRING
    description: Full name of the main character as catalogued by MyAnimeList community. Primary field for character search functionality and display labels in dashboard interfaces.
    checks:
      - name: not_null
  - name: image_url
    type: STRING
    description: |
      Direct HTTPS URL to character's official profile image hosted on MyAnimeList CDN,
      following pattern "https://cdn.myanimelist.net/images/characters/...". Typically
      JPG format with consistent dimensions. Essential for visual character grids,
      profile displays, and image galleries in the OtakuLens dashboard without
      requiring separate image hosting infrastructure. Stable for character lifetime.
    checks:
      - name: not_null

@bruin */

SELECT
    c.anime_id,
    d.title    AS anime_title,
    c.character_id,
    c.name,
    c.image_url
FROM `mal_pipeline.stg_characters` c
LEFT JOIN `mal_pipeline.stg_descriptions` d USING (anime_id)

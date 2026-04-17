/* @bruin

name: mal_pipeline.mart_episodes
type: bq.sql
description: |
  Final episode-level fact table for MyAnimeList analytics, combining episode metadata with anime titles.
  Each row represents a single episode from the top 250 anime by popularity, enriched with anime context for
  dashboard consumption and episode-level analysis. Sourced from Jikan API data and optimized for OtakuLens
  dashboard queries and anime episode exploration.

  This mart table serves as the primary source for episode-level analytics in the OtakuLens dashboard,
  supporting episode discovery, filler content analysis, and episode-specific recommendations. The table
  combines normalized episode metadata from staging layers with anime title context for rich analytical capabilities.

  **Key business uses:**
  - Episode-level content analysis and filtering in dashboard applications
  - Filler vs canon episode identification for content recommendations
  - Episode quality scoring and community rating analysis
  - Episode title search and discovery features
  - Cross-anime episode comparison and analytics

  **Operational characteristics:**
  - Expected volume: 15,000-25,000 episode records across top 250 anime series
  - Refresh pattern: Daily batch processing as part of ELT pipeline (@daily schedule)
  - Data coverage: ~60-70% of episodes have community scores due to rating participation patterns
  - Performance profile: Sub-second response times for typical dashboard filtering operations
  - Downstream consumption: Primarily OtakuLens Streamlit dashboard with BigQuery/DuckDB dual support
tags:
  - domain:entertainment
  - domain:anime
  - data_type:fact_table
  - pipeline_role:mart
  - source:jikan_api
  - source_system:myanimelist
  - update_pattern:daily_batch
  - sensitivity:public
  - quality:production
  - consumption:dashboard
  - consumption:analytics
  - content_type:episodes
  - granularity:episode_level
  - api_version:v4
  - freshness:daily

materialization:
  type: table

depends:
  - mal_pipeline.stg_episodes
  - mal_pipeline.stg_descriptions

columns:
  - name: anime_id
    type: INTEGER
    description: |
      MyAnimeList unique identifier for the anime series, serving as the primary foreign key linking
      to mart_anime and other anime-related tables. Sourced from the top 250 anime by popularity
      seed list. ID ranges from classic series (low IDs ~1-1000) to recent releases (up to 50000+).
      Essential for anime-level aggregations and cross-table joins in dashboard applications.
    checks:
      - name: not_null
  - name: anime_title
    type: STRING
    description: |
      Primary display title of the anime series, typically in romanized Japanese (romaji) format.
      Sourced from MyAnimeList canonical title field for consistent display across dashboard components.
      Used for episode-to-anime mapping, search functionality, and user-facing displays in OtakuLens.
      Denormalized here to avoid JOINs in dashboard queries for better performance.
    checks:
      - name: not_null
  - name: episode_id
    type: INTEGER
    description: |
      MyAnimeList unique identifier for the specific episode (mal_id), serving as the primary key
      for episode-level analysis and joins to episode-specific data. Globally unique across all
      episodes in the MyAnimeList database. Used for deep-linking to episode pages on myanimelist.net
      and episode-specific analytics in dashboard applications. Critical for deduplication and joins.
    checks:
      - name: not_null
      - name: unique
  - name: episode_title
    type: STRING
    description: |
      Display title of the individual episode from MyAnimeList community data. Language varies
      by series and community preference - may be Japanese, English, or romanized format.
      Frequently null for episodes without official titles, untranslated episodes from Japanese-only
      series, or episodes with generic numbering schemes. Some episodes may have generic titles
      like "Episode N" for unnamed content. Used for episode search and detailed episode analytics.
  - name: score
    type: FLOAT
    description: |
      Community rating score for the episode on MyAnimeList's standard 1.0-10.0 scale,
      aggregated from user ratings. Null for episodes with insufficient votes (<5-10 ratings typically)
      or newly added episodes without community engagement. Episode scores generally exhibit lower
      variance than anime-level scores. Used in episode quality filtering and episode-specific
      recommendation algorithms. Expected coverage: ~60-70% of episodes have valid scores.
  - name: is_filler
    type: BOOLEAN
    description: |
      Canon status flag indicating anime-original content not adapted from source material.
      True for filler episodes, false for canon content. Based on community consensus and
      official source material mapping from MyAnimeList contributors. Critical for content
      analysis in long-running series (e.g., Naruto, Bleach, One Piece) where filler content
      can represent 40-60% of total episodes. Essential for recommendation filtering where
      users may want to skip filler content or focus on main storyline progression.
    checks:
      - name: not_null

@bruin */

SELECT
    e.anime_id,
    d.title       AS anime_title,
    e.episode_id,
    e.title       AS episode_title,
    e.score,
    e.is_filler
FROM `mal_pipeline.stg_episodes` e
LEFT JOIN `mal_pipeline.stg_descriptions` d USING (anime_id)

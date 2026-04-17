/* @bruin

name: mal_pipeline.stg_episodes
type: bq.sql
description: |
  Staging view for episode-level data sourced from MyAnimeList via the Jikan API.

  This view applies type casting and standardizes column naming for raw episode data,
  serving as the clean foundation for downstream episode analytics and aggregations.
  Each row represents a single episode from the top 250 anime by popularity.

  Key transformations:
  - Standardizes data types for BigQuery analytics (INT64, FLOAT64, BOOL)
  - Renames 'filler' to 'is_filler' for semantic clarity
  - Preserves all episode metadata from the upstream API response

  The view serves two primary downstream consumers:
  - int_episode_agg: Episode-level aggregations by anime series
  - mart_episodes: Final episode fact table with anime title enrichment

  Data characteristics:
  - Expected volume: 15,000-25,000 episodes across top 250 anime
  - Refresh pattern: Daily as part of ELT pipeline (@daily schedule)
  - Score coverage: ~60-70% of episodes (many lack sufficient community ratings)
  - Filler analysis: Particularly valuable for long-running shonen series
  - Episode distribution: Varies widely from 1 episode (movies) to 500+ (long-running series)
  - Performance profile: View materialization typically completes in seconds due to simple type casting
  - Data lineage: Raw episodes → stg_episodes → int_episode_agg + mart_episodes
tags:
  - domain:entertainment
  - domain:anime
  - data_type:staging_view
  - pipeline_role:staging
  - source:jikan_api
  - sensitivity:public
  - update_pattern:daily_refresh
  - content_type:episodes
  - myanimelist
  - granularity:episode_level
  - quality:production
  - api_version:v4
  - type_casting

materialization:
  type: view

depends:
  - load.load_episodes

columns:
  - name: anime_id
    type: INTEGER
    description: MyAnimeList anime identifier, foreign key to anime-related tables. References top 250 anime by popularity, with IDs ranging from classic series (low IDs) to recent releases (38000+)
    checks:
      - name: not_null
      - name: positive
  - name: episode_id
    type: INTEGER
    description: MyAnimeList episode identifier (mal_id), globally unique across all episodes. Used for deep-linking to episode pages on myanimelist.net and episode-level joins
    checks:
      - name: not_null
      - name: positive
      - name: unique
  - name: title
    type: STRING
    description: Episode title from MyAnimeList community data. Language varies by series and community preference - may be Japanese, English, or romanized. Some episodes have generic titles like "Episode N" for unnamed content
  - name: score
    type: FLOAT
    description: Community episode rating on 0.0-10.0 scale. Null for episodes with insufficient votes (<5-10 ratings typically). Episode scores generally have lower variance than anime-level scores
  - name: is_filler
    type: BOOLEAN
    description: Canon status flag indicating anime-original content not adapted from source material. True for filler episodes, false for canon. Critical for content analysis in long-running series like Naruto, Bleach, One Piece
    checks:
      - name: not_null

@bruin */

SELECT
    CAST(anime_id    AS INT64)    AS anime_id,
    CAST(episode_id  AS INT64)    AS episode_id,
    title,
    CAST(score       AS FLOAT64)  AS score,
    CAST(filler      AS BOOL)     AS is_filler
FROM `mal_pipeline.raw_episodes`

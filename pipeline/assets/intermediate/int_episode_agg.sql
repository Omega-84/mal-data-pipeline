/* @bruin

name: mal_pipeline.int_episode_agg
type: bq.sql
description: |
  Episode-level aggregations by anime, providing comprehensive episode metrics for MyAnimeList anime series.
  This intermediate table processes episode data from the Jikan API to calculate key statistics used in
  downstream analytics and recommendation systems.

  Aggregations include episode counts, scoring metrics, filler content analysis, and identification of
  top-rated episodes. Data is sourced from staged episodes table which contains individual episode metadata
  including user scores and filler flags.

  Key transformations:
  - Groups all episodes by anime_id to create per-anime summaries
  - Calculates percentage of filler content (episodes marked as non-canon)
  - Identifies best-rated episode using ARRAY_AGG with ordering
  - Handles NULL scores gracefully (scored vs total episode counts)

  This table serves as a key input for int_anime_base and supports episode-centric analytics.
tags:
  - domain:entertainment
  - domain:anime
  - data_type:intermediate_table
  - pipeline_role:intermediate
  - source:mal_api
  - aggregation_level:anime
  - update_pattern:daily_batch
  - sensitivity:public

materialization:
  type: table

depends:
  - mal_pipeline.stg_episodes

columns:
  - name: anime_id
    type: INTEGER
    description: Unique identifier for anime from MyAnimeList database, serves as primary key
    checks:
      - name: not_null
      - name: unique
  - name: total_episodes
    type: INTEGER
    description: Total count of episodes available for this anime
    checks:
      - name: not_null
  - name: scored_episodes
    type: INTEGER
    description: Count of episodes that have user scores (subset of total_episodes)
    checks:
      - name: not_null
  - name: avg_episode_score
    type: FLOAT
    description: Average user score across all scored episodes (1.0-10.0 scale), rounded to 2 decimal places
  - name: filler_count
    type: INTEGER
    description: Number of episodes marked as filler content (non-canon episodes)
    checks:
      - name: not_null
  - name: filler_pct
    type: FLOAT
    description: Percentage of episodes that are filler content, rounded to 1 decimal place
    checks:
      - name: not_null
  - name: best_episode_score
    type: FLOAT
    description: Highest user score among all episodes for this anime
  - name: best_episode_id
    type: INTEGER
    description: Episode identifier of the highest-rated episode for this anime
  - name: best_episode_title
    type: STRING
    description: Title of the highest-rated episode, useful for identifying standout content

@bruin */

SELECT
    anime_id,
    COUNT(*)                                              AS total_episodes,
    COUNT(score)                                          AS scored_episodes,
    ROUND(AVG(score), 2)                                  AS avg_episode_score,
    COUNTIF(is_filler)                                    AS filler_count,
    ROUND(COUNTIF(is_filler) / COUNT(*) * 100, 1)        AS filler_pct,
    MAX(score)                                            AS best_episode_score,
    ARRAY_AGG(
        STRUCT(episode_id, title, score)
        IGNORE NULLS
        ORDER BY score DESC
        LIMIT 1
    )[OFFSET(0)].episode_id                               AS best_episode_id,
    ARRAY_AGG(
        STRUCT(episode_id, title, score)
        IGNORE NULLS
        ORDER BY score DESC
        LIMIT 1
    )[OFFSET(0)].title                                    AS best_episode_title
FROM `de-zoomcamp-485104.mal_pipeline.stg_episodes`
GROUP BY anime_id

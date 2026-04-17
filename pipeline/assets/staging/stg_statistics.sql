/* @bruin

name: mal_pipeline.stg_statistics
type: bq.sql
description: |
  Standardized staging view of MyAnimeList user engagement statistics and score distributions, providing clean typed
  data for downstream analytics. This view transforms the raw JSON-loaded statistics data by casting all columns to
  proper BigQuery native types (INT64/FLOAT64) and serves as the foundation for user behavior analysis.

  The data captures comprehensive user interaction patterns including viewing status distributions (watching, completed,
  dropped, etc.) and granular rating breakdowns showing both absolute vote counts and percentage distributions for each
  score tier (1-10). This enables analysis of anime popularity, user engagement patterns, and rating distribution skew.

  Key transformations:
  - Type casting from autodetected JSON schema to explicit BigQuery types
  - Consistent naming convention with snake_case for all column names
  - Preservation of all raw data fields without business logic modifications

  Data lineage: Jikan API → GCS JSON → raw_statistics table → stg_statistics view → int_anime_base table

  Operational characteristics:
  - Materialized as view (no storage overhead, always current with raw data)
  - Refreshes automatically when upstream raw_statistics table is updated (daily)
  - Typically processes ~250 rows (one per anime) with 22 statistical columns each
  - Used by int_anime_base for comprehensive anime analytics and dashboard features
tags:
  - domain:anime
  - domain:entertainment
  - data_type:staging_view
  - pipeline_role:staging
  - source:myanimelist
  - update_pattern:view_refresh
  - sensitivity:public
  - quality:standardized
  - user_behavior
  - engagement_metrics
  - rating_distributions
  - type_casting
  - data_standardization

materialization:
  type: view

depends:
  - load.load_statistics

columns:
  - name: anime_id
    type: INTEGER
    description: Unique identifier for anime title from MyAnimeList database, serves as primary key
    checks:
      - name: not_null
      - name: unique
  - name: watching
    type: INTEGER
    description: Count of users currently watching this anime (active viewers in progress)
    checks:
      - name: not_null
  - name: completed
    type: INTEGER
    description: Count of users who have finished watching all episodes of this anime
    checks:
      - name: not_null
  - name: on_hold
    type: INTEGER
    description: Count of users who paused watching and may resume later (temporarily inactive)
    checks:
      - name: not_null
  - name: dropped
    type: INTEGER
    description: Count of users who stopped watching and don't intend to continue (abandoned)
    checks:
      - name: not_null
  - name: plan_to_watch
    type: INTEGER
    description: Count of users who added this anime to their watchlist but haven't started viewing
    checks:
      - name: not_null
  - name: total
    type: INTEGER
    description: Total count of users who have interacted with this anime across all viewing statuses
    checks:
      - name: not_null
  - name: score_1_votes
    type: INTEGER
    description: Count of users who rated this anime 1/10 (lowest possible score, indicates very poor quality)
  - name: score_2_votes
    type: INTEGER
    description: Count of users who rated this anime 2/10 (very poor quality)
  - name: score_3_votes
    type: INTEGER
    description: Count of users who rated this anime 3/10 (poor quality)
  - name: score_4_votes
    type: INTEGER
    description: Count of users who rated this anime 4/10 (below average quality)
  - name: score_5_votes
    type: INTEGER
    description: Count of users who rated this anime 5/10 (average/neutral rating, neither good nor bad)
  - name: score_6_votes
    type: INTEGER
    description: Count of users who rated this anime 6/10 (above average quality)
  - name: score_7_votes
    type: INTEGER
    description: Count of users who rated this anime 7/10 (good quality)
  - name: score_8_votes
    type: INTEGER
    description: Count of users who rated this anime 8/10 (very good quality)
  - name: score_9_votes
    type: INTEGER
    description: Count of users who rated this anime 9/10 (excellent quality)
  - name: score_10_votes
    type: INTEGER
    description: Count of users who rated this anime 10/10 (perfect score, highest possible rating)
  - name: score_1_pct
    type: FLOAT
    description: Percentage of total ratings that are 1/10 scores (decimal format, e.g., 0.05 = 5%)
  - name: score_2_pct
    type: FLOAT
    description: Percentage of total ratings that are 2/10 scores (decimal format)
  - name: score_3_pct
    type: FLOAT
    description: Percentage of total ratings that are 3/10 scores (decimal format)
  - name: score_4_pct
    type: FLOAT
    description: Percentage of total ratings that are 4/10 scores (decimal format)
  - name: score_5_pct
    type: FLOAT
    description: Percentage of total ratings that are 5/10 scores (decimal format)
  - name: score_6_pct
    type: FLOAT
    description: Percentage of total ratings that are 6/10 scores (decimal format)
  - name: score_7_pct
    type: FLOAT
    description: Percentage of total ratings that are 7/10 scores (decimal format)
  - name: score_8_pct
    type: FLOAT
    description: Percentage of total ratings that are 8/10 scores (decimal format)
  - name: score_9_pct
    type: FLOAT
    description: Percentage of total ratings that are 9/10 scores (decimal format)
  - name: score_10_pct
    type: FLOAT
    description: Percentage of total ratings that are 10/10 scores (decimal format)

@bruin */

SELECT
    CAST(anime_id       AS INT64)   AS anime_id,
    CAST(watching       AS INT64)   AS watching,
    CAST(completed      AS INT64)   AS completed,
    CAST(on_hold        AS INT64)   AS on_hold,
    CAST(dropped        AS INT64)   AS dropped,
    CAST(plan_to_watch  AS INT64)   AS plan_to_watch,
    CAST(total          AS INT64)   AS total,

    -- score vote counts
    CAST(score_1_votes  AS INT64)   AS score_1_votes,
    CAST(score_2_votes  AS INT64)   AS score_2_votes,
    CAST(score_3_votes  AS INT64)   AS score_3_votes,
    CAST(score_4_votes  AS INT64)   AS score_4_votes,
    CAST(score_5_votes  AS INT64)   AS score_5_votes,
    CAST(score_6_votes  AS INT64)   AS score_6_votes,
    CAST(score_7_votes  AS INT64)   AS score_7_votes,
    CAST(score_8_votes  AS INT64)   AS score_8_votes,
    CAST(score_9_votes  AS INT64)   AS score_9_votes,
    CAST(score_10_votes AS INT64)   AS score_10_votes,

    -- score percentages
    CAST(score_1_pct    AS FLOAT64) AS score_1_pct,
    CAST(score_2_pct    AS FLOAT64) AS score_2_pct,
    CAST(score_3_pct    AS FLOAT64) AS score_3_pct,
    CAST(score_4_pct    AS FLOAT64) AS score_4_pct,
    CAST(score_5_pct    AS FLOAT64) AS score_5_pct,
    CAST(score_6_pct    AS FLOAT64) AS score_6_pct,
    CAST(score_7_pct    AS FLOAT64) AS score_7_pct,
    CAST(score_8_pct    AS FLOAT64) AS score_8_pct,
    CAST(score_9_pct    AS FLOAT64) AS score_9_pct,
    CAST(score_10_pct   AS FLOAT64) AS score_10_pct

FROM `mal_pipeline.raw_statistics`

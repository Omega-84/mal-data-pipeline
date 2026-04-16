/* @bruin
name: mal_pipeline.stg_statistics
type: bq.sql
materialization:
  type: view
depends:
  - load.load_statistics
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

FROM `de-zoomcamp-485104.mal_pipeline.raw_statistics`

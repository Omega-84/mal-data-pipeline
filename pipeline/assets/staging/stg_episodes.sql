/* @bruin
name: mal_pipeline.stg_episodes
type: bq.sql
materialization:
  type: view
depends:
  - load.load_episodes
@bruin */

SELECT
    CAST(anime_id    AS INT64)    AS anime_id,
    CAST(episode_id  AS INT64)    AS episode_id,
    title,
    CAST(score       AS FLOAT64)  AS score,
    CAST(filler      AS BOOL)     AS is_filler
FROM `de-zoomcamp-485104.mal_pipeline.raw_episodes`

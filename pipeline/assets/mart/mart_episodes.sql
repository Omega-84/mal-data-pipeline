/* @bruin
name: mal_pipeline.mart_episodes
type: bq.sql
materialization:
  type: table
depends:
  - mal_pipeline.stg_episodes
  - mal_pipeline.stg_descriptions
@bruin */

SELECT
    e.anime_id,
    d.title       AS anime_title,
    e.episode_id,
    e.title       AS episode_title,
    e.score,
    e.is_filler
FROM `de-zoomcamp-485104.mal_pipeline.stg_episodes` e
LEFT JOIN `de-zoomcamp-485104.mal_pipeline.stg_descriptions` d USING (anime_id)

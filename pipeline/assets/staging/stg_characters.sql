/* @bruin
name: mal_pipeline.stg_characters
type: bq.sql
materialization:
  type: view
depends:
  - load.load_characters
@bruin */

SELECT
    CAST(anime_id      AS INT64)  AS anime_id,
    CAST(character_id  AS INT64)  AS character_id,
    name,
    image_url
FROM `de-zoomcamp-485104.mal_pipeline.raw_characters`

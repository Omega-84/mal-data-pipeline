/* @bruin
name: mal_pipeline.mart_characters
type: bq.sql
materialization:
  type: table
depends:
  - mal_pipeline.stg_characters
  - mal_pipeline.stg_descriptions
@bruin */

SELECT
    c.anime_id,
    d.title    AS anime_title,
    c.character_id,
    c.name,
    c.image_url
FROM `de-zoomcamp-485104.mal_pipeline.stg_characters` c
LEFT JOIN `de-zoomcamp-485104.mal_pipeline.stg_descriptions` d USING (anime_id)

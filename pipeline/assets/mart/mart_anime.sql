/* @bruin
name: mal_pipeline.mart_anime
type: bq.sql
materialization:
  type: table
  partition_by: TIMESTAMP_TRUNC(airing_start, YEAR)
  cluster_by:
    - genre_1
depends:
  - mal_pipeline.int_anime_base
@bruin */

SELECT *
FROM `de-zoomcamp-485104.mal_pipeline.int_anime_base`

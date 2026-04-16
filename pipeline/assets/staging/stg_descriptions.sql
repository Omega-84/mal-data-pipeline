/* @bruin
name: mal_pipeline.stg_descriptions
type: bq.sql
materialization:
  type: view
depends:
  - load.load_descriptions
@bruin */

SELECT
    CAST(anime_id        AS INT64)     AS anime_id,
    title,
    title_english,
    title_japanese,
    status,
    airing                             AS is_airing,
    CAST(score           AS FLOAT64)   AS score,
    CAST(rank            AS INT64)     AS rank,
    CAST(popularity      AS INT64)     AS popularity_rank,
    synopsis,
    CAST(year            AS INT64)     AS year,
    rating,
    image_url,
    CAST(airing_start AS TIMESTAMP)                  AS airing_start,
    CAST(airing_end   AS TIMESTAMP)                  AS airing_end,
    studios,
    genres,
    themes,
    anime_type,
    source,
    demographics
FROM `de-zoomcamp-485104.mal_pipeline.raw_descriptions`

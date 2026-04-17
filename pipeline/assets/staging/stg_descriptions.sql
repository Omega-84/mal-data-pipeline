/* @bruin

name: mal_pipeline.stg_descriptions
type: bq.sql
description: |
  Staging view for anime metadata that transforms raw JSON data from MyAnimeList into typed columns.

  This view provides the primary staging layer for anime descriptive metadata in the MAL data pipeline,
  performing essential data type conversions and column renaming from the raw Jikan API response format.
  The data represents comprehensive metadata for ~250 popular anime titles sourced from MyAnimeList
  via the Jikan API v4.

  **Key transformations performed:**
  - Type casting from JSON strings to proper BigQuery types (timestamps, integers, floats)
  - Field renaming for consistency (airing → is_airing, popularity → popularity_rank)
  - Timestamp conversion from ISO 8601 strings to native TIMESTAMP type for temporal queries

  **Data lineage:**
  Jikan API → GCS JSON files → raw_descriptions table → stg_descriptions view → int_anime_base/marts

  **Downstream usage:**
  - Joined with statistics and episode data in int_anime_base for comprehensive anime fact table
  - Consumed directly by mart_anime for dashboard analytics and ML recommendation features
  - Primary source for anime title search, filtering, and content-based recommendations

  **Operational characteristics:**
  - Materialized as view for real-time consistency with raw layer changes
  - Refreshes automatically when upstream raw_descriptions table is reloaded daily
  - Expected row count: ~250 anime records matching dim_anime.csv seed file
  - Query performance: Sub-second for typical filtering and aggregation operations
tags:
  - domain:entertainment
  - domain:anime
  - data_type:dimension_table
  - pipeline_role:staging
  - update_pattern:snapshot
  - sensitivity:public
  - source:jikan_api
  - source_system:myanimelist
  - content:anime_metadata
  - freshness:daily
  - transformation:type_conversion
  - api_version:v4

materialization:
  type: view

depends:
  - load.load_descriptions
owner: Omega-84

columns:
  - name: anime_id
    type: INTEGER
    description: |
      Primary identifier from MyAnimeList platform, unique across all anime entries.
      Used for API calls and joins with episodes, characters, and statistics datasets.
      Sourced from dim_anime.csv seed containing ~250 popular anime. Range 1-50000+.
    checks:
      - name: not_null
      - name: unique
  - name: title
    type: STRING
    description: |
      Primary anime title in romanized Japanese (romaji). Canonical identifier used across
      MAL platform and default display name in dashboards and search results.
    checks:
      - name: not_null
  - name: title_english
    type: STRING
    description: |
      Official English localized title when available. Null for anime without official
      English releases. Used for English-language search and international display.
  - name: title_japanese
    type: STRING
    description: |
      Original Japanese title in native characters (kanji/hiragana/katakana).
      Provides authentic representation for cultural context and Japanese search.
  - name: status
    type: STRING
    description: |
      Current airing/release status from MyAnimeList. Standard enum values:
      'Finished Airing', 'Currently Airing', 'Not yet aired'. Critical for
      filtering active vs completed series in analytics and recommendations.
    checks:
      - name: not_null
  - name: is_airing
    type: BOOLEAN
    description: |
      Boolean indicator if anime is actively broadcasting new episodes. Derived from
      raw 'airing' field for direct boolean filtering of currently active series.
  - name: score
    type: FLOAT
    description: |
      Community average rating on 0.0-10.0 scale from MyAnimeList users.
      Null for new/unrated anime with insufficient votes. Used in ranking
      algorithms and quality-based filtering for recommendations.
  - name: rank
    type: INTEGER
    description: |
      Current ranking position based on MyAnimeList weighted scoring algorithm.
      Lower values indicate higher rank (1 = best). Null for unranked anime.
      Primary metric for top-N recommendations and popularity analysis.
  - name: popularity_rank
    type: INTEGER
    description: |
      Popularity ranking based on MAL member list additions. Measures engagement
      breadth rather than quality perception. Renamed from 'popularity' for clarity.
    checks:
      - name: not_null
  - name: synopsis
    type: STRING
    description: |
      Plot summary and description from MyAnimeList. May contain spoiler warnings.
      Essential for content-based recommendations, search, and vector embeddings.
  - name: year
    type: INTEGER
    description: |
      Year anime first aired, extracted from airing_start timestamp. Used for
      temporal analysis, decade filtering, and historical trend examination.
  - name: rating
    type: STRING
    description: |
      Content rating/age classification (G, PG, PG-13, R, etc.). Used for
      age-appropriate content filtering in family-safe recommendation modes.
  - name: image_url
    type: STRING
    description: |
      URL to cover image from MyAnimeList CDN (cdn.myanimelist.net).
      Typically ~225x350px JPG format. Essential for visual dashboard presentation.
    checks:
      - name: not_null
  - name: airing_start
    type: TIMESTAMP
    description: |
      Timestamp when anime first aired/was released. Converted from ISO 8601 string
      in raw layer. Used for temporal queries and date-based mart partitioning.
  - name: airing_end
    type: TIMESTAMP
    description: |
      Timestamp when anime finished airing. Null for ongoing/upcoming series.
      Used to calculate series duration and identify completed content.
  - name: studios
    type: STRING
    description: |
      Comma-separated animation studios (e.g., 'Studio Ghibli,Mappa').
      Key dimension for studio-based analytics and production house recommendations.
  - name: genres
    type: STRING
    description: |
      Comma-separated genre classifications from MAL taxonomy (Action, Comedy, Drama).
      Primary content dimension for filtering and recommendation algorithms. Split
      into individual columns in downstream intermediate tables.
  - name: themes
    type: STRING
    description: |
      Comma-separated thematic tags complementing genres (School, Military, Isekai).
      Provides granular content categorization for specialized recommendations.
  - name: anime_type
    type: STRING
    description: |
      Format classification: 'TV', 'Movie', 'OVA', 'Special', 'ONA'.
      Used for format-specific recommendations and consumption pattern analysis.
    checks:
      - name: not_null
  - name: source
    type: STRING
    description: |
      Original source material (Manga, Light novel, Original, Game, etc.).
      Important for adaptation analysis and source-based discovery patterns.
  - name: demographics
    type: STRING
    description: |-
      Comma-separated target demographics (Shounen, Seinen, Shoujo, Josei, Kids).
      Critical for audience targeting and demographically-appropriate filtering.

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
FROM `mal_pipeline.raw_descriptions`

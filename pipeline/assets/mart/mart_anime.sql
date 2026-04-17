/* @bruin

name: mal_pipeline.mart_anime
type: bq.sql
description: |
  Final consumption mart table for anime analytics and dashboard features, optimized for the OtakuLens Streamlit application.
  This table serves as the primary data source for anime discovery, recommendation engines, and user engagement analytics.

  Contains comprehensive anime metadata from MyAnimeList (via Jikan API) including:
  - Core anime information (titles, scores, rankings, airing dates)
  - Production details (studios, source material, content ratings)
  - Genre/theme categorization (up to 3 genres, 2 themes)
  - User engagement statistics (watching, completed, dropped counts)
  - Episode-level aggregations (total count, average scores, filler analysis)
  - Perfect score analytics and best episode identification

  This mart is optimized for analytical workloads with year-based partitioning for historical analysis
  and genre clustering for recommendation and filtering operations. Data is refreshed daily as part of
  the automated ELT pipeline via Bruin orchestration. Supports both BigQuery production and DuckDB
  local development modes.

  **Data Characteristics:**
  - Expected volume: ~250 top anime by popularity from MyAnimeList
  - Refresh pattern: Full table replacement daily at @daily schedule
  - Latency: Updated within hours of MAL API changes via daily pipeline execution
  - Partitioning: Year-based partitioning enables efficient temporal filtering (1950-2030 range)
  - Clustering: Genre-based clustering optimizes recommendation engine queries

  **Performance Notes:**
  - Primary query patterns: genre filtering, popularity ranking, temporal analysis
  - Most queries filter by year range and/or primary genre for optimal partition pruning
  - Dashboard queries typically include ORDER BY total_users DESC for popularity sorting
  - Recommendation queries leverage genre_1 clustering for efficient similar anime discovery

  Key use cases:
  - Anime search and discovery in dashboard applications
  - Recommendation system feature engineering
  - User engagement and popularity trend analysis
  - Content quality assessment and episode-level insights
  - Genre-based analytics and demographic targeting
tags:
  - domain:entertainment
  - domain:anime
  - data_type:mart_table
  - pipeline_role:mart
  - source:mal_api
  - update_pattern:daily_batch
  - sensitivity:public
  - quality:production
  - consumption:dashboard
  - consumption:analytics
  - consumption:ml_features
  - partition_strategy:yearly
  - cluster_strategy:genre
  - volume:small
  - latency:daily
  - query_pattern:filtering
  - query_pattern:ranking
  - optimization:recommendation_engine

materialization:
  type: table
  partition_by: TIMESTAMP_TRUNC(airing_start, YEAR)
  cluster_by:
    - genre_1

depends:
  - mal_pipeline.int_anime_base

columns:
  - name: anime_id
    type: INTEGER
    description: Primary identifier for anime from MyAnimeList database, used as foreign key across mart tables
    checks:
      - name: not_null
      - name: unique
  - name: title
    type: STRING
    description: Primary display title in romanized form, used for search and dashboard presentation
    checks:
      - name: not_null
  - name: title_english
    type: STRING
    description: Official English localized title, null for anime without English releases
  - name: title_japanese
    type: STRING
    description: Original Japanese title in native characters, valuable for cultural and linguistic analysis
  - name: status
    type: STRING
    description: Current production status (Finished Airing, Currently Airing, Not yet aired)
    checks:
      - name: accepted_values
        value:
          - Finished Airing
          - Currently Airing
          - Not yet aired
  - name: is_airing
    type: BOOLEAN
    description: Binary flag indicating active airing status, optimized for filtering currently broadcasting anime
  - name: score
    type: FLOAT
    description: MyAnimeList community average rating (1.0-10.0), primary quality metric for recommendations
  - name: rank
    type: INTEGER
    description: Global ranking by community score, null for unranked anime, key metric for top lists
  - name: popularity_rank
    type: INTEGER
    description: Ranking by total user engagement (list additions), measures mainstream appeal vs quality
  - name: synopsis
    type: STRING
    description: Plot summary and description, used for content-based recommendation and search features
  - name: year
    type: INTEGER
    description: Release year derived from airing_start, enables temporal analysis and decade-based filtering
  - name: rating
    type: STRING
    description: Content maturity rating (G, PG-13, R, R+ Mild Nudity, Rx Hentai), important for content filtering
  - name: image_url
    type: STRING
    description: CDN URL for anime poster/cover art, required for dashboard and visual presentation
  - name: airing_start
    type: TIMESTAMP
    description: Broadcast start timestamp, partition key for temporal queries and historical analysis
  - name: airing_end
    type: TIMESTAMP
    description: Broadcast completion timestamp, null for ongoing or upcoming anime
  - name: anime_type
    type: STRING
    description: Content format (TV, Movie, OVA, ONA, Special, Music), affects viewing patterns and analysis
    checks:
      - name: accepted_values
        value:
          - TV
          - Movie
          - OVA
          - ONA
          - Special
          - Music
  - name: source
    type: STRING
    description: Origin material (Manga, Light novel, Original, Game), valuable for adaptation success analysis
  - name: demographics
    type: STRING
    description: Target audience classification (Shounen, Seinen, Shoujo, Josei, Kids), enables demographic analysis
  - name: studios
    type: STRING
    description: Production studio names (comma-separated), key for studio performance and style analysis
  - name: genre_1
    type: STRING
    description: Primary genre classification, cluster key for efficient genre-based filtering and recommendations
  - name: genre_2
    type: STRING
    description: Secondary genre, null if anime has fewer than 2 genres, supports multi-genre filtering
  - name: genre_3
    type: STRING
    description: Tertiary genre, null if anime has fewer than 3 genres, enables comprehensive genre analysis
  - name: theme_1
    type: STRING
    description: Primary thematic element, complements genre classification for nuanced categorization
  - name: theme_2
    type: STRING
    description: Secondary theme, null if anime has fewer than 2 themes, supports theme-based discovery
  - name: watching
    type: INTEGER
    description: Active viewer count, real-time engagement metric for trending and popularity analysis
  - name: completed
    type: INTEGER
    description: Users who finished watching, measures completion rate and long-term appeal
  - name: on_hold
    type: INTEGER
    description: Users who paused viewing, potential indicator of pacing or quality issues
  - name: dropped
    type: INTEGER
    description: Users who abandoned the anime, negative engagement signal for recommendation filtering
  - name: plan_to_watch
    type: INTEGER
    description: Users who bookmarked for future viewing, forward-looking popularity indicator
  - name: total_users
    type: INTEGER
    description: Sum of all engagement categories, primary popularity metric for ranking and discovery
  - name: perfect_score_pct
    type: FLOAT
    description: Percentage of 10/10 ratings, measures passionate fandom and exceptional quality
  - name: total_episodes
    type: INTEGER
    description: Complete episode count, essential for time investment calculations and series length analysis
  - name: scored_episodes
    type: INTEGER
    description: Episodes with individual ratings, subset of total episodes used for episode quality metrics
  - name: avg_episode_score
    type: FLOAT
    description: Mean score across rated episodes, granular quality metric beyond overall series rating
  - name: filler_count
    type: INTEGER
    description: Non-canon episode count, important for manga readers and story progression analysis
  - name: filler_pct
    type: FLOAT
    description: Percentage of filler content, quality indicator for adaptation faithfulness
  - name: best_episode_id
    type: INTEGER
    description: Identifier for highest-rated episode, useful for highlight recommendations and quality peaks
  - name: best_episode_title
    type: STRING
    description: Title of highest-rated episode, provides context for quality peaks and memorable moments
  - name: best_episode_score
    type: FLOAT
    description: Rating of highest-rated episode, measures peak quality and exceptional storytelling moments

@bruin */

SELECT *
FROM `mal_pipeline.int_anime_base`

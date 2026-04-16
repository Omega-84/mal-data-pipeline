/* @bruin

name: mal_pipeline.int_anime_base
type: bq.sql
description: |
  Comprehensive anime base table combining core metadata, user engagement statistics, and episode-level aggregations.
  This intermediate table serves as the foundation for downstream marts by joining staging data from MyAnimeList API.

  Data is sourced from the Jikan API (unofficial MAL API) and includes:
  - Core anime metadata (titles, airing dates, ratings, genres)
  - User engagement statistics (watching/completed counts, score distributions)
  - Episode-level aggregations (episode counts, scores, filler detection)

  Genres and themes are split from comma-separated strings into individual columns (max 3 genres, 2 themes).
  This table is materialized daily and serves analytics, recommendations, and dashboard features.

  Key transformations:
  - LEFT JOINs preserve all anime from descriptions even if statistics/episodes are missing
  - Genre/theme parsing handles cases where fewer than max values exist (NULL-filled)
  - Perfect score percentage represents users who rated anime 10/10
  - Episode aggregations include filler detection and best-rated episode identification
tags:
  - domain:entertainment
  - domain:anime
  - data_type:fact_table
  - pipeline_role:intermediate
  - source:mal_api
  - update_pattern:daily_batch
  - sensitivity:public

materialization:
  type: table

depends:
  - mal_pipeline.stg_descriptions
  - mal_pipeline.stg_statistics
  - mal_pipeline.int_episode_agg

columns:
  - name: anime_id
    type: INTEGER
    description: Unique identifier for anime from MyAnimeList database
    checks:
      - name: not_null
      - name: unique
  - name: title
    type: STRING
    description: Primary title of the anime, typically in romanized form
    checks:
      - name: not_null
  - name: title_english
    type: STRING
    description: Official English title, may be null if no English localization exists
  - name: title_japanese
    type: STRING
    description: Original Japanese title in native characters
  - name: status
    type: STRING
    description: Current airing status (Finished Airing, Currently Airing, Not yet aired)
  - name: is_airing
    type: BOOLEAN
    description: Boolean flag indicating if anime is currently airing new episodes
  - name: score
    type: FLOAT
    description: MyAnimeList community average score (1.0-10.0 scale), null if unscored
  - name: rank
    type: INTEGER
    description: Overall ranking by score on MyAnimeList, null if unranked
  - name: popularity_rank
    type: INTEGER
    description: Ranking by total user engagement (members who added to their list)
  - name: synopsis
    type: STRING
    description: Plot summary and description of the anime
  - name: year
    type: INTEGER
    description: Year the anime first aired, extracted from airing_start
  - name: rating
    type: STRING
    description: Content rating (G, PG-13, R, R+ Mild Nudity, Rx Hentai)
  - name: image_url
    type: STRING
    description: URL to the anime's poster/cover image on MyAnimeList CDN
  - name: airing_start
    type: TIMESTAMP
    description: Timestamp when anime started airing, null for unreleased anime
  - name: airing_end
    type: TIMESTAMP
    description: Timestamp when anime finished airing, null for ongoing/unreleased anime
  - name: anime_type
    type: STRING
    description: Format type (TV, Movie, OVA, ONA, Special, Music)
  - name: source
    type: STRING
    description: Source material (Manga, Light novel, Original, Game, etc.)
  - name: demographics
    type: STRING
    description: Target demographic (Shounen, Seinen, Shoujo, Josei, Kids)
  - name: studios
    type: STRING
    description: Animation studios involved in production, comma-separated if multiple
  - name: genre_1
    type: STRING
    description: Primary genre extracted from comma-separated genres list
  - name: genre_2
    type: STRING
    description: Secondary genre, null if anime has fewer than 2 genres
  - name: genre_3
    type: STRING
    description: Tertiary genre, null if anime has fewer than 3 genres
  - name: theme_1
    type: STRING
    description: Primary theme extracted from comma-separated themes list
  - name: theme_2
    type: STRING
    description: Secondary theme, null if anime has fewer than 2 themes
  - name: watching
    type: INTEGER
    description: Number of users currently watching this anime
  - name: completed
    type: INTEGER
    description: Number of users who have completed watching this anime
  - name: on_hold
    type: INTEGER
    description: Number of users who have this anime on hold
  - name: dropped
    type: INTEGER
    description: Number of users who dropped this anime
  - name: plan_to_watch
    type: INTEGER
    description: Number of users planning to watch this anime
  - name: total_users
    type: INTEGER
    description: Total number of users who have interacted with this anime
  - name: perfect_score_pct
    type: FLOAT
    description: Percentage of users who gave this anime a perfect 10/10 score
  - name: total_episodes
    type: INTEGER
    description: Total number of episodes for this anime
  - name: scored_episodes
    type: INTEGER
    description: Number of episodes that have individual scores (subset of total_episodes)
  - name: avg_episode_score
    type: FLOAT
    description: Average score across all scored episodes, rounded to 2 decimal places
  - name: filler_count
    type: INTEGER
    description: Number of episodes marked as filler content
  - name: filler_pct
    type: FLOAT
    description: Percentage of episodes that are filler, rounded to 1 decimal place
  - name: best_episode_id
    type: INTEGER
    description: Episode ID of the highest-rated episode for this anime
  - name: best_episode_title
    type: STRING
    description: Title of the highest-rated episode for this anime
  - name: best_episode_score
    type: FLOAT
    description: Score of the highest-rated episode for this anime

@bruin */

SELECT
    d.anime_id,
    d.title,
    d.title_english,
    d.title_japanese,
    d.status,
    d.is_airing,
    d.score,
    d.rank,
    d.popularity_rank,
    d.synopsis,
    d.year,
    d.rating,
    d.image_url,
    d.airing_start,
    d.airing_end,
    d.anime_type,
    d.source,
    d.demographics,
    d.studios,

    -- genres split (max 3, NULL if not present)
    SPLIT(d.genres, ', ')[SAFE_OFFSET(0)]  AS genre_1,
    SPLIT(d.genres, ', ')[SAFE_OFFSET(1)]  AS genre_2,
    SPLIT(d.genres, ', ')[SAFE_OFFSET(2)]  AS genre_3,

    -- themes split (max 2, NULL if not present)
    SPLIT(d.themes, ', ')[SAFE_OFFSET(0)]  AS theme_1,
    SPLIT(d.themes, ', ')[SAFE_OFFSET(1)]  AS theme_2,

    -- engagement stats
    s.watching,
    s.completed,
    s.on_hold,
    s.dropped,
    s.plan_to_watch,
    s.total           AS total_users,
    s.score_10_pct    AS perfect_score_pct,

    -- episode aggregates
    e.total_episodes,
    e.scored_episodes,
    e.avg_episode_score,
    e.filler_count,
    e.filler_pct,
    e.best_episode_id,
    e.best_episode_title,
    e.best_episode_score

FROM `de-zoomcamp-485104.mal_pipeline.stg_descriptions` d
LEFT JOIN `de-zoomcamp-485104.mal_pipeline.stg_statistics`   s USING (anime_id)
LEFT JOIN `de-zoomcamp-485104.mal_pipeline.int_episode_agg`  e USING (anime_id)

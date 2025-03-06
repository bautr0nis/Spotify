{{ config(
    materialized='table'
) }}

WITH filtered_tracks AS (
    SELECT
        *
    FROM {{ source('spotifydb', 'tracks') }}
    WHERE name IS NOT NULL AND duration_ms >= 60000
)
SELECT
    -- Select columns without filtering or prefix
    id,
    name,
    popularity,
    duration_ms,
    explicit,
    REPLACE(REPLACE(REPLACE(artists, '[', ''), ']', ''), '''', '') AS artists, --REGEXP_REPLACE was not working
    REPLACE(REPLACE(REPLACE(id_artists, '[', ''), ']', ''), '''', '') AS id_artists,
    release_date,
    danceability,
    energy,
    key,
    loudness,
    mode,
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    valence,
    tempo,
    time_signature,
    EXTRACT(YEAR FROM release_date) AS release_year,
    EXTRACT(MONTH FROM release_date) AS release_month,
    EXTRACT(DAY FROM release_date) AS release_day,
    CASE
        WHEN danceability < 0.5 THEN 'Low'
        WHEN danceability BETWEEN 0.5 AND 0.6 THEN 'Medium'
        ELSE 'High'
    END AS danceability_category
FROM filtered_tracks
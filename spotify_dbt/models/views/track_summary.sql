{{ config(
    materialized='view'
) }}

WITH tmp AS (
    SELECT
        t.id AS track_id,
        t.name AS track_name,
        t.popularity,
        t.energy,
        t.danceability_category,
        a.followers AS artist_followers
    FROM {{ ref('transform_tracks') }} AS t
    JOIN {{ ref('transform_artists') }} AS a
        ON t.id_artists = a.id
)
SELECT
    track_id,
    track_name,
    popularity,
    energy,
    danceability_category,
    artist_followers
FROM tmp
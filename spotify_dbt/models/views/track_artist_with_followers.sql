{{ config(
    materialized='view'
) }}

WITH artist_tracks AS (
    SELECT
        -- Concatenate artist_id and track_id and hash them using md5
        md5(a.id || t.id) AS pk, -- Hash-based composite key
        t.id AS track_id,
        t.name AS track_name,
        a.id AS artist_id,
        a.name AS artist_name,
        a.followers
    FROM {{ ref('transform_tracks') }} AS t
    JOIN {{ ref('transform_artists') }} AS a
        ON t.id_artists = a.id
    WHERE a.followers > 0  -- Only artists with followers
)
SELECT
    pk,  -- Unique key based on artist and track
    artist_id,
    artist_name,
    track_id,
    track_name
FROM artist_tracks
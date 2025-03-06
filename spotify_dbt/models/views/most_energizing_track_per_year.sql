{{ config(
    materialized='view'
) }}

WITH year_energy_data AS (
    SELECT
        t.release_year,
        t.id AS track_id,
        t.name AS track_name,
        t.energy,
        a.followers AS artist_followers,
        ROW_NUMBER() OVER (
            PARTITION BY t.release_year
            ORDER BY t.energy DESC, a.followers DESC
        ) AS row_num
    FROM {{ ref('transform_tracks') }} AS t
    JOIN {{ ref('transform_artists') }} AS a
        ON t.id_artists = a.id
)
SELECT
    release_year,
    track_id,
    track_name,
    energy AS highest_energy
FROM year_energy_data
WHERE row_num = 1  -- Get the most energizing track for each year, with tie-breaker on followers
ORDER BY release_year
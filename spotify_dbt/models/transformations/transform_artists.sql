{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    a.*
FROM {{ source('spotifydb', 'artists') }} a
INNER JOIN {{ref('transform_tracks')}} t ON t.id_artists = a.id
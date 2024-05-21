{{ config(materialized = 'table') }}

SELECT {{ dbt_utils.surrogate_key(['movie_id']) }} AS movieKey,
       *
FROM (

    (
        SELECT movie_id AS movieId,
               movie_name AS movieName,
               year,
               certificate,
               runtime,
               genre,
               rating,
               description,
               director,
               director_id AS directorId,
               star,
               star_id AS starId,
               votes,
               "gross(in $)" AS gross
        FROM {{ source('staging', 'movies') }}
    )

    UNION ALL

    (
        SELECT 'NNNNNNNNNNNNNNNNNNN' AS movieId,
               'NA' AS movieName,
               0 AS year,
               'NA' AS certificate,
               'NA' AS runtime,
               'NA' AS genre,
               0.0 AS rating,
               'NA' AS description,
               'NA' AS director,
               'NA' AS directorId,
               'NA' AS star,
               'NA' AS starId,
               0 AS votes,
               0.0 AS gross
    )
)

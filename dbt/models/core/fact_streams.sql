{{ config(
  materialized = 'table',
  partition_by={
    "field": "ts",
    "data_type": "timestamp",
    "granularity": "hour"
  }
  ) }}

SELECT 
    dim_users.userKey AS userKey,
    dim_movies.movieKey AS videoKey ,
    dim_datetime.dateKey AS dateKey,
    dim_location.locationKey AS locationKey,
    watch_events.ts AS ts
 FROM {{ source('staging', 'watch_events') }}
  LEFT JOIN {{ ref('dim_users') }} 
    ON watch_events.userId = dim_users.userId AND CAST(watch_events.ts AS DATE) >= dim_users.rowActivationDate AND CAST(watch_events.ts AS DATE) < dim_users.RowExpirationDate
  LEFT JOIN {{ ref('dim_movies') }}
    ON REPLACE(REPLACE(watch_events.videoTitle, '"', ''), '\\', '') = dim_movies.movieName
  LEFT JOIN {{ ref('dim_location') }} 
    ON watch_events.city = dim_location.city AND watch_events.state = dim_location.stateCode AND watch_events.lat = dim_location.latitude AND watch_events.lon = dim_location.longitude
  LEFT JOIN {{ ref('dim_datetime') }} 
    ON dim_datetime.date = date_trunc(watch_events.ts, HOUR)
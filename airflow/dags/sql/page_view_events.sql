INSERT {{ BIGQUERY_DATASET }}.{{ PAGE_VIEW_EVENTS_TABLE }}
SELECT
    timestamp,
    COALESCE(page, 'NA') AS page,
    COALESCE(auth, 'NA') AS auth,
    COALESCE(method, 'NA') AS method,
    COALESCE(status, 0) AS status,
    COALESCE(level, 'NA') AS level,
    COALESCE(city, 'NA') AS city,
    COALESCE(state, 'NA') AS state,
    COALESCE(userAgent, 'NA') AS userAgent,
    COALESCE(lon, 0.0) AS lon,
    COALESCE(lat, 0.0) AS lat,
    COALESCE(userId, 0) AS userId,
    COALESCE(lastName, 'NA') AS lastName,
    COALESCE(firstName, 'NA') AS firstName,
    COALESCE(dateOfBirth, 'NA') AS dateOfBirth,
    COALESCE(gender, 'NA') AS gender,
    COALESCE(registration, 9999999999999) AS registration,
    COALESCE(videoTitle, 'NA') AS video,
    COALESCE(deviceType, 'NA') AS device,
    COALESCE(deviceOs, 'NA') AS os,
    COALESCE(duration, -1) AS duration
FROM {{ BIGQUERY_DATASET }}.{{ PAGE_VIEW_EVENTS_TABLE}}_{{ logical_date.strftime("%m%d%H") }} -- Creates a table name with month day and hour values appended to it
                                                                                            -- like page_view_events_052117 for 21-05-2024 17:00:00
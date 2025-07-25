{{ config(materialized='table') }}

WITH RECURSIVE hours_series AS (
    -- Start with the first hour of the day
    SELECT
        TIME('00:00:00') AS time_value,
        0 AS hour_24
    UNION ALL
    -- Recursively add one hour until the end of the day (23:00:00)
    SELECT
        DATEADD(hour, 1, time_value),
        hour_24 + 1
    FROM
        hours_series
    WHERE
        hour_24 < 23 -- Stop at 23 to include 00 to 23
)
SELECT
    -- Create a unique integer key for each hour (e.g., 0 to 23)
    hour_24 AS hour_id,
    time_value AS hour_of_day,
    hour_24,
    TO_VARCHAR(time_value, 'HH12 AM') AS hour_12_ampm,
    CASE
        WHEN hour_24 BETWEEN 0 AND 11 THEN 'AM'
        ELSE 'PM'
    END AS am_pm_indicator,
    CASE
        WHEN hour_24 BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour_24 BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN hour_24 BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day_segment
FROM
    hours_series
ORDER BY
    hour_24
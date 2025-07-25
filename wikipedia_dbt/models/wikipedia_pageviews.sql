{{ config(materialized='table') }}

WITH wiki_data AS(
    SELECT
        TO_DATE(SPLIT_PART(FILE_NAME, '-', 2), 'YYYYMMDD') AS pageview_date
        , TO_TIME(SPLIT_PART(SPLIT_PART(FILE_NAME, '-', 3), '.', 0), 'HH24MISS') AS pageview_hour
        , SPLIT_PART(PROJECT_CODE, '.', 2) AS pageview_source
        , SPLIT_PART(PROJECT_CODE, '.', 0) AS page_language
        , PAGE_TITLE
        , VIEW_COUNT
        , BYTE_SIZE
    FROM snowflake_learning_db.public.wikipedia_pageviews_raw -- Use the table that was created in the ingest script
)
SELECT
{{ dbt_utils.generate_surrogate_key(
['pageview_date',
    'pageview_hour',
    'pageview_source',
    'page_language',
    'PAGE_TITLE',
    'VIEW_COUNT',
    'BYTE_SIZE']) }} AS wikipedia_pageviews_id
, TO_TIMESTAMP(pageview_date || ' ' || pageview_hour) AS pageview_date_hour
, CASE WHEN pageview_source = 'm' THEN 'Mobile Web'
    WHEN pageview_source = 'b' THEN 'Bot/Spider Traffic'
        WHEN pageview_source = 'd' THEN 'Desktop Web'
            WHEN pageview_source = 'f' THEN 'Mobile App'
                WHEN pageview_source = 't' THEN 'Tablet Web'
                    WHEN pageview_source = 'w' THEN 'Wikimedia Mobile App'
                        ELSE 'Unknown' END
    AS pageview_source
, PAGE_TITLE
, CLASSIFY_WIKIPEDIA_PAGE(PAGE_TITLE) AS PAGE_CATEGORY -- Categorize the PAGE_TITLE using Snowflake's Cortex AI
, VIEW_COUNT
, BYTE_SIZE
FROM wiki_data
WHERE page_language = 'en'
LIMIT 1000
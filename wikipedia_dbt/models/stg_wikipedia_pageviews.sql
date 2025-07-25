{{ config(materialized='table') }}

    SELECT
        TO_DATE(SPLIT_PART(FILE_NAME, '-', 2), 'YYYYMMDD') AS pageview_date
        , LEFT(SPLIT_PART(SPLIT_PART(FILE_NAME, '-', 3), '.', 0), 2) AS pageview_hour
        , SPLIT_PART(PROJECT_CODE, '.', 2) AS pageview_source
        , SPLIT_PART(PROJECT_CODE, '.', 0) AS page_language
        , PAGE_TITLE
        , CLASSIFY_WIKIPEDIA_PAGE(PAGE_TITLE) AS PAGE_CATEGORY -- Categorize the PAGE_TITLE using Snowflake's Cortex AI
        , VIEW_COUNT
        , BYTE_SIZE
    FROM snowflake_learning_db.public.wikipedia_pageviews_raw -- Use the table that was created in the ingest script
    WHERE page_language = 'en' -- We only care about English pages for now

LIMIT 1000
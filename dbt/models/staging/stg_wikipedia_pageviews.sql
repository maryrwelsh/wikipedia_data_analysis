{{ config(materialized='table') }}

WITH wiki_data AS(
    SELECT
        TO_DATE(SPLIT_PART(FILE_NAME, '-', 2), 'YYYYMMDD') AS pageview_date
        , LEFT(SPLIT_PART(SPLIT_PART(FILE_NAME, '-', 3), '.', 0), 2) AS pageview_hour
        , SPLIT_PART(PROJECT_CODE, '.', 2) AS pageview_source
        , SPLIT_PART(PROJECT_CODE, '.', 0) AS page_language
        , PAGE_TITLE
        , CLASSIFY_WIKIPEDIA_PAGE(PAGE_TITLE) AS page_category_ai -- Categorize the PAGE_TITLE using Snowflake's Cortex AI
        , VIEW_COUNT
        , BYTE_SIZE
    FROM {{ source('raw_wikipedia_pageviews', 'raw_wikipedia_pageviews') }} -- Use the table that was created in the ingest script
    WHERE page_language = 'en' -- We only care about English pages for now
-- LIMIT 1000 -- For testing we limit the dataset to save $
)
, categories_cleanup AS(
    SELECT
        pageview_date
        , pageview_hour
        , pageview_source
        , page_language
        , PAGE_TITLE
        , TRIM(SPLIT_PART(SPLIT_PART(SPLIT_PART(page_category_ai, '\n', 1), ',', 1), ' ', 2)) AS page_category_trimmed -- Trim off any new lines or excess categories it generates
        , VIEW_COUNT
        , BYTE_SIZE
    FROM wiki_data
)
SELECT
    pageview_date
    , pageview_hour
    , pageview_source
    , page_language
    , PAGE_TITLE
    , CASE WHEN page_category_trimmed NOT IN ('Technology', 'History', 'Science', 'Sports',
                                                          'Arts_and_Culture', 'Geography', 'Politics', 'Current_Events',
                                                          'Biography', 'Health', 'Nature', 'Entertainment',
                                                          'Miscellaneous')
        THEN 'Unknown'
        ELSE page_category_trimmed
        END AS page_category -- Bucket the rest of the categories into 'Unknown'
    , VIEW_COUNT
    , BYTE_SIZE
FROM categories_cleanup

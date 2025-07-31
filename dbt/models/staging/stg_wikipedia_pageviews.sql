{{ config(
    materialized='incremental',
    unique_key='wikipedia_pageview_id',
    on_schema_change='fail'
) }}

WITH raw_data AS (
    SELECT
    {{ dbt_utils.generate_surrogate_key([
        'FILE_NAME',
        'PROJECT_CODE',
        'PAGE_TITLE'
    ]) }} AS wikipedia_pageview_id
    , PROJECT_CODE
    , PAGE_TITLE
    , VIEW_COUNT
    , BYTE_SIZE
    , FILE_NAME
    , LOAD_TIMESTAMP
    , SPLIT_PART(PROJECT_CODE, '.', 0) AS page_language
    , SPLIT_PART(PROJECT_CODE, '.', 2) AS pageview_source_raw
    , CLASSIFY_WIKIPEDIA_PAGE(PAGE_TITLE) AS page_category_ai -- Categorize the page category using Snowflake's Cortex AI
    FROM {{ source('raw_wikipedia_pageviews', 'raw_wikipedia_pageviews') }}
    WHERE SPLIT_PART(PROJECT_CODE, '.', 0) = 'en'
    
    {% if is_incremental() %}
        -- Only process new data loaded in the last 2 hours
        AND LOAD_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 HOURS'
    {% endif %}
    
     --LIMIT 1000
)
SELECT
    wikipedia_pageview_id,
    TO_DATE(SPLIT_PART(raw_data.FILE_NAME, '-', 2), 'YYYYMMDD') AS pageview_date,
    LEFT(SPLIT_PART(SPLIT_PART(raw_data.FILE_NAME, '-', 3), '.', 0), 2) AS pageview_hour,
    CASE
        WHEN raw_data.pageview_source_raw = 'm' THEN 'Mobile Web'
        WHEN raw_data.pageview_source_raw = 'b' THEN 'Bot/Spider Traffic'
        WHEN raw_data.pageview_source_raw = 'd' THEN 'Desktop Web'
        WHEN raw_data.pageview_source_raw = 'f' THEN 'Mobile App'
        WHEN raw_data.pageview_source_raw = 't' THEN 'Tablet Web'
        WHEN raw_data.pageview_source_raw = 'w' THEN 'Wikimedia Mobile App'
        ELSE 'Unknown'
    END AS pageview_source,
    raw_data.page_language,
    raw_data.PAGE_TITLE,
    COALESCE(
        CASE
            WHEN GET(raw_data.page_category_ai, 'labels')[0]::VARCHAR IN
                ('Technology', 'History', 'Science', 'Sports', 'Arts_and_Culture',
                'Geography', 'Politics', 'Current_Events', 'Biography', 'Health',
                'Nature', 'Entertainment', 'Miscellaneous')
            THEN GET(raw_data.page_category_ai, 'labels')[0]::VARCHAR
            ELSE 'Unknown'
        END, 'Unknown' -- Handle cases where AI_CLASSIFY might return NULL or empty
    ) AS page_category,
    raw_data.VIEW_COUNT,
    raw_data.BYTE_SIZE,
    raw_data.LOAD_TIMESTAMP
FROM raw_data

{{ config(materialized='table') }}

SELECT
{{ dbt_utils.generate_surrogate_key(
    ['pageview_source',
    'PAGE_TITLE',
    'PAGE_CATEGORY'
]) }} AS dim_wikipedia_page_id
, CASE WHEN pageview_source = 'm' THEN 'Mobile Web'
    WHEN pageview_source = 'b' THEN 'Bot/Spider Traffic'
        WHEN pageview_source = 'd' THEN 'Desktop Web'
            WHEN pageview_source = 'f' THEN 'Mobile App'
                WHEN pageview_source = 't' THEN 'Tablet Web'
                    WHEN pageview_source = 'w' THEN 'Wikimedia Mobile App'
                        ELSE 'Unknown' END
    AS pageview_source
, PAGE_TITLE
, PAGE_CATEGORY
FROM {{ ref('stg_wikipedia_pageviews') }}
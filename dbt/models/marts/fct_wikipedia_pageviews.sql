{{ config(materialized='table') }}

SELECT
pageview_date AS pageview_date_id
, pageview_hour AS pageview_hour_id
, dwp.dim_wikipedia_page_id
, VIEW_COUNT
, BYTE_SIZE
FROM {{ ref('stg_wikipedia_pageviews') }} stg
LEFT JOIN {{ ref('dim_wikipedia_page') }} dwp ON stg.pageview_source = dwp.pageview_source
    AND stg.page_language = dwp.page_language
    AND stg.page_title = dwp.page_title
    AND stg.page_category = dwp.page_category
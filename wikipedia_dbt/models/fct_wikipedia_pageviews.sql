{{ config(materialized='table') }}

SELECT
pageview_date AS pageview_date_id
, pageview_hour AS pageview_hour_id
, {{ dbt_utils.generate_surrogate_key(
    ['pageview_source',
    'PAGE_TITLE',
    'PAGE_CATEGORY'
]) }} AS dim_wikipedia_page_id
, VIEW_COUNT
, BYTE_SIZE
FROM {{ ref('stg_wikipedia_pageviews') }}
{{ config(materialized='table') }}

SELECT DISTINCT
{{ dbt_utils.generate_surrogate_key(
    ['pageview_source',
    'page_language',
    'PAGE_TITLE',
    'PAGE_CATEGORY'
]) }} AS dim_wikipedia_page_id
, pageview_source
, page_language
, PAGE_TITLE
, PAGE_CATEGORY
FROM {{ ref('stg_wikipedia_pageviews') }}
{{ config(
    materialized='incremental',
    unique_key=['wikipedia_pageview_id'],
    on_schema_change='fail'
) }}

SELECT
stg.wikipedia_pageview_id AS fct_wikipedia_pageview_id
, stg.pageview_date AS pageview_date_id
, stg.pageview_hour AS pageview_hour_id
, dwp.dim_wikipedia_page_id
, stg.VIEW_COUNT
, stg.BYTE_SIZE
, stg.LOAD_TIMESTAMP
FROM {{ ref('stg_wikipedia_pageviews') }} stg
LEFT JOIN {{ ref('dim_wikipedia_page') }} dwp ON stg.pageview_source = dwp.pageview_source
    AND stg.page_language = dwp.page_language
    AND stg.page_title = dwp.page_title
    AND stg.page_category = dwp.page_category

{% if is_incremental() %}
WHERE stg.LOAD_TIMESTAMP >= CURRENT_TIMESTAMP() - INTERVAL '2 HOURS'
{% endif %}
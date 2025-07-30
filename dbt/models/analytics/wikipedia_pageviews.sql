{{ config(materialized='view') }}

-- Final data mart view
SELECT
dd.*
, dt.*
, dwp.pageview_source
, dwp.page_title
, dwp.page_category
, fwp.view_count
, fwp.byte_size
FROM {{ ref('fct_wikipedia_pageviews') }} fwp
JOIN {{ ref('dim_date') }} dd ON fwp.PAGEVIEW_DATE_ID = dd.DATE_DAY
JOIN {{ ref('dim_hour') }} dt ON fwp.PAGEVIEW_HOUR_ID = dt.HOUR_ID
JOIN {{ ref('dim_wikipedia_page') }} dwp ON fwp.DIM_WIKIPEDIA_PAGE_ID = dwp.DIM_WIKIPEDIA_PAGE_ID
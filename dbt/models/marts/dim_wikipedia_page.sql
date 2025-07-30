{{ config(materialized='table') }}

SELECT DISTINCT
{{ dbt_utils.generate_surrogate_key(['pageview_source']) }} AS dim_wikipedia_pageview_source_id
, pageview_source
FROM {{ ref('stg_wikipedia_pageviews') }}
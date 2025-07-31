{% macro create_cortex_udf() %}
  {% set sql %}
    CREATE OR REPLACE FUNCTION CLASSIFY_WIKIPEDIA_PAGE(page_title VARCHAR)
    RETURNS OBJECT
    LANGUAGE SQL
    AS
    $$
        SNOWFLAKE.CORTEX.AI_CLASSIFY(page_title,
        ['Technology', 'History', 'Science', 'Sports', 'Arts_and_Culture', 
        'Geography', 'Politics', 'Current_Events', 'Biography', 'Health', 
        'Nature', 'Entertainment', 'Miscellaneous'])
    $$;
  {% endset %}

  {% do run_query(sql) %}
  {{ log("Created or replaced CLASSIFY_WIKIPEDIA_PAGE UDF", info=True) }}
{% endmacro %}
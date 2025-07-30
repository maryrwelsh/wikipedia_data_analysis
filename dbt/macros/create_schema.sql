{% macro create_schema_if_not_exists() %}
  {% set schema_name = target.schema %}
  {% set sql %}
    CREATE SCHEMA IF NOT EXISTS {{ schema_name }}
  {% endset %}

  {% do run_query(sql) %}
  {{ log("Ensured schema '" ~ schema_name ~ "' exists", info=True) }}
{% endmacro %} 
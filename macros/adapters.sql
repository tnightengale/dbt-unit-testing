{% macro quote_column_name(column_name) %}
    {{ return(adapter.dispatch('quote_column_name','dbt_unit_testing')(column_name)) }}
{% endmacro %}

{% macro default__quote_column_name(column_name) -%}
    {% if column_name.startswith('"') %}
      {{ return(column_name) }}
    {% else %}
      {{ return('"' ~ column_name ~ '"') }}
    {% endif %}
{%- endmacro %}

{% macro bigquery__quote_column_name(column_name) %}
    {{ return(column_name) }}
{% endmacro %}

{% macro snowflake__quote_column_name(column_name) %}
    {% if column_name.startswith('"') %}
      {{ return(column_name) }}
    {% else %}
      {{ return('"' ~ column_name | upper ~ '"') }}
    {% endif %}
{% endmacro %}

{%- macro fetch_relation_name(relation) -%}
    {%- do return(adapter.dispatch('fetch_relation_name','dbt_unit_testing')(relation)) -%}
{%- endmacro -%}

{%- macro default__fetch_relation_name(relation) -%}
  {%- do return(relation.name) -%}
{%- endmacro -%}

{% macro build_model_complete_sql(model_name, test_inputs) %}

  {% set node = dbt_unit_testing.model_node(model_name) %}

  {% set model_dependencies = dbt_unit_testing.build_model_dependencies(node, test_inputs) %}

  {%- set cte_with_dependencies -%}
    {%- for d in model_dependencies -%}

      {% if execute %}
        {% set node = dbt_unit_testing.node_by_id(d) %}
      {% else %}
        {% set node = {"raw_sql":""} %}
      {% endif %}

      {% set rendered_sql_outside_conditional_block = render(node.get("raw_sql", "")) %}

      {% if execute %}

        {%- if loop.first -%}
        with
        {%- endif -%}

        {{ node.name }} as (
        {%- if node.resource_type in ('model', 'snapshot') -%}
            {{ rendered_sql_outside_conditional_block }}
        {%- elif node.resource_type == 'seed' -%}
            {{ dbt_unit_testing.fake_seed_sql(node) }}
        {%- else -%}
            {{ dbt_unit_testing.fake_source_sql(node) }}
        {%- endif -%}
          )
        {%- if not loop.last -%},{%- endif -%}

      {% endif %}
    {%- endfor %}
  {%- endset -%}

    {%- set full_sql -%}
      {{ cte_with_dependencies }}
      select * from ({{ render(node.raw_sql) }}) as tmp
    {%- endset -%}
    {% do return(full_sql) %}

{% endmacro %}

{% macro build_model_dependencies(node={}, test_input=[], model_dependencies=[]) %}
  {% if execute %}

  {% set parent_node_names = node.depends_on.nodes %}

  {% for node_name in parent_node_names %}
    {% set parent_node = dbt_unit_testing.node_by_id(node_name) %}
    {% if parent_node.resource_type == 'model' and parent_node.name not in test_input %}
      {% set new_dependencies = model_dependencies + dbt_unit_testing.build_model_dependencies(parent_node, test_input, model_dependencies) %}
      {% do return(new_dependencies) %}
    {% else %}
      {% do return(model_dependencies) %}
    {% endif %}
  {% endfor %}

  {% endif %}

{% endmacro %}

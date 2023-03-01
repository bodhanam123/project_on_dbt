{%- if execute %}
  {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
    {% set var=node.depends_on.macros %}
    {% set var=var|join(', ') %}
    {% if var=="macro.my_new_project.placement_status_null_enum_macro" -%}
        {{node.name}}
    {%- endif %}
  {% endfor %}
{% endif -%}
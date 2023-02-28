{% if execute %}
  {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
    {% set name %}
        a={{node.depends_on.macros}}
    {% endset %}
    {% if a=='macro.my_new_project.placement_status_null_enum_macro' %}
        {{node.name}}
    {% endif %}
  {% endfor %}
{% endif %}
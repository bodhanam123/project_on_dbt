{% for node in graph.nodes.values() %}
    {% set var1=node.name %}
    {% if var1=='ccbp_users_placement_corner_resume_details' %}
        {% set var2=node.depends_on.nodes %}
          {% for i in var2 -%}
              {{i}}
          {% endfor %}
    {% endif %}
{%- endfor %}
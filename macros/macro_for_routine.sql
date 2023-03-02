    {%- macro drop_table_macro(model) -%}
    {%- set drop_query -%}
        DROP VIEW `{{target.database}}`.`{{target.schema}}`.{{model}}
    {%- endset -%}
    {% do run_query(drop_query) %}
{%- endmacro -%}
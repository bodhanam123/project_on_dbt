{%- macro placement_routine() -%}
    {%- set routine_example -%}
    CREATE OR REPLACE FUNCTION `{{target.database}}.backend_tables.placement_status_null_enum`() 
    RETURNS STRING 
    AS (
"To Be Placed"
);    
    {%- endset -%}
    {% do run_query(routine_example) %}
{%- endmacro -%}
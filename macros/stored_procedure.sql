{%- macro stored_procedure_test() -%}
   {%- set tables_query -%}
        CREATE OR REPLACE PROCEDURE `{{target.database}}.{{target.schema}}.stored_procedure`()
        BEGIN 
        CREATE OR REPLACE TABLE {{ ref('my_first_dbt_model') }}  AS (select 1 as id
        union all
        select null as id
        union all
        select 2 
        union all 
        select 3
        union all 
        select 1);
    END
    {%- endset -%}
    {% do run_query(tables_query) %}
{%- endmacro -%}




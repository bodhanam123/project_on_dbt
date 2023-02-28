
{{ config(materialized='table') }}

with source_data as (

    select 6 as id
    union all
    select 4 as id
    union all
    select 8 
    union all 
    select 2
    union all 
    select 10

)

select *
from source_data
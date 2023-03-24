{% test is_even(model, column_name,m,n) %}
{% set m=0 %}


with validation as (

    select
        {{ column_name }} as even_field

    from {{ model }}

),

validation_errors as (

    select
        even_field

    from validation
    -- if this is true, then even_field is actually odd!
    where (even_field / 2) = {{m}}

)

select *
from validation_errors

{% endtest %}
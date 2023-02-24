{{
    config(
        materialized='incremental',
        unique_key='date_day'
    )
}}


select count(distinct(user_id)) AS no_of_enrolled_users_per_day,datetime_trunc(cast(registration_date_time as timestamp),DAY) AS date_day
from {{ source('backend_tables', 'user_data_for_incremental_model') }}
{%if is_incremental()%}
where cast(registration_date_time as timestamp)>(select MAX(date_day) from {{this}})
{% endif %}
GROUP BY date_day
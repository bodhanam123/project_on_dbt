{{ config(alias='ccbp_users_placement_corner_resume_details',schema='team_one') }}

select *
from {{ ref('ccbp_users_placement_corner_resume_details') }}
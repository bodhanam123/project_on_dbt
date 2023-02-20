{{ config(alias='ccbp_users_placement_corner_video_submission_details',schema='team_two') }}

select *
from {{ ref('ccbp_users_placement_corner_video_submission_details') }}
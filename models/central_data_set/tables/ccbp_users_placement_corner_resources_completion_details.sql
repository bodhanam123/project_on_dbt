SELECT
  REPLACE(user_placement_section.user_id,"-","") AS user_id,
  placement_corner_section_details.section_id,
  placement_corner_section_details.section_title,
  placement_corner_section_details.section_type,
  placement_corner_section_details.section_order,
  placement_corner_section_details.step_id,
  placement_corner_section_details.step_type,
  placement_corner_section_details.step_title,
  placement_corner_section_details.step_order,
  REPLACE(placement_corner_section_details.resource_id,"-","") AS resource_id,
  placement_corner_section_details.resource_order,
  user_resource_wise_completion_details.resource_title,
  user_resource_wise_completion_details.completion_percentage,
  user_resource_wise_completion_details.completion_datetime,
  user_resource_wise_completion_details.last_progress_datetime

FROM {{ source('backend_tables', 'nkb_placement_support_userplacementsection') }} AS user_placement_section 


INNER JOIN {{ ref('placement_corner_sections_steps_resources_details') }} AS placement_corner_section_details
ON placement_corner_section_details.section_id = user_placement_section.placement_section_id 
AND placement_corner_section_details.resource_id IS NOT NULL

LEFT JOIN {{ ref('users_resource_wise_completion_details') }}  AS user_resource_wise_completion_details 
ON (
  user_resource_wise_completion_details.last_progress_datetime IS NOT NULL
  AND user_resource_wise_completion_details.resource_type="UNIT"
  AND user_resource_wise_completion_details.resource_id =  REPLACE(placement_corner_section_details.resource_id,"-","")  
  AND user_resource_wise_completion_details.user_id = user_placement_section.user_id
)

WHERE (
  EXISTS(
    SELECT 
        xpm_ccbp_users_master_db.user_id 
    FROM {{ source('backend_tables', 'xpm_ccbp_users_master_db_view') }} AS xpm_ccbp_users_master_db 
    WHERE xpm_ccbp_users_master_db.user_id = REPLACE(user_placement_section.user_id,"-","")
  )
)
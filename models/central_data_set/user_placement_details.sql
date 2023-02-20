SELECT
  user_placement_details.* EXCEPT(user_id,company_name,job_role,interview_success_expert,corporate_relations_manager,placement_status),
  REPLACE(user_placement_details.user_id,"-","") AS user_id,
  INITCAP(TRIM(user_placement_details.company_name)) AS company_name,
  INITCAP(TRIM(user_placement_details.job_role)) AS job_role,
  INITCAP(TRIM(user_placement_details.interview_success_expert)) AS interview_success_expert,
  INITCAP(TRIM(user_placement_details.corporate_relations_manager)) AS corporate_relations_manager,
  CASE
    WHEN placement_status_enums_config.placement_status_string IS NOT NULL  
    THEN placement_status_enums_config.placement_status_string
    ELSE user_placement_details.placement_status
  END AS placement_status
FROM {{ source('backend_tables', 'nkb_jobs_userplacementdetails') }} AS user_placement_details

LEFT JOIN {{ ref('placement_status_enums_config') }} 
ON placement_status_enums_config.placement_status_enum = user_placement_details.placement_status
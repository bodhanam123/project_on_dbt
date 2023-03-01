SELECT  
    REPLACE(user_resume_details.user_id,"-","") AS user_id,
    user_resume_details.creation_datetime AS resume_creation_datetime,
    user_resume_details.last_update_datetime AS resume_last_update_datetime,
    RANK() OVER(
        PARTITION BY 
            user_resume_details.user_id
        ORDER BY 
            user_resume_details.creation_datetime,
            user_resume_details.id 
    ) AS resume_submission_no,
    
    user_resume_details.resume_generation_status,
    user_resume_details.is_final AS is_resume_final,
    CASE
        WHEN user_resume_details.resume_generation_status = "SUCCESS"
        THEN user_resume_details.id
    END AS unique_resume_id,

    CASE 
      WHEN user_placement_section_details.user_id IS NOT NULL 
      THEN "PLACEMENT_SECTION_ACCESS_ENABLED"
      ELSE "PLACEMENT_SECTION_ACCCESS_NOT_ENABLED"
    END AS placement_section_access,

    CASE
        WHEN users_placement_status.placement_status IS NULL
        THEN {{placement_status_null_enum_macro()}}
        ELSE users_placement_status.placement_status
    END AS placement_status

FROM {{ source('backend_tables', 'nkb_placement_support_userresume') }}  AS user_resume_details

LEFT JOIN (
  SELECT 
    DISTINCT 
      user_id
  FROM {{ source('backend_tables', 'nkb_placement_support_userplacementsection') }} AS user_placement_section
) AS user_placement_section_details
ON user_placement_section_details.user_id = user_resume_details.user_id

LEFT JOIN {{ ref('user_placement_status') }} AS users_placement_status
ON REPLACE(user_resume_details.user_id,"-","") = REPLACE(users_placement_status.user_id,"-","")

WHERE (
    EXISTS(
        SELECT 
            xpm_ccbp_users_master_db.user_id 
        FROM {{ source('backend_tables', 'xpm_ccbp_users_master_db_view') }} AS xpm_ccbp_users_master_db 
        WHERE xpm_ccbp_users_master_db.user_id = user_resume_details.user_id
    )
)
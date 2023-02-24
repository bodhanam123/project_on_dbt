SELECT
  REPLACE(user_placement_step_interview.user_id,"-","") AS user_id,
  user_placement_step_interview.scheduled_datetime,
  user_placement_step_interview.interview_status AS mock_interview_status,
  user_placement_step_interview.placement_step_id,
  RANK() OVER (
    PARTITION BY
      user_placement_step_interview.user_id
    ORDER BY
      user_placement_step_interview.scheduled_datetime,
      user_placement_step_interview.id
  ) AS mock_interview_schedule_no,

  CASE
    WHEN users_placement_status.placement_status IS NULL
        THEN {{ placement_status_null_enum_macro()}}
    ELSE users_placement_status.placement_status
  END AS placement_status

FROM {{ source('backend_tables', 'nkb_placement_support_userplacementstepinterview') }}  AS user_placement_step_interview

LEFT JOIN {{ ref('user_placement_status') }} AS users_placement_status
ON REPLACE(user_placement_step_interview.user_id,"-","") = REPLACE(users_placement_status.user_id,"-","")

WHERE (
  EXISTS(
    SELECT 
        xpm_ccbp_users_master_db.user_id 
    FROM {{ source('backend_tables', 'xpm_ccbp_users_master_db_view') }} AS xpm_ccbp_users_master_db 
    WHERE xpm_ccbp_users_master_db.user_id = REPLACE(user_placement_step_interview.user_id,"-","")
  )
)
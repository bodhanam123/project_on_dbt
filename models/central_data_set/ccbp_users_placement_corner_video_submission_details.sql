SELECT
    REPLACE(user_placement_step_video_submissions.user_id,"-","") AS user_id,
    user_placement_step_video_submissions.id AS video_submission_id,
    user_video_submission_evaluation.id AS video_evaluation_id,
    user_placement_step_video_submissions.creation_datetime AS video_submission_datetime,
    user_video_submission_evaluation.creation_datetime AS video_evaluation_datetime,
    user_video_submission_evaluation.evaluation_result AS video_submission_evaluation_result,
    user_placement_step_video_submissions.placement_step_id,
    RANK () OVER (
      PARTITION BY
        user_placement_step_video_submissions.user_id
      ORDER BY
        user_placement_step_video_submissions.creation_datetime,
        user_placement_step_video_submissions.id
    ) AS video_submission_no,

    CASE
        WHEN users_placement_status.placement_status IS NULL
            THEN `project-on-dbt.central_data_set.placement_status_null_enum`()
        ELSE users_placement_status.placement_status
    END AS placement_status
 
    
FROM {{ source('backend_tables', 'nkb_placement_support_userplacementstepvideosubmission') }}  AS user_placement_step_video_submissions 


LEFT JOIN {{ source('backend_tables', 'nkb_placement_support_uservideosubmissionevaluation') }}  AS user_video_submission_evaluation 
ON user_video_submission_evaluation.user_video_submission_id = user_placement_step_video_submissions.id

LEFT JOIN {{ ref('user_placement_status') }}  AS users_placement_status
ON REPLACE(user_placement_step_video_submissions.user_id,"-","") = REPLACE(users_placement_status.user_id,"-","")

WHERE (
    EXISTS(
        SELECT 
            xpm_ccbp_users_master_db.user_id 
        FROM {{ source('backend_tables', 'xpm_ccbp_users_master_db_view') }} AS xpm_ccbp_users_master_db 
        WHERE xpm_ccbp_users_master_db.user_id = REPLACE(user_placement_step_video_submissions.user_id,"-","")
    )
)
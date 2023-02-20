WITH nkb_placement_support_userplacementsection AS (
  SELECT
      REPLACE(user_section_details.user_id,"-","") AS user_id,
      user_section_details.placement_section_id AS section_id,
  FROM {{ source('backend_tables', 'nkb_placement_support_userplacementsection') }}  AS user_section_details
  WHERE (
      EXISTS (
        SELECT
            xpm_ccbp_users_master_db.user_id
        FROM {{ source('backend_tables', 'xpm_ccbp_users_master_db_view') }} AS xpm_ccbp_users_master_db
        WHERE (
            xpm_ccbp_users_master_db.user_id = user_section_details.user_id
        )
      )
  )
),

sections_steps_resources_details AS (
    SELECT 
        section_step_details.*
    FROM {{ ref('placement_corner_sections_steps_resources_details') }} AS section_step_details 
),

no_of_resources_in_step_details AS (
    SELECT 
        section_step_resource_details.step_id,
        COUNT(section_step_resource_details.resource_id) AS total_no_of_resources_in_step
    FROM `sections_steps_resources_details` AS section_step_resource_details 
    WHERE (
        section_step_resource_details.resource_id IS NOT NULL
    )
    GROUP BY 
        section_step_resource_details.step_id
),

placement_corner_sections_steps_details AS (
    SELECT
        DISTINCT
        section_step_details.section_id,
        section_step_details.section_title,
        section_step_details.section_order,
        section_step_details.section_type,
        section_step_details.step_id,
        section_step_details.step_title,
        section_step_details.step_type,
        section_step_details.step_order,
    FROM `sections_steps_resources_details`  AS section_step_details 
    WHERE (
        section_step_details.section_type <> "LEARNING"
    )
),

ccbp_uses_placement_corner_resume_details AS (
    SELECT
        ccbp_users_placement_corner_resume_details.*
    FROM {{ ref('ccbp_users_placement_corner_resume_details') }}  AS ccbp_users_placement_corner_resume_details
),

ccbp_users_plaement_corner_resume_final AS (
    SELECT
        ccbp_uses_placement_corner_resume_details.user_id,
        100 AS completion_percentage,
        CAST(ccbp_uses_placement_corner_resume_details.resume_last_update_datetime AS DATETIME ) AS completion_datetime
    FROM `ccbp_uses_placement_corner_resume_details` AS ccbp_uses_placement_corner_resume_details
    WHERE (
        ccbp_uses_placement_corner_resume_details.is_resume_final = "1"
    )
),

ccbp_users_placement_corner_resume_success AS (
    SELECT
        ccbp_uses_placement_corner_resume_details.user_id,
        50 AS completion_percentage,
        CAST(NULL AS DATETIME) AS completion_datetime
    FROM `ccbp_uses_placement_corner_resume_details` AS ccbp_uses_placement_corner_resume_details
    WHERE (
        NOT EXISTS (
            SELECT
                ccbp_users_plaement_corner_resume_final.user_id
            FROM `ccbp_users_plaement_corner_resume_final` AS ccbp_users_plaement_corner_resume_final
            WHERE (
                ccbp_users_plaement_corner_resume_final.user_id = ccbp_uses_placement_corner_resume_details.user_id
            )
        )
        AND ccbp_uses_placement_corner_resume_details.resume_generation_status = "SUCCESS"
    )
    GROUP BY
        ccbp_uses_placement_corner_resume_details.user_id

),

ccbp_users_placement_corner_resume_unsuccess AS (
    SELECT
        ccbp_uses_placement_corner_resume_details.user_id,
        0 AS completion_percentage,
        CAST(NULL AS DATETIME) AS completion_datetime
    FROM `ccbp_uses_placement_corner_resume_details` AS ccbp_uses_placement_corner_resume_details
    WHERE (
        NOT EXISTS (
            SELECT
                ccbp_users_plaement_corner_resume_final.user_id
            FROM `ccbp_users_plaement_corner_resume_final` AS ccbp_users_plaement_corner_resume_final
            WHERE (
                ccbp_users_plaement_corner_resume_final.user_id = ccbp_uses_placement_corner_resume_details.user_id
            )
        )
        AND NOT EXISTS (
            SELECT
                ccbp_users_placement_corner_resume_success.user_id
            FROM `ccbp_users_placement_corner_resume_success` AS ccbp_users_placement_corner_resume_success
            WHERE (
                ccbp_users_placement_corner_resume_success.user_id = ccbp_uses_placement_corner_resume_details.user_id
            )
        )
    )
    QUALIFY (
          RANK() OVER (
              PARTITION BY
                ccbp_uses_placement_corner_resume_details.user_id
              ORDER BY
                ccbp_uses_placement_corner_resume_details.resume_creation_datetime DESC,
                ccbp_uses_placement_corner_resume_details.unique_resume_id
          ) = 1
      )
),

/* Start: Fetching user resume details */
ccbp_users_placement_corner_resume_completion_details AS (
    SELECT
        ccbp_users_plaement_corner_resume_final.*
    FROM `ccbp_users_plaement_corner_resume_final` AS ccbp_users_plaement_corner_resume_final
    UNION ALL
    SELECT
        ccbp_users_placement_corner_resume_success.*
    FROM `ccbp_users_placement_corner_resume_success` AS ccbp_users_placement_corner_resume_success
    UNION ALL
    SELECT
        ccbp_users_placement_corner_resume_unsuccess.*
    FROM `ccbp_users_placement_corner_resume_unsuccess` AS ccbp_users_placement_corner_resume_unsuccess


),
/* End: Fetching user resume details */


/* Start: Fetching user checklist details */
user_placement_checklist_details AS (
    SELECT 
        REPLACE(user_placement_checklist.user_id,"-","") AS user_id,
        user_placement_checklist.placement_step_id AS step_id,
        CASE 
            WHEN user_placement_checklist.is_accepted = 1 THEN 100 
            ELSE 0 
        END AS completion_percentage,
        CASE 
            WHEN user_placement_checklist.is_accepted = 1
            THEN user_placement_checklist.creation_datetime 
        END AS completion_datetime
    FROM {{ source('backend_tables', 'nkb_placement_support_userplacementstepchecklist') }}  AS user_placement_checklist
),
/* End: Fetching user checklist details */


/* Start: Fetching video submission details */
users_placement_video_submission_details AS (
  SELECT 
      REPLACE(users_placement_video_submissions.user_id,"-","") AS user_id,
      users_placement_video_submissions.step_id,
      CASE 
          WHEN video_submission_evaluation_result = "PASSED" THEN 100 
          WHEN video_submission_id IS NOT NULL THEN 50 
          ELSE 0 
      END AS completion_percentage,
      CASE 
          WHEN video_submission_evaluation_result = "PASSED" THEN video_evaluation_datetime 
      END AS completion_datetime
  FROM (
      SELECT 
        users_placement_video_submissions.user_id,
        users_placement_video_submissions.video_submission_evaluation_result,
        users_placement_video_submissions.video_submission_id,
        users_placement_video_submissions.video_evaluation_datetime,
        users_placement_video_submissions.placement_step_id AS step_id
      FROM {{ ref('ccbp_users_placement_corner_video_submission_details') }}  AS users_placement_video_submissions
      WHERE TRUE
      QUALIFY (
          RANK() OVER (
              PARTITION BY
                users_placement_video_submissions.user_id
              ORDER BY
                users_placement_video_submissions.video_submission_datetime DESC,
                users_placement_video_submissions.video_submission_id
          ) = 1
      )
  ) AS users_placement_video_submissions
),
/* End: Fetching video submission details */

/* Start: Fetching mock interview details */
users_mock_interview_details AS (
    SELECT 
        users_mock_interview_details.user_id,
        users_mock_interview_details.placement_step_id AS step_id,
        CASE 
            WHEN users_mock_interview_details.mock_interview_status = "PASSED" THEN 100
            WHEN users_mock_interview_details.mock_interview_status = "YET_TO_ATTEND" THEN 50
            ELSE 0 
        END AS completion_percentage,
        CASE 
            WHEN users_mock_interview_details.mock_interview_status = "PASSED" THEN users_mock_interview_details.scheduled_datetime 
        END AS completion_datetime
    FROM (
        SELECT
            users_mock_interview_details.user_id,
            users_mock_interview_details.mock_interview_status,
            users_mock_interview_details.scheduled_datetime,
            users_mock_interview_details.placement_step_id
        FROM `project-on-dbt`.`tables`.`ccbp_users_placement_corner_mock_interview_schedule_details`  AS users_mock_interview_details 
        WHERE TRUE 
        QUALIFY (
            RANK() OVER (
                PARTITION BY
                    users_mock_interview_details.user_id
                ORDER BY
                    users_mock_interview_details.scheduled_datetime DESC,
                    users_mock_interview_details.mock_interview_schedule_no DESC
            ) = 1
        )
    ) AS users_mock_interview_details
),
/* End: Fetching mock interview details */



/* Start: Fetching users resource completion details */
users_placement_corner_resource_completion_details AS (
    SELECT 
        users_placement_resources_completion_details.user_id,
        users_placement_resources_completion_details.step_id,
        SUM(users_placement_resources_completion_details.completion_percentage)/MAX(total_no_of_resources_in_step) AS completion_percentage,
        MAX(users_placement_resources_completion_details.completion_datetime) AS completion_datetime
    FROM `project-on-dbt`.`tables`.`ccbp_users_placement_corner_resources_completion_details` AS users_placement_resources_completion_details 
    LEFT JOIN `no_of_resources_in_step_details` AS no_of_resources_in_step 
    ON no_of_resources_in_step.step_id = users_placement_resources_completion_details.step_id
    GROUP BY
        users_placement_resources_completion_details.user_id,
        users_placement_resources_completion_details.step_id
       
)
/* End: Fetching users resource completion details */

SELECT 
    user_section_details.user_id,
    user_section_details.section_id,
    section_step_details.section_title,
    section_step_details.section_order,
    section_step_details.section_type,
    section_step_details.step_id,
    section_step_details.step_title,
    section_step_details.step_type,
    section_step_details.step_order,
    CASE 
      WHEN (
          section_step_details.step_type IN ("LEARNING_SET","STEP_RESOURCES") 
          AND users_placement_corner_resource_completion_details.completion_percentage IS NOT NULL
      ) THEN users_placement_corner_resource_completion_details.completion_percentage
      WHEN (
          section_step_details.step_type = "RESUME_CREATION" 
          AND ccbp_users_placement_corner_resume_completion_details.completion_percentage IS NOT NULL
      ) THEN ccbp_users_placement_corner_resume_completion_details.completion_percentage
      WHEN (
          section_step_details.step_type = "VIDEO_PREPARATION_CHECKLIST" 
          AND user_placement_checklist_details.completion_percentage IS NOT NULL
      ) THEN user_placement_checklist_details.completion_percentage
      WHEN (
          section_step_details.step_type = "VIDEO_SUBMISSION_EVALUATION" 
          AND users_placement_video_submission_details.completion_percentage IS NOT NULL
      ) THEN users_placement_video_submission_details.completion_percentage
      WHEN (
          section_step_details.step_type = "MOCK_INTERVIEW" 
          AND users_mock_interview_details.completion_percentage IS NOT NULL
      ) THEN users_mock_interview_details.completion_percentage
      ELSE 0
    END AS step_completion_percentage,

    CASE 
        WHEN (
            section_step_details.step_type IN ("LEARNING_SET","STEP_RESOURCES") 
            AND users_placement_corner_resource_completion_details.completion_percentage = 100
        )
        THEN CAST(users_placement_corner_resource_completion_details.completion_datetime AS DATETIME)
        
        WHEN section_step_details.step_type = "RESUME_CREATION" 
        THEN CAST(ccbp_users_placement_corner_resume_completion_details.completion_datetime AS DATETIME)
        
        WHEN section_step_details.step_type = "VIDEO_PREPARATION_CHECKLIST" 
        THEN CAST(user_placement_checklist_details.completion_datetime AS DATETIME)
        
        WHEN section_step_details.step_type = "VIDEO_SUBMISSION_EVALUATION" 
        THEN CAST(users_placement_video_submission_details.completion_datetime AS DATETIME)
        
        WHEN section_step_details.step_type = "MOCK_INTERVIEW" 
        THEN CAST(users_mock_interview_details.completion_datetime AS DATETIME)
    END AS step_completion_datetime,

    CASE
      WHEN users_placement_status.placement_status IS NULL
          THEN "To be Placed"
      ELSE users_placement_status.placement_status
    END AS placement_status

FROM `nkb_placement_support_userplacementsection` AS user_section_details

INNER JOIN `placement_corner_sections_steps_details` AS section_step_details 
ON section_step_details.section_id = user_section_details.section_id

LEFT JOIN `users_placement_corner_resource_completion_details` AS users_placement_corner_resource_completion_details
ON users_placement_corner_resource_completion_details.user_id = user_section_details.user_id
AND users_placement_corner_resource_completion_details.step_id = section_step_details.step_id

LEFT JOIN `ccbp_users_placement_corner_resume_completion_details` AS ccbp_users_placement_corner_resume_completion_details
ON ccbp_users_placement_corner_resume_completion_details.user_id = user_section_details.user_id


LEFT JOIN `user_placement_checklist_details` AS user_placement_checklist_details
ON user_placement_checklist_details.user_id = user_section_details.user_id 
AND user_placement_checklist_details.step_id = section_step_details.step_id

LEFT JOIN `users_placement_video_submission_details` AS users_placement_video_submission_details
ON users_placement_video_submission_details.user_id = user_section_details.user_id 
AND users_placement_video_submission_details.step_id = section_step_details.step_id

LEFT JOIN `users_mock_interview_details` AS users_mock_interview_details 
ON users_mock_interview_details.user_id = user_section_details.user_id 
AND users_mock_interview_details.step_id = section_step_details.step_id

LEFT JOIN `project-on-dbt`.`central_data_set`.`user_placement_status` AS users_placement_status
ON user_section_details.user_id = users_placement_status.user_id
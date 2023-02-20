WITH placement_corner_sections_steps_resource_ids_details AS (
    SELECT
        placement_section.id AS section_id,
        placement_section.title AS section_title,
        placement_section.section_type,
        placement_section.order AS section_order,
        placement_section_step.id AS step_id,
        placement_section_step.title AS step_title,
        placement_section_step.step_type,
        placement_section_step.order AS step_order,
        CASE
            WHEN placement_section.section_type = "ORIENTATION" 
            THEN placement_section_step.id
            ELSE REPLACE(placement_section_resource.resource_id,"-","") 
        END AS resource_id,
        CASE
            WHEN placement_section.section_type = "ORIENTATION" 
            THEN 1
            ELSE placement_section_resource.order 
        END AS resource_order

    FROM {{ source('backend_tables', 'nkb_placement_support_placementsection') }}  AS placement_section 

    LEFT JOIN {{ source('backend_tables', 'nkb_placement_support_placementsectionstep') }}  AS placement_section_step 
    ON placement_section_step.placement_section_id = placement_section.id 

    LEFT JOIN {{ source('backend_tables', 'nkb_placement_support_placementsectionstepresource') }}  AS placement_section_resource 
    ON placement_section_resource.placement_step_id = placement_section_step.id 
)

SELECT
    placement_corner_sections_steps_resource_ids_details.*,
    unit_details.unit_title AS resource_title
FROM `placement_corner_sections_steps_resource_ids_details` AS placement_corner_sections_steps_resource_ids_details

LEFT JOIN {{ ref('unit_details') }} AS unit_details
ON unit_details.unit_id = placement_corner_sections_steps_resource_ids_details.resource_id
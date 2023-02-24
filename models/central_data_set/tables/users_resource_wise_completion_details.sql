SELECT 
    userresource.user_id,
    userresource.resource_id ,
    resources_details.resource_title, 
    resources_details.resource_type ,
    resources_details.unit_type,
    userresource.availability_status ,
    userresource.completion_status ,
    userresource.completion_percentage ,
    userresource.completion_datetime ,
    userresource.last_progress_datetime,
    userresource.creation_datetime
    


FROM {{ source('backend_tables', 'nkb_resources_userresource') }}  AS userresource

INNER JOIN {{ ref('resource_details') }} AS resources_details
ON resources_details.resource_id = userresource.resource_id
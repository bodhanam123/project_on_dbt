SELECT 
  users_placement_details.user_id,
  users_placement_details.placement_status
FROM {{ ref('user_placement_details') }}  AS users_placement_details 
WHERE TRUE 
QUALIFY RANK() OVER(
  PARTITION BY 
    users_placement_details.user_id
  ORDER BY 
    users_placement_details.date_of_entry DESC,
    users_placement_details.id DESC
)= 1
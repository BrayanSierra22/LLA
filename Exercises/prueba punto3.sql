SELECT 
   interaction_purpose_descrip,
    COUNT(DISTINCT interaction_id) as Total_interaction_august_2022
FROM 
    "db-stage-prod"."interactions_cwp"
WHERE
    EXTRACT(month from cast(interaction_start_time as date)) = 8
    AND EXTRACT(year from cast(interaction_start_time as date)) = 2022
GROUP BY 
    1
ORDER BY 
    1

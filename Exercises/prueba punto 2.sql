SELECT 
    EXTRACT(MONTH FROM order_start_date) as MONTH,
    COUNT(DISTINCT order_id) as Total_orders
FROM 
    "db-stage-dev"."so_hdr_cwc" 
WHERE
    account_type = 'Residential'
    AND org_cntry = 'Jamaica'
    AND EXTRACT(year from cast(order_start_date as date)) = 2022
GROUP BY 
    1
ORDER BY 
    1

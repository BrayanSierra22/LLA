/* Para hacer mas dinamico y funcional el query no se toma el dia 1 de cada mes sino el primer dia con información existente */
SELECT
    EXTRACT(month from cast(dt as date)) as month,
    MIN(extract(day from cast(dt as date))) as First_day,
    COUNT(DISTINCT act_acct_cd) as Users
FROM 
    "db-analytics-prod"."fixed_cwp" 
WHERE act_cust_typ_nm = 'Residencial'
    AND extract(year from cast(dt as date)) = 2022
GROUP BY 
    1
ORDER BY 
    1

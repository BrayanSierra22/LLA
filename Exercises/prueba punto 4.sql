/* Para hacer mas dinamico y funcional el query no se toma el dia 1 de cada mes sino el primer dia con información existente */
WITH

Base_Auxiliar as (
SELECT
dt,
act_acct_cd,
case
when date_diff('day', cast(act_acct_inst_dt as date), cast(dt as date))<180 then 'Early Tenure' 
when date_diff('day', cast(act_acct_inst_dt as date), cast(dt as date))>=180 and date_diff('day', cast(act_acct_inst_dt as date), cast(dt as date))<360 then 'Mid Tenure'
when date_diff('day', cast(act_acct_inst_dt as date), cast(dt as date))>=360 then 'Late Tenure'
else 'null' end as Tenure
FROM "db-analytics-prod"."fixed_cwp" 
 where act_cust_typ_nm = 'Residencial'
 and extract(year from cast(dt as date)) = 2022
)

SELECT
extract(month from cast(dt as date)) as Month,
min(extract(day from cast(dt as date))) as First_day,
Tenure,
count(distinct act_acct_cd) as Users
from Base_Auxiliar

group by 1,3
order by 1,3

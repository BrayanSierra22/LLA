--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- ##### LCPR - SPRINT 3 - OPERATIONAL DRIVERS - FULL FLAGS TABLE ##### --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

--- WARNING: Estimated runtime of 7 minutes.
--- December may take more than 20 minutes for some reason.
CREATE TABLE IF NOT EXISTS "db_stage_dev"."test_op_drivers" AS
WITH

parameters as (SELECT date('2023-01-01') as input_month)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- FMC Table --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, fmc_table as (
SELECT
    *
FROM "lla_cco_lcpr_ana_dev"."lcpr_fmc_churn_dev"
WHERE 
    fmc_s_dim_month = (SELECT input_month FROM parameters)
    and fmc_s_dim_month = date(dt)
)

, repeated_accounts as (
SELECT 
    fmc_s_dim_month, 
    fix_s_att_account,
    row_number() over(partition by fmc_s_dim_month,fix_s_att_account) as records_per_user
    -- count(*) as records_per_user
FROM fmc_table
WHERE 
    fix_s_att_account is not null
--GROUP BY 1, 2
-- ORDER BY 3 desc
)

, fmc_table_adj as (
SELECT 
    F.*,
    records_per_user
FROM fmc_table F
LEFT JOIN repeated_accounts R
    ON F.fix_s_att_account = R.fix_s_att_account and F.fmc_s_dim_month = R.fmc_s_dim_month
    where records_per_user = 1
)


--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- New customers --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, new_customers_pre as (
SELECT
    date_trunc('month', date(dt)) as dna_month,
    cast(cast(first_value(connect_dte_sbb) over (partition by sub_acct_no_sbb order by DATE(dt) DESC) as timestamp) as date) as fix_b_att_maxstart,   
    SUB_ACCT_NO_SBB as fix_s_att_account, 
    bill_from_dte_sbb, 
    --- The total MRC must be calculated summing up the charges for the different fixed services.
    (video_chrg + hsd_chrg + voice_chrg) as fi_tot_mrc_amt,
    delinquency_days,
    dt
FROM "db-stage-prod-lf"."insights_customer_services_rates_lcpr" 
WHERE 
    play_type != '0P'
    and cust_typ_sbb = 'RES' 
    and date_trunc('month', date(CONNECT_DTE_SBB)) between ((SELECT input_month FROM parameters) - interval '3' month) and (SELECT input_month FROM parameters)
ORDER BY 1
)


, new_customers as (   
SELECT 
    dna_month,
    date_trunc('month', fix_b_att_maxstart) as install_month, 
    fix_b_att_maxstart,  
    fix_s_att_account as new_sales_flag,
    fix_s_att_account, 
    fi_tot_mrc_amt,
    delinquency_days,
    bill_from_dte_sbb, 
    dt
FROM new_customers_pre
WHERE date_trunc('month', date(fix_b_att_maxstart)) = (SELECT input_month FROM parameters)
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Interactions and truckrolls --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, clean_interaction_time as (
SELECT *
FROM "db-stage-prod-lf"."interactions_lcpr"
WHERE
    cast(interaction_start_time as varchar) != ' ' 
    and interaction_start_time is not null
    and date_trunc('month', date(interaction_start_time)) between (SELECT input_month FROM parameters) and ((SELECT input_month FROM parameters) + interval '2' month)
    and account_type = 'RES'
)

, interactions_fields as (
SELECT
    *, 
    cast(substr(cast(interaction_start_time as varchar), 1, 10) as date) as interaction_date, 
    date_trunc('month', cast(substr(cast(interaction_start_time as varchar), 1, 10) as date)) as month
FROM clean_interaction_time
)

, interactions_not_repeated as (
SELECT
    first_value(interaction_id) OVER(PARTITION BY account_id, interaction_date, interaction_channel, interaction_agent_id, interaction_purpose_descrip ORDER BY interaction_date DESC) AS interaction_id2
FROM interactions_fields
)

, interactions_fields2 as (
SELECT *
FROM interactions_not_repeated a
LEFT JOIN interactions_fields b
    ON a.interaction_id2 = b.interaction_id
)

, truckrolls as (
SELECT 
    create_dte_ojb, 
    job_no_ojb, 
    sub_acct_no_sbb
FROM "lcpr.stage.dev"."truckrolls" 
WHERE 
    date_trunc('month', date(create_dte_ojb)) between (SELECT input_month FROM parameters) and ((SELECT input_month FROM parameters) + interval '2' month)
)

, full_interactions as (
SELECT 
    *, 
    case 
    
        when create_dte_ojb is not null then 'truckroll'
    
        when (
        lower(interaction_purpose_descrip) like '%ppv%problem%'
        or lower(interaction_purpose_descrip) like '%hsd%problem%'
        or lower(interaction_purpose_descrip) like '%cable%problem%'
        or lower(interaction_purpose_descrip) like '%tv%problem%'
        or lower(interaction_purpose_descrip) like '%video%problem%'
        or lower(interaction_purpose_descrip) like '%tel%problem%'
        or lower(interaction_purpose_descrip) like '%phone%problem%'
        or lower(interaction_purpose_descrip) like '%int%problem%'
        or lower(interaction_purpose_descrip) like '%line%problem%'
        or lower(interaction_purpose_descrip) like '%hsd%issue%'
        or lower(interaction_purpose_descrip) like '%ppv%issue%'
        or lower(interaction_purpose_descrip) like '%video%issue%'
        or lower(interaction_purpose_descrip) like '%tel%issue%'
        or lower(interaction_purpose_descrip) like '%phone%issue%'
        or lower(interaction_purpose_descrip) like '%int%issue%'
        or lower(interaction_purpose_descrip) like '%line%issue%'
        or lower(interaction_purpose_descrip) like '%cable%issue%'
        or lower(interaction_purpose_descrip) like '%tv%issue%'
        or lower(interaction_purpose_descrip) like '%bloq%'
        or lower(interaction_purpose_descrip) like '%slow%'
        or lower(interaction_purpose_descrip) like '%slow%service%'
        or lower(interaction_purpose_descrip) like '%service%tech%'
        or lower(interaction_purpose_descrip) like '%tech%service%'
        or lower(interaction_purpose_descrip) like '%no%service%'
        or lower(interaction_purpose_descrip) like '%hsd%no%'
        or lower(interaction_purpose_descrip) like '%hsd%slow%'
        or lower(interaction_purpose_descrip) like '%hsd%intermit%'
        or lower(interaction_purpose_descrip) like '%no%brows%'
        or lower(interaction_purpose_descrip) like '%phone%cant%'
        or lower(interaction_purpose_descrip) like '%phone%no%'
        or lower(interaction_purpose_descrip) like '%no%connect%'
        or lower(interaction_purpose_descrip) like '%no%conect%'
        or lower(interaction_purpose_descrip) like '%no%start%'
        or lower(interaction_purpose_descrip) like '%equip%'
        or lower(interaction_purpose_descrip) like '%intermit%'
        or lower(interaction_purpose_descrip) like '%no%dat%'
        or lower(interaction_purpose_descrip) like '%dat%serv%'
        or lower(interaction_purpose_descrip) like '%int%data%'
        or lower(interaction_purpose_descrip) like '%tech%'
        or lower(interaction_purpose_descrip) like '%supp%'
        or lower(interaction_purpose_descrip) like '%outage%'
        or lower(interaction_purpose_descrip) like '%mass%'
        or lower(interaction_purpose_descrip) like '%discon%warn%'
        ) and (
        lower(interaction_purpose_descrip) not like '%work%order%status%'
        and lower(interaction_purpose_descrip) not like '%default%call%wrapup%'
        and lower(interaction_purpose_descrip) not like '%bound%call%'
        and lower(interaction_purpose_descrip) not like '%cust%first%'
        and lower(interaction_purpose_descrip) not like '%audit%'
        and lower(interaction_purpose_descrip) not like '%eq%code%'
        and lower(interaction_purpose_descrip) not like '%downg%'
        and lower(interaction_purpose_descrip) not like '%upg%'
        and lower(interaction_purpose_descrip) not like '%vol%discon%'
        and lower(interaction_purpose_descrip) not like '%discon%serv%'
        and lower(interaction_purpose_descrip) not like '%serv%call%'
        )
        then 'tech_call'
        
        else null
        
        end as interaction_type
        
FROM interactions_fields2 a
FULL OUTER JOIN truckrolls b
    ON a.interaction_date = cast(create_dte_ojb as date) and cast(a.account_id as varchar) = cast(b.sub_acct_no_sbb as varchar)
WHERE
    interaction_purpose_descrip not in ('Work Order Status', 'Default Call Wrapup', 'G:outbound Calls', 'Eq: Cust. First', 'Eq: Audit', 'Eq: Code Error', 'Downgrade Service', 'Disconnect Service', 'Rt: Dowgrde Service', 'Cust Service Calls')

)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- Never Paid (using Payments Table as in CWP) --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

,bills_of_interest AS (
SELECT 
    dna_month,
    fix_s_att_account,
    --- I take the equivalent to oldest_unpaid_bill because no much more alternative info is available
    first_value(bill_from_dte_sbb) over (partition by fix_s_att_account order by dt asc) as first_bill_created
FROM new_customers
)

, mrc_calculation as (
SELECT
    nc.fix_s_att_account, 
    min(first_bill_created) as first_bill_created, 
    max(fi_tot_mrc_amt) as max_tot_mrc, 
    array_agg(distinct fi_tot_mrc_amt order by fi_tot_mrc_amt desc) as arreglo_mrc
FROM new_customers nc
INNER JOIN bills_of_interest bi
    ON nc.fix_s_att_account = bi.fix_s_att_account 
        and date(nc.dt) between date(first_bill_created) and (date(first_bill_created) + interval '3' month)
GROUP BY nc.fix_s_att_account
)

, first_cycle_info as (
SELECT
    nc.fix_s_att_account, 
    min(fix_b_att_maxstart) as first_installation_date, 
    min(first_bill_created) as first_bill_created, 
    try(array_agg(arreglo_mrc)[1]) as arreglo_mrc, 
    max(delinquency_days) as max_delinquency_days_first_bill, 
    max(max_tot_mrc) as max_mrc_first_bill, 
    count(distinct max_tot_mrc) as diff_mrc
FROM new_customers nc
INNER JOIN mrc_calculation mrcc
    ON nc.fix_s_att_account = mrcc.fix_s_att_account
WHERE 
    date(nc.bill_from_dte_sbb) = date(mrcc.first_bill_created)
GROUP BY nc.fix_s_att_account
)

, payments_basic as (
SELECT  
    account_id as fix_s_att_account_payments, 
    try(array_agg(date(dt) order by date(dt))[1]) as first_payment_date, 
    try(array_agg(cast(payment_amt_usd as double) order by date(dt))[1]) as first_payment_amt, 
    try(filter(array_agg(case when cast(payment_amt_usd as double) >= max_mrc_first_bill then date(dt) else null end order by date(dt)), x -> x is not null)[1]) as first_payment_above_date, 
    try(filter(array_agg(case when cast(payment_amt_usd as double) >= max_mrc_first_bill then cast(payment_amt_usd as double) else null end order by date(dt)), x -> x is not null)[1]) as first_payment_above, 
    array_agg(cast(payment_amt_usd as double) order by date(dt)) as arreglo_pagos, 
    try(array_agg(date(dt) order by date(dt))[1]) as first_pay_date, 
    try(array_agg(date(dt) order by date(dt) desc)[1]) as last_pay_date, 
    array_agg(date(dt) order by date(dt)) as arreglo_pagos_dates, 
    
    round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<30, cast(payment_amt_usd as double),null)),2) as total_payments_30_days
    ,round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<60, cast(payment_amt_usd as double),null)),2) as total_payments_60_days
    ,round(sum(if(date_diff('day', date(fc.first_bill_created), date(dt))<85, cast(payment_amt_usd as double),null)),2) as total_payments_85_days
    
FROM "lcpr.stage.prod"."payments_lcpr" p
INNER JOIN first_cycle_info as fc
    ON cast(fc.fix_s_att_account as varchar) = cast(p.account_id as varchar)
WHERE 
    date(dt) between (fc.first_bill_created - interval '50' day) and (fc.first_bill_created + interval '85' day)
GROUP BY account_id
)

, npn_85 as (
SELECT
    *, 
    fc.fix_s_att_account as fix_s_att_account_def,
    date_diff('day', first_bill_created, first_payment_above_date) as days_between_payment, 
    case when first_payment_above_date is null then 86 else date_diff('day', first_bill_created, first_payment_above_date) end as fixed_days_unpaid_bill, 
    case
        when total_payments_30_days is null then fc.fix_s_att_account
        when total_payments_30_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_30_flag,
    
    case
        when total_payments_60_days is null then fc.fix_s_att_account
        when total_payments_60_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_60_flag,
    case
        when total_payments_85_days is null then fc.fix_s_att_account
        when total_payments_85_days < max_mrc_first_bill then fc.fix_s_att_account else null 
    end as npn_85_flag
FROM first_cycle_info fc
LEFT JOIN payments_basic p 
    ON cast(p.fix_s_att_account_payments as varchar) = cast(fc.fix_s_att_account as varchar)
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Early tickets --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, relevant_interactions as (
SELECT
    customer_id, 
    interaction_id, 
    job_no_ojb,
    interaction_type,
    min(interaction_date) as min_interaction_date, 
    min(date_trunc('month', date(interaction_date))) as interaction_start_month 
FROM full_interactions
GROUP BY 1, 2, 3, 4
)

, early_tickets AS (
SELECT 
    A.fix_s_att_account, 
    --new_sales2m_flag,
    install_month, 
    interaction_start_month, 
    fix_b_att_maxstart,
    case when date_diff('week', date(fix_b_att_maxstart), date(min_interaction_date)) <= 7 then fix_s_att_account else null end as early_ticket_flag
FROM new_customers A 
LEFT JOIN relevant_interactions B 
    ON cast(A.fix_s_att_account as varchar) = cast(B.customer_id as varchar)
WHERE interaction_type in ('tech_call', 'truckroll')
)

--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Outlier Installs --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, installations as (
SELECT
    *
FROM "db-stage-prod-lf"."so_ln_lcpr"
WHERE
    org_id = 'LCPR' and org_cntry = 'PR'
    and order_status = 'COMPLETE'
    and command_id = 'CONNECT'
)

, outlier_installs as (
SELECT
    fix_s_att_account, 
    fix_b_att_maxstart, 
    new_sales_flag,
    install_month,
    cast(cast(order_start_date as timestamp) as date) as order_start_date, 
    cast(cast(completed_date as timestamp) as date) as completed_date, 
    case when date_diff('day', date(order_start_date), date(completed_date)) > 6 then fix_s_att_account else null end as outlier_install_flag
FROM new_customers a
LEFT JOIN installations b
    ON cast(a.fix_s_att_account as varchar) = cast(b.account_id as varchar)
)


--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- Final flags --- --- --- --- --- --- --- --- --- --- ---
--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

, final_flags as (
SELECT
    F.*, 
    I.new_sales_flag,
    npn_30_flag,
    npn_60_flag,
    npn_85_flag,
    early_ticket_flag
    
   
FROM fmc_table_adj F
LEFT JOIN new_customers I ON cast(F.fix_s_att_account as varchar) = cast(I.fix_s_att_account as varchar)
LEFT JOIN npn_85 N  ON cast(F.fix_s_att_account as varchar) = cast(N.fix_s_att_account_def as varchar)
LEFT JOIN early_tickets E ON cast(F.fix_s_att_account as varchar) = cast(E.fix_s_att_account as varchar)

WHERE
    F.fmc_s_dim_month = (SELECT input_month FROM parameters)
    -- and fix_e_att_active = 1
)
, final_table as (
SELECT 
    fmc_s_dim_month as opd_s_dim_month,
    fmc_e_fla_tech as opd_e_fla_final_tech,
    fmc_e_fla_fmcsegment as opd_e_fla_opd_segment,
    fmc_e_fla_fmc as opd_e_fla_opd_type,
    fmc_e_fla_tenure as opd_e_fla_final_tenure,
    count(distinct case when fix_e_att_active = 1 then fix_s_att_account else null end) as opd_s_mes_active_base,
    count(distinct fix_s_att_account) as opd_s_mes_soft_dx,
    count(distinct new_sales_flag) as opd_s_mes_sales,
    count(distinct npn_30_flag) as opd_s_mes_never_paid_30_days,
    count(distinct npn_60_flag) as opd_s_mes_never_paid_60_days,
    count(distinct npn_85_flag) as opd_s_mes_never_paid_85_days,  
    count(distinct early_ticket_flag) as opd_s_mes_uni_early_tickets
FROM final_flags
WHERE 
    fmc_s_fla_churnflag != 'Fixed Churner' 
    and fmc_s_fla_waterfall not in ('Downsell-Fixed Customer Gap', 'Fixed Base Exception', 'Churn Exception') 
    and fix_s_fla_mainmovement != '6.Null last day'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
)
 SELECT * FROM final_table

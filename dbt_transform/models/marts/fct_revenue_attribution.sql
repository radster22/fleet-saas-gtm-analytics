with opps as (
    select * from {{ ref('stg_sfdc_opportunities') }}
    where is_closed_won = true
),

first_web_visits as (
    select 
        contact_id,
        marketing_channel,
        visit_timestamp,
        row_number() over (partition by contact_id order by visit_timestamp asc) as rn
    from {{ ref('stg_hubspot_web_visits') }}
),

first_touch as (
    select * from first_web_visits where rn = 1
),

contacts as (
    select * from {{ ref('stg_hubspot_contacts') }}
)

select
    o.opportunity_id,
    o.contact_id,
    c.restaurant_name,
    o.arr_amount,
    ft.marketing_channel as acquiring_channel,
    o.opp_closed_at
from opps o
left join contacts c on o.contact_id = c.contact_id
left join first_touch ft on o.contact_id = ft.contact_id

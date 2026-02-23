with opps as (
    select * from {{ ref('stg_sfdc_opportunities') }}
    where is_closed_won = true
),

first_touch as (
    -- Calling the macro to get the very first web visit per contact
    {{ get_top_row(
        model=ref('stg_hubspot_web_visits'),
        partition_by='contact_id',
        order_by='visit_timestamp asc'
    ) }}
),

contacts as (
    select * from {{ ref('stg_hubspot_contacts') }}
)

select
    o.opportunity_id,
    o.contact_id,
    c.company_name,
    o.arr_amount,
    ft.marketing_channel as acquiring_channel,
    o.opp_closed_at,

    {{ add_audit_columns() }}
from opps o
left join contacts c on o.contact_id = c.contact_id
left join first_touch ft on o.contact_id = ft.contact_id
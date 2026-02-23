with contacts as (
    select * from {{ ref('stg_hubspot_contacts') }}
),

opps as (
    select * from {{ ref('stg_sfdc_opportunities') }}
),

latest_opp_stage as (
    -- Calling the macro to get the most recent opportunity per contact
    {{ get_top_row(
        model='opps',
        partition_by='contact_id',
        order_by='coalesce(opp_closed_at, opp_created_at) desc, opp_created_at desc, opportunity_id desc'
    ) }}
),

web_visits as (
    select
        contact_id,
        count(visit_id) as total_web_visits
    from {{ ref('stg_hubspot_web_visits') }}
    group by 1
)

select
    c.contact_id,
    c.first_name,
    c.last_name,
    c.company_name,
    c.contact_created_at,
    coalesce(w.total_web_visits, 0) as total_web_visits,
    count(o.opportunity_id) as total_opportunities,
    coalesce(sum(case when o.is_closed_won then o.arr_amount else 0 end), 0) as total_arr_generated,
    los.opp_stage as current_pipeline_stage,

    {{ add_audit_columns() }}
from contacts c
left join web_visits w on c.contact_id = w.contact_id
left join opps o on c.contact_id = o.contact_id
left join latest_opp_stage los on c.contact_id = los.contact_id
group by 1, 2, 3, 4, 5, 6, 9
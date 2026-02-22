select
    opportunity_id,
    contact_id,
    stage as opp_stage,
    cast(arr_amount as float) as arr_amount,
    cast(created_date as timestamp) as opp_created_at,
    cast(close_date as timestamp) as opp_closed_at,
    case when stage = 'Closed Won' then true else false end as is_closed_won
from {{ source('gtm_raw_sources', 'raw_sfdc_opportunities') }}
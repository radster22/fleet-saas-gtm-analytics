select
    trim(opportunity_id) as opportunity_id,
    trim(contact_id) as contact_id,
    trim(stage) as opp_stage,
    cast(arr_amount as float) as arr_amount,
    cast(created_date as timestamp) as opp_created_at,
    cast(close_date as timestamp) as opp_closed_at,
    case 
        when trim(stage) = 'Closed Won' then true 
        else false 
    end as is_closed_won
from {{ source('gtm_raw_sources', 'raw_sfdc_opportunities') }}
where opportunity_id is not null
  and contact_id is not null
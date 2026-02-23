select
    trim(visit_id) as visit_id,
    trim(contact_id) as contact_id,
    coalesce(trim(utm_source), 'Direct') as marketing_channel,
    trim(page_visited) as page_visited,
    cast(visit_timestamp as timestamp) as visit_timestamp
from {{ source('gtm_raw_sources', 'raw_hubspot_web_visits') }}
where visit_id is not null
  and contact_id is not null
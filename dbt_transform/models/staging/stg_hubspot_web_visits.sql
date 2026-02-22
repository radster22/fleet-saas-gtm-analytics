select
    visit_id,
    contact_id,
    utm_source as marketing_channel,
    page_visited,
    cast(visit_timestamp as timestamp) as visit_timestamp
from {{ source('gtm_raw_sources', 'raw_hubspot_web_visits') }}
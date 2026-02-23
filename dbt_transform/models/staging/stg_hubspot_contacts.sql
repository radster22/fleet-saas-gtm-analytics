select
    trim(contact_id) as contact_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    trim(company_name) as company_name,
    cast(contact_created_at as timestamp) as contact_created_at
from {{ source('gtm_raw_sources', 'raw_hubspot_contacts') }}
where contact_id is not null
  and lower(company_name) not like '%test%'
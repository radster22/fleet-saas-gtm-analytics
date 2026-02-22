select
    contact_id,
    first_name,
    last_name,
    company_name,
    cast(contact_created_at as timestamp) as contact_created_at
from {{ source('gtm_raw_sources', 'raw_hubspot_contacts') }}
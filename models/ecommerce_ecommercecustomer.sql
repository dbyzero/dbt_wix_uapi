{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_contacts".id ||
      'contacts' ||
      'wix'
    )  as id,
    'wix' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_contacts._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_contacts".id as external_id,
    "{{ var("table_prefix") }}_contacts".info->'name'->>'first' as firstname,
    "{{ var("table_prefix") }}_contacts".info->'name'->>'last'as lastname,
    NULL as username,
    NULL::date as birthday,
    "{{ var("table_prefix") }}_contacts".primaryinfo->>'email' as email,
    "{{ var("table_prefix") }}_contacts".info->'addresses'->'items'->0->'address'->>'formattedAddress' as address,
    NULL::boolean as email_marketing_consent,
    NULL::float as order_count,
    NULL as state,
    NULL::float as total_spent,
    NULL as note,
    "{{ var("table_prefix") }}_contacts".primaryinfo->>'phone' as phone,
    ("{{ var("table_prefix") }}_contacts".info->'addresses'->>'items')::jsonb as addresses,
    NULL as tags,
    NULL as role,
    0::boolean as tax_exempt,
    NULL::jsonb as billing,
    NULL::jsonb as shipping,
    NULL as avatar_url,
    NULL::boolean as is_guest,
    TRUE as active,
    "{{ var("table_prefix") }}_contacts".updateddate as service_date_updated,
    "{{ var("table_prefix") }}_contacts".createddate as service_date_created 
FROM "{{ var("table_prefix") }}_contacts"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_contacts
ON _airbyte_raw_{{ var("table_prefix") }}_contacts._airbyte_ab_id = "{{ var("table_prefix") }}_contacts"._airbyte_ab_id
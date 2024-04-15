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
      "{{ var("table_prefix") }}_customers".id ||
      'customer' ||
      'wix'
    )  as id,
    'wix' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_customers._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_customers".id as external_id,
    "{{ var("table_prefix") }}_customers".first_name as firstname,
    "{{ var("table_prefix") }}_customers".last_name as lastname,
    NULL as username,
    NULL::date as birthday,
    "{{ var("table_prefix") }}_customers".email as email,
    "{{ var("table_prefix") }}_customers".default_address as address,
    NULL::boolean as email_marketing_consent,
    "{{ var("table_prefix") }}_customers".orders_count as order_count,
    "{{ var("table_prefix") }}_customers".state,
    "{{ var("table_prefix") }}_customers".total_spent,
    "{{ var("table_prefix") }}_customers".note,
    "{{ var("table_prefix") }}_customers".phone,
    "{{ var("table_prefix") }}_customers".addresses as addresses,
    "{{ var("table_prefix") }}_customers".last_name as last_name,
    "{{ var("table_prefix") }}_customers".tags,
    NULL as role,
    "{{ var("table_prefix") }}_customers".tax_exempt,
    NULL::jsonb as billing,
    NULL::jsonb as shipping,
    NULL as avatar_url,
    NULL::boolean as is_guest,
    TRUE as active,
    "{{ var("table_prefix") }}_customers".updated_at as service_date_updated,
    "{{ var("table_prefix") }}_customers".created_at as service_date_created 
FROM "{{ var("table_prefix") }}_customers"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_customers
ON _airbyte_raw_{{ var("table_prefix") }}_customers._airbyte_ab_id = "{{ var("table_prefix") }}_customers"._airbyte_ab_id

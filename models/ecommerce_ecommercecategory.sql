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
      "{{ var("table_prefix") }}_collections".id ||
      'category' ||
      'wix'
    )  as id,
    'wix' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_collections._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_collections".id as external_id,
    "{{ var("table_prefix") }}_collections".name as name,
    "{{ var("table_prefix") }}_collections".slug as slug,
    NULL as description, 
    NULL::date as created_date,
    NULL::date as updated_date,
    NULL as sort_order,
    NULL as template_suffix,
    NULL::float as products_count,
    NULL as type,
    NULL as published_scope,
    "{{ var("table_prefix") }}_collections".visible as active,
    NULL::float as level_depth,
    NULL as parent_category_id
FROM "{{ var("table_prefix") }}_collections"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_collections
ON _airbyte_raw_{{ var("table_prefix") }}_collections._airbyte_ab_id = "{{ var("table_prefix") }}_collections"._airbyte_ab_id

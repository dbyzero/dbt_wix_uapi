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
      'shopify'
    )  as id,
    'shopify' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_collections._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_collections".id as external_id,
    "{{ var("table_prefix") }}_collections".title as name,
    "{{ var("table_prefix") }}_collections".title as slug,
    "{{ var("table_prefix") }}_collections".body_html as description, 
    "{{ var("table_prefix") }}_collections".published_at as created_date,
    "{{ var("table_prefix") }}_collections".updated_at as updated_date,
    "{{ var("table_prefix") }}_collections".sort_order as sort_order,
    NULL  as template_suffix,
    NULL::float as products_count,
    "{{ var("table_prefix") }}_collections".collection_type as type,
    NULL as published_scope,
    NULL::boolean as active,
    NULL::float as level_depth,
    NULL as parent_category_id
FROM "{{ var("table_prefix") }}_collections"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_collections
ON _airbyte_raw_{{ var("table_prefix") }}_collections._airbyte_ab_id = "{{ var("table_prefix") }}_collections"._airbyte_ab_id
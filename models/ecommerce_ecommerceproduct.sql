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
      "{{ var("table_prefix") }}_products".id ||
      'product' ||
      'wix'
    )  as id,
    'wix' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_products".id as external_id,
    "{{ var("table_prefix") }}_products".name as name,
    "{{ var("table_prefix") }}_products".description as description,
    "{{ var("table_prefix") }}_products".description as short_description ,
    "{{ var("table_prefix") }}_products".numericid as reference,
    "{{ var("table_prefix") }}_products".producttype as type ,
    NULL as url ,
    "{{ var("table_prefix") }}_products".variants as variations ,
    NULL::float as quantity_available,
    NULL::float as minimal_quantity,
    CASE WHEN ("{{ var("table_prefix") }}_products".stock->>'inStock')::boolean = TRUE THEN 'Available' ELSE 'Unavailable' END AS stock_status ,
    jsonb_build_array("{{ var("table_prefix") }}_products".media->'mainMedia'->'image'->>'url') as images,
    NULL::jsonb  as tags,
    ("{{ var("table_prefix") }}_products".stock->>'inStock')::boolean AS purchasable,
    ("{{ var("table_prefix") }}_products".price->>'price')::float as regular_price ,
    ("{{ var("table_prefix") }}_products".price->>'price')::float as sale_price ,
    ("{{ var("table_prefix") }}_products".price->>'price')::float as price ,
    NULL::float  as total_sales ,
    NULL::boolean as on_sale ,
    NULL as rate ,
    "{{ var("table_prefix") }}_products".sku,
    "{{ var("table_prefix") }}_products".slug,
    CASE WHEN ("{{ var("table_prefix") }}_products".stock->>'inStock')::boolean = TRUE THEN 'Available' ELSE 'Unavailable' END AS status ,
    NULL::boolean as virtual ,
    "{{ var("table_prefix") }}_products".weight,
    NULL as ean13 ,
    NULL::float as height ,
    NULL::float as width ,
    NULL as location ,
    NULL as manufacturer_name ,
    "{{ var("table_prefix") }}_products".price->'currency' as unity
FROM "{{ var("table_prefix") }}_products"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_products
ON _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_ab_id = "{{ var("table_prefix") }}_products"._airbyte_ab_id

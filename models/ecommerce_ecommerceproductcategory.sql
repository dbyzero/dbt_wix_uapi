{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

WITH source_data AS (
    SELECT
        id as product_id,
        jsonb_array_elements_text(collectionids) AS category_id
    FROM "{{ var("table_prefix") }}_products"
)


SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
        '{{ var("integration_id") }}' ||
        source_data.product_id ||
        source_data.category_id ||
        'productcategory' ||
        'wix'
    ) AS id,
    'wix' as source,
    '{{ var("integration_id") }}'::uuid AS integration_id,
    NULL::jsonb as last_raw_data,
    '{{ var("timestamp") }}' as sync_timestamp,
    source_data.product_id as external_id,
    md5(
        '{{ var("integration_id") }}' ||
        source_data.product_id ||
        'product' ||
        'wix'
    ) as product_id,
    md5(
        '{{ var("integration_id") }}' ||
        source_data.category_id ||
        'category' ||
        'wix'
    ) as category_id
FROM source_data
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
      "{{ var("table_prefix") }}_orders".id ||
      'order' ||
      'wix'
    )  as id,
    'wix' as source,
    '{{ var("integration_id") }}'::uuid as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_orders._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_orders".id as external_id,
    "{{ var("table_prefix") }}_orders".number as reference,
    "{{ var("table_prefix") }}_orders".number as number,
    NULL as created_via,
    NULL as version,
    "{{ var("table_prefix") }}_orders".paymentstatus as status,
    "{{ var("table_prefix") }}_orders".currency as currency,
    ("{{ var("table_prefix") }}_orders".totals->>'discount')::float as discount_total,
    NULL as discount_tax,
    ("{{ var("table_prefix") }}_orders".totals->>'shipping')::float as shipping_total,
    NULL as shipping_tax,
    NULL as cart_tax,
    ("{{ var("table_prefix") }}_orders".totals->>'total')::float as total,
    ("{{ var("table_prefix") }}_orders".totals->>'tax')::float as total_tax,
    NULL::boolean as prices_include_tax,
    "{{ var("table_prefix") }}_orders".buyernote as customer_note,
    "{{ var("table_prefix") }}_orders".billinginfo as billing,
    "{{ var("table_prefix") }}_orders".shippinginfo as shipping,
    ("{{ var("table_prefix") }}_orders".channelinfo->>'type') as payment_method,
    "{{ var("table_prefix") }}_orders".checkoutid as transaction_id,
    NULL::date as date_paid,
    NULL::date as date_completed,
    "{{ var("table_prefix") }}_orders".lineitems as lines,
    NULL::jsonb as tax_lines,
    NULL::jsonb as shipping_lines,
    NULL::jsonb as fee_lines,
    NULL::jsonb as coupon_lines,
    "{{ var("table_prefix") }}_orders".refunds as refunds,
    ("{{ var("table_prefix") }}_orders".paymentstatus = 'PAID')::boolean as paid,
    "{{ var("table_prefix") }}_orders".lastupdated as service_date_updated,
    "{{ var("table_prefix") }}_orders".datecreated as service_date_created,
    "{{ var("table_prefix") }}_orders".number as invoice_number,
    NULL::date as invoice_date,
    NULL as delivery_number,
    NULL::date as delivery_date,
    ("{{ var("table_prefix") }}_orders".archived = False)::boolean as valid,
    NULL as shipping_number,
        -- "{{ var("table_prefix") }}_orders".buyerInfo->>'id' as customer_id
    CASE
      WHEN (
        "{{ var("table_prefix") }}_orders".buyerInfo->>'id'
      ) is not NULL THEN md5(
        '{{ var("integration_id") }}' || ("{{ var("table_prefix") }}_orders".buyerInfo->>'id')::text || 'contacts' || 'wix'
      )
      ELSE NULL
    END as customer_id
FROM "{{ var("table_prefix") }}_orders"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_orders
ON _airbyte_raw_{{ var("table_prefix") }}_orders._airbyte_ab_id = "{{ var("table_prefix") }}_orders"._airbyte_ab_id

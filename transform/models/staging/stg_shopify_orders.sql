with source as (
    select * from {{ source('raw', 'shopify_orders') }}
),

cleaned as (
    select
        order_id,
        created_at,
        total_price,
        subtotal_price,
        total_tax,
        currency,
        landing_site,
        fetched_at
    from source
    where order_id is not null
      and total_price >= 0
)

select * from cleaned

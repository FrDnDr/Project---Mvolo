with source as (
    select * from {{ source('raw', 'shopify_order_items') }}
),

cleaned as (
    select
        line_item_id,
        order_id,
        sku,
        name,
        quantity,
        price,
        total_discount,
        fetched_at
    from source
    where line_item_id is not null
      and order_id is not null
      and quantity > 0
      and price >= 0
)

select * from cleaned

with source as (
    select * from {{ source('raw', 'bol_orders') }}
),

cleaned as (
    select
        order_id,
        order_item_id,
        -- order_placed_at is stored as VARCHAR in raw — cast to timestamp
        try_cast(order_placed_at as timestamp)  as order_placed_at,
        fulfillment_method,
        ean,
        title,
        quantity,
        unit_price,
        total_price,
        commission,
        fetched_at
    from source
    where order_id is not null
      and order_item_id is not null
      and quantity > 0
      and unit_price > 0
)

select * from cleaned

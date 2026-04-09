with source as (
    select * from {{ source('raw', 'bol_offers') }}
),

cleaned as (
    select
        offer_id,
        ean,
        product_title,
        stock_amount,
        corrected_stock,
        managed_by_retailer,
        unit_price,
        fulfillment_method,
        delivery_code,
        fetched_at
    from source
    where offer_id is not null
      and ean is not null
)

select * from cleaned

with orders as (
    select * from {{ ref('stg_bol_orders') }}
),

offers as (
    select * from {{ ref('stg_bol_offers') }}
),

costs as (
    select * from {{ source('raw', 'bol_product_costs') }}
),

final as (
    select
        o.order_id,
        o.order_item_id,
        o.order_placed_at                                               as date,
        o.title                                                         as product_name,
        o.ean,
        o.quantity                                                      as units_sold,
        o.unit_price                                                    as selling_price,
        o.commission                                                    as bol_commission,

        -- original_price: prefer manual cost entry, fall back to current offer price
        coalesce(nullif(c.original_price, 0), off.unit_price)          as original_price,

        coalesce(c.cogs, 0)                                            as cogs,
        coalesce(c.estimated_ad_cost, 0)                               as estimated_ad_cost,

        -- calculated fields
        coalesce(c.cogs, 0) * o.quantity                               as cogs_total,
        round(o.unit_price / 1.21, 2)                                  as net_selling_price,

        case
            when coalesce(nullif(c.original_price, 0), off.unit_price) > 0
            then round(
                    (coalesce(nullif(c.original_price, 0), off.unit_price) - o.unit_price)
                    / coalesce(nullif(c.original_price, 0), off.unit_price) * 100
                , 1)
            else 0
        end                                                             as discount_used_pct,

        round(
            o.unit_price / 1.21
            - coalesce(c.cogs, 0)
            - coalesce(c.estimated_ad_cost, 0)
            - o.commission
        , 2)                                                            as net_margin_eur,

        case
            when o.unit_price > 0
            then round(
                    (o.unit_price / 1.21 - coalesce(c.cogs, 0) - coalesce(c.estimated_ad_cost, 0) - o.commission)
                    / (o.unit_price / 1.21) * 100
                , 1)
            else 0
        end                                                             as net_margin_pct,

        o.unit_price * o.quantity                                      as revenue,

        round(
            (o.unit_price / 1.21 - coalesce(c.cogs, 0) - coalesce(c.estimated_ad_cost, 0) - o.commission)
            * o.quantity
        , 2)                                                            as profit

    from orders o
    left join offers off on o.ean = off.ean
    left join costs c on o.ean = c.ean
)

select * from final

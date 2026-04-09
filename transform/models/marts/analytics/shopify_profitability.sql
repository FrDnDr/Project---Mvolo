with orders as (
    select * from {{ ref('stg_shopify_orders') }}
),

items as (
    select * from {{ ref('stg_shopify_order_items') }}
),

costs as (
    select * from {{ source('raw', 'shopify_product_costs') }}
),

line_item_base as (
    select
        o.order_id,
        l.line_item_id,
        o.created_at                                                        as date,
        o.landing_site,
        l.sku,
        coalesce(c.product_name, l.name)                                    as product_name,
        l.quantity                                                          as units_sold,
        coalesce(c.original_price, l.price)                                as original_price,
        -- effective per-unit price after discounts
        (l.price * l.quantity - l.total_discount) / nullif(l.quantity, 0)  as selling_price,
        coalesce(c.cogs, 0)                                                as cogs,
        coalesce(c.estimated_ad_cost, 0)                                   as estimated_ad_cost
    from items l
    join orders o on l.order_id = o.order_id
    left join costs c on l.sku = c.sku
),

final as (
    select
        order_id,
        line_item_id,
        date,
        product_name,
        sku,
        units_sold,
        selling_price,
        original_price,
        cogs,
        estimated_ad_cost,

        -- 12% affiliate fee only when order came via a referral landing site
        case
            when landing_site is not null
            then round(selling_price * 0.12, 2)
            else 0
        end                                                                 as shopify_affiliate_fee,

        cogs * units_sold                                                   as cogs_total,
        round(selling_price / 1.21, 2)                                     as net_selling_price,

        round(
            case when original_price > 0
                 then (original_price - selling_price) / original_price
                 else 0
            end
        , 3) * 100                                                          as discount_used_pct,

        round(
            (selling_price / 1.21)
            - cogs
            - case when landing_site is not null then selling_price * 0.12 else 0 end
        , 2)                                                                as net_margin_eur,

        selling_price * units_sold                                          as revenue,

        round(
            (
                (selling_price / 1.21)
                - cogs
                - case when landing_site is not null then selling_price * 0.12 else 0 end
            ) * units_sold
        , 2)                                                                as profit

    from line_item_base
)

select * from final

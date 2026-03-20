# 📂 transform/ — dbt Transformation Layer

> SQL-based transformations inside the warehouse using **dbt (data build tool)** — from raw API dumps to analytics-ready models.

---

## 🎯 Purpose

This is the **"T" in ELT**. After raw data is loaded into the warehouse, dbt transforms it through a layered model architecture:

```
Raw Data → Staging → Intermediate → Marts
              ↓           ↓            ↓
          (clean)    (join/merge)  (business-ready)
```

All transformations are **SQL**, **version-controlled**, and **testable**.

---

## 📁 Structure

```
transform/
├── dbt_project.yml           # Project config (name, version, model paths)
├── profiles.yml              # DB connection profiles (DuckDB / PostgreSQL)
├── packages.yml              # External dbt packages
│
├── models/
│   ├── staging/              # 1:1 with source tables — clean & rename
│   ├── intermediate/         # Cross-source joins & business logic
│   └── marts/                # Final analytical tables
│       ├── core/             # Dimension & fact tables (star schema)
│       └── analytics/        # Pre-aggregated analytics views
│
├── macros/                   # Reusable SQL functions
├── seeds/                    # Static CSV reference data
├── snapshots/                # Slowly changing dimension tracking
└── tests/                    # Custom data quality assertions
```

---

## 🔧 Implementation Plan

### 1. Project Configuration

**`dbt_project.yml`**
```yaml
name: "mvolo"
version: "1.0.0"
config-version: 2
profile: "mvolo"

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

models:
  mvolo:
    staging:
      +materialized: view         # Staging models are views (lightweight)
      +schema: staging
    intermediate:
      +materialized: view         # Intermediate are views too
      +schema: intermediate
    marts:
      core:
        +materialized: table      # Core marts are tables (performance)
        +schema: core
      analytics:
        +materialized: table      # Analytics marts are tables
        +schema: analytics
```

**`profiles.yml`**
```yaml
mvolo:
  target: dev   # Switch to 'prod' for PostgreSQL

  outputs:
    dev:
      type: duckdb
      path: "../data/mvolo.duckdb"
      threads: 4

    prod:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      port: "{{ env_var('POSTGRES_PORT') | int }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      schema: public
      threads: 4
```

**`packages.yml`**
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.0.0"
  - package: dbt-labs/codegen       # Helps generate staging models
    version: ">=0.12.0"
```

---

### 2. Staging Models — `models/staging/`

Staging models are **1:1 representations** of raw source tables. They:
- Rename columns to a consistent naming convention
- Cast data types
- Filter out test/invalid records
- Apply no business logic

**`stg_bol_orders.sql`**
```sql
-- Cleans and standardizes raw Bol.com order data

with source as (
    select * from {{ source('raw', 'bol_orders') }}
),

renamed as (
    select
        order_id,
        (order_data->>'orderPlacedDateTime')::timestamp    as order_placed_at,
        order_data->>'shipmentDetails'                     as shipment_details,
        (order_data->>'orderItems')                        as order_items_json,
        extracted_at,
        'bol' as channel
    from source
)

select * from renamed
```

**`stg_shopify_orders.sql`**
```sql
-- Cleans and standardizes raw Shopify order data

with source as (
    select * from {{ source('raw', 'shopify_orders') }}
),

renamed as (
    select
        order_id::varchar                                   as order_id,
        (order_data->>'created_at')::timestamp              as order_placed_at,
        (order_data->>'total_price')::decimal(10,2)         as total_price,
        order_data->>'currency'                             as currency,
        order_data->>'financial_status'                     as financial_status,
        order_data->>'fulfillment_status'                   as fulfillment_status,
        (order_data->>'line_items')                         as line_items_json,
        extracted_at,
        'shopify' as channel
    from source
)

select * from renamed
```

**`_staging.yml`** — Schema definitions & tests
```yaml
version: 2

sources:
  - name: raw
    schema: raw
    tables:
      - name: bol_orders
      - name: bol_offers
      - name: bol_shipments
      - name: bol_returns
      - name: shopify_orders
      - name: shopify_products
      - name: shopify_customers
      - name: shopify_inventory

models:
  - name: stg_bol_orders
    description: "Cleaned Bol.com order data"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null

  - name: stg_shopify_orders
    description: "Cleaned Shopify order data"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
```

---

### 3. Intermediate Models — `models/intermediate/`

Intermediate models **join data across sources** and apply business logic.

**`int_unified_orders.sql`**
```sql
-- Merges Bol.com and Shopify orders into a single unified view

with bol_orders as (
    select
        order_id,
        order_placed_at,
        channel,
        -- extract order total from nested JSON
        -- (implementation depends on Bol response structure)
        extracted_at
    from {{ ref('stg_bol_orders') }}
),

shopify_orders as (
    select
        order_id,
        order_placed_at,
        total_price,
        currency,
        channel,
        extracted_at
    from {{ ref('stg_shopify_orders') }}
),

unified as (
    select * from bol_orders
    union all
    select * from shopify_orders
)

select * from unified
```

**`int_order_profitability.sql`**
```sql
-- Calculates profit per order (revenue - cost - shipping)
-- Requires product cost data (from seeds or another source)

with orders as (
    select * from {{ ref('int_unified_orders') }}
),

product_costs as (
    select * from {{ ref('stg_bol_offers') }}  -- or a seeds CSV with cost data
),

profitability as (
    select
        o.order_id,
        o.channel,
        o.total_price as revenue,
        -- pc.cost_price,
        -- o.total_price - pc.cost_price as gross_profit,
        o.order_placed_at
    from orders o
    -- left join product_costs pc on o.product_id = pc.product_id
)

select * from profitability
```

---

### 4. Mart Models — `models/marts/`

#### Core (Star Schema)

**`dim_products.sql`** — Product dimension table
```sql
-- Unique product catalog from both Bol and Shopify

with bol_products as (
    select * from {{ ref('stg_bol_products') }}
),

shopify_products as (
    select * from {{ ref('stg_shopify_products') }}
),

final as (
    select
        -- generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id', 'channel']) }} as product_key,
        product_id,
        product_name,
        sku,
        channel,
        category,
        price
    from bol_products
    union all
    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'channel']) }} as product_key,
        product_id,
        product_name,
        sku,
        channel,
        category,
        price
    from shopify_products
)

select * from final
```

**`fct_orders.sql`** — Order fact table
```sql
-- Central fact table for all orders across channels

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'channel']) }} as order_key,
    order_id,
    order_placed_at,
    channel,
    total_price,
    currency,
    -- product_key (FK to dim_products)
    -- customer_key (FK to dim_customers)
    extracted_at
from {{ ref('int_unified_orders') }}
```

#### Analytics (Pre-Aggregated)

**`revenue_by_channel.sql`** — Used by Google Sheets & Metabase
```sql
-- Daily revenue breakdown by channel

select
    date_trunc('day', order_placed_at)::date as order_date,
    channel,
    count(*) as total_orders,
    sum(total_price) as total_revenue,
    avg(total_price) as avg_order_value
from {{ ref('fct_orders') }}
group by 1, 2
order by 1 desc, 2
```

**`product_performance.sql`**
```sql
-- Product-level sales performance

select
    p.product_name,
    p.sku,
    p.channel,
    count(distinct o.order_id) as total_orders,
    sum(o.total_price) as total_revenue,
    -- return rate calculated from returns data
    order by total_revenue desc
from {{ ref('fct_orders') }} o
join {{ ref('dim_products') }} p on o.product_key = p.product_key
group by 1, 2, 3
```

---

### 5. Macros — `macros/`

**`currency_conversion.sql`**
```sql
-- Convert between EUR and other currencies
{% macro convert_currency(amount, from_currency, to_currency='EUR') %}
    case
        when {{ from_currency }} = {{ to_currency }} then {{ amount }}
        -- Add conversion rates as needed
        else {{ amount }}
    end
{% endmacro %}
```

---

### 6. Seeds — `seeds/`

Static reference data loaded as CSV:
- `country_codes.csv` — Country code → name mapping
- `product_categories.csv` — Category taxonomy

```bash
dbt seed  # Loads CSV files into the warehouse as tables
```

---

### 7. Snapshots — `snapshots/`

Track slowly changing data (price changes, inventory fluctuations):

**`snap_product_prices.sql`**
```sql
{% snapshot snap_product_prices %}
{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['price'],
    )
}}
select * from {{ ref('stg_bol_products') }}
{% endsnapshot %}
```

---

## 🏃 Running dbt

```bash
cd transform

# Install packages
dbt deps

# Run all models
dbt run

# Run only staging
dbt run --select staging.*

# Run only marts
dbt run --select marts.*

# Test all models
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

---

## 📦 Dependencies

```
dbt-core>=1.7.0
dbt-duckdb>=1.7.0      # POC
dbt-postgres>=1.7.0     # Production
dbt-utils>=1.0.0
```

---

## ⚠️ Key Considerations

1. **Materialization strategy** — Staging/intermediate as `view` (lightweight), marts as `table` (fast queries from Metabase/Sheets)
2. **Testing** — Every primary key should have `unique` + `not_null` tests at minimum
3. **Documentation** — Use `_schema.yml` files to document every model and column
4. **Incremental models** — For large tables, consider `materialized: incremental` to avoid full rebuilds
5. **Run order** — dbt handles dependencies via `{{ ref() }}`  — staging runs first, then intermediate, then marts

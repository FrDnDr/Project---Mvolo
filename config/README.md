# 📂 config/ — Configuration

> Centralized pipeline configuration — settings, logging, source definitions, and secret templates.

---

## 🎯 Purpose

All pipeline behavior is controlled from this directory:
- **What** to extract and how often
- **How** to log pipeline activity
- **Where** data sources are and how to connect

---

## 📁 Files

| File | Purpose |
|------|---------|
| `settings.yaml` | Pipeline settings — schedule, batch sizes, feature flags |
| `logging.yaml` | Python logging configuration |
| `sources.yaml` | Data source definitions — endpoints, schemas, extraction rules |
| `google_service_account.json` | ⚠️ Google Sheets credentials (gitignored, never committed) |

---

## 🔧 Implementation Plan

### 1. `settings.yaml`

```yaml
# Pipeline-level settings

pipeline:
  name: "mvolo"
  version: "1.0.0"
  environment: "dev"           # "dev" = DuckDB, "prod" = PostgreSQL

extraction:
  default_batch_size: 100      # Records per API page
  max_retries: 3               # Retry failed API calls
  retry_backoff_seconds: 5     # Wait between retries (exponential)
  full_load_on_first_run: true # Extract all history on first run

loading:
  strategy: "upsert"           # "upsert" or "full_load" or "append"
  warehouse: "duckdb"          # "duckdb" or "postgres"

scheduling:
  daily_pipeline_time: "06:00" # When to run the daily ETL
  weekly_export_day: "monday"  # Day for Google Sheets export
  weekly_export_time: "07:00"  # Time for weekly export
  timezone: "Europe/Amsterdam"

export:
  google_sheets:
    enabled: true
    overwrite_on_export: true   # Clear sheet before writing
    include_reporting_period: true  # Add "Week of..." header

features:
  enable_snapshots: false       # SCD Type 2 tracking (enable later)
  enable_profitability: false   # Cost-based calculations (needs cost data)
```

### 2. `logging.yaml`

```yaml
# Python logging configuration

version: 1
disable_existing_loggers: false

formatters:
  standard:
    format: "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt: "%Y-%m-%d %H:%M:%S"
  detailed:
    format: "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"

handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: standard
    stream: ext://sys.stdout

  file:
    class: logging.FileHandler
    level: DEBUG
    formatter: detailed
    filename: logs/pipeline.log
    mode: a

filters: {}

loggers:
  extract:
    level: DEBUG
    handlers: [console, file]
  load:
    level: DEBUG
    handlers: [console, file]
  export:
    level: DEBUG
    handlers: [console, file]

root:
  level: INFO
  handlers: [console, file]
```

### 3. `sources.yaml`

```yaml
# Data source definitions

sources:
  bol:
    name: "Bol.com"
    type: "rest_api"
    auth_method: "oauth2_client_credentials"
    token_url: "https://login.bol.com/token"
    base_url: "https://api.bol.com/retailer/"
    api_version: null
    rate_limit:
      requests_per_second: 25
    endpoints:
      orders:
        path: "orders"
        method: "GET"
        pagination: "offset"
        page_size: 50
        date_filter_param: "fulfilment-method"
        raw_table: "raw.bol_orders"
      offers:
        path: "offers"
        method: "GET"
        pagination: "offset"
        page_size: 50
        raw_table: "raw.bol_offers"
      shipments:
        path: "shipments"
        method: "GET"
        pagination: "offset"
        page_size: 50
        raw_table: "raw.bol_shipments"
      returns:
        path: "returns"
        method: "GET"
        pagination: "offset"
        page_size: 50
        raw_table: "raw.bol_returns"

  shopify:
    name: "Shopify"
    type: "rest_api"
    auth_method: "access_token"
    auth_header: "X-Shopify-Access-Token"
    base_url: "https://{store}.myshopify.com/admin/api/{version}/"
    api_version: "2024-01"
    rate_limit:
      requests_per_second: 2
    endpoints:
      orders:
        path: "orders.json"
        method: "GET"
        pagination: "cursor"
        page_size: 250
        date_filter_params:
          - "created_at_min"
          - "created_at_max"
        raw_table: "raw.shopify_orders"
      products:
        path: "products.json"
        method: "GET"
        pagination: "cursor"
        page_size: 250
        raw_table: "raw.shopify_products"
      customers:
        path: "customers.json"
        method: "GET"
        pagination: "cursor"
        page_size: 250
        raw_table: "raw.shopify_customers"
      inventory:
        path: "inventory_levels.json"
        method: "GET"
        pagination: "cursor"
        page_size: 250
        raw_table: "raw.shopify_inventory"
```

---

## ⚠️ Key Considerations

1. **`google_service_account.json` is gitignored** — Never commit this file. Share it securely with team members
2. **Environment-based settings** — `settings.yaml` uses `environment: "dev"` to toggle between DuckDB and PostgreSQL
3. **Feature flags** — Gradually enable features (snapshots, profitability) as the pipeline matures
4. **Log rotation** — For production, add log rotation to prevent disk fill-up

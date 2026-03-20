# 📂 scripts/ — Utility Scripts

> Entry points and helpers for running the pipeline, setting up the warehouse, and validating connections.

---

## 🎯 Purpose

These scripts are the **command-line interface** to the pipeline. They're what N8N calls, what you run during development, and what the `Makefile` wraps.

---

## 📁 Files

| File | Purpose |
|------|---------|
| `run_pipeline.py` | Main entry point — orchestrates the full or partial pipeline |
| `setup_warehouse.py` | Creates schemas and raw tables in DuckDB or PostgreSQL |
| `seed_sample_data.py` | Generates fake/anonymized data for testing and demos |
| `validate_connections.py` | Tests all API keys and database connections |

---

## 🔧 Implementation Plan

### 1. `run_pipeline.py` — Main Entry Point

```python
"""
Main pipeline script — called by N8N or run manually.

Usage:
    python scripts/run_pipeline.py                          # Full pipeline
    python scripts/run_pipeline.py --mode extract           # Extraction only
    python scripts/run_pipeline.py --mode extract --source bol  # Bol only
    python scripts/run_pipeline.py --mode load              # Loading only
    python scripts/run_pipeline.py --mode export-sheets     # Google Sheets export
    python scripts/run_pipeline.py --mode export-sheets --start-date 2026-03-10 --end-date 2026-03-16
"""

import argparse
import logging
from datetime import datetime, timedelta

# Pipeline steps:
# 1. Parse CLI arguments (mode, source, date range)
# 2. Load environment variables from .env
# 3. Initialize logger
# 4. Based on mode:
#    - "extract": Run extractors (bol, shopify, or both)
#    - "load": Run loader (DuckDB or Postgres based on config)
#    - "transform": Shell out to `dbt run && dbt test`
#    - "export-sheets": Run Google Sheets export for date range
#    - (default): Run all steps in sequence
# 5. Log summary: records processed, duration, errors

def calculate_last_week_range():
    """Returns (last_monday, last_sunday) for the weekly report."""
    today = datetime.now()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday.date(), last_sunday.date()
```

### 2. `setup_warehouse.py` — Database Initialization

```python
"""
Creates the warehouse schema and raw tables.
Run once during initial setup, or to reset the warehouse.

Usage:
    python scripts/setup_warehouse.py                # Uses default DB from .env
    python scripts/setup_warehouse.py --db duckdb    # Force DuckDB
    python scripts/setup_warehouse.py --db postgres  # Force PostgreSQL
    python scripts/setup_warehouse.py --reset        # Drop and recreate all tables
"""

# Steps:
# 1. Read DB config from .env
# 2. Connect to database
# 3. Create schemas: raw, staging, intermediate, core, analytics, snapshots
# 4. Create raw tables (bol_orders, shopify_orders, etc.)
# 5. Log success
```

### 3. `seed_sample_data.py` — Test Data Generator

```python
"""
Generates fake/anonymized data and loads it into the warehouse.
Useful for testing the pipeline without real API credentials.

Usage:
    python scripts/seed_sample_data.py               # Generate default dataset
    python scripts/seed_sample_data.py --records 100  # Generate 100 records per table
"""

# Uses: Faker library to generate realistic but fake data
# Generates:
#   - Fake Bol.com orders (order_id, date, product, price, status)
#   - Fake Shopify orders (matching schema)
#   - Fake products, customers, shipments
# Saves to:
#   - data/sample/ (JSON files)
#   - Optionally loads directly into warehouse
```

### 4. `validate_connections.py` — Connection Tester

```python
"""
Tests all external connections before running the pipeline.
Run this first to ensure everything is configured correctly.

Usage:
    python scripts/validate_connections.py

Output:
    ✅ Bol.com API: Connected (token valid)
    ✅ Shopify API: Connected (store: your-store.myshopify.com)
    ✅ PostgreSQL: Connected (database: mvolo)
    ✅ DuckDB: Connected (path: ./data/mvolo.duckdb)
    ✅ Google Sheets: Connected (spreadsheet: Weekly Report)
    ❌ N8N: Not reachable (http://localhost:5678) — is Docker running?
"""

# Tests:
# 1. Bol.com API — Attempt OAuth2 token request
# 2. Shopify API — GET /shop.json to verify access
# 3. PostgreSQL — psycopg2.connect() test
# 4. DuckDB — duckdb.connect() test
# 5. Google Sheets — gspread.authorize() + open spreadsheet
# 6. N8N — HTTP GET http://localhost:5678/healthz
```

---

## 📦 Dependencies

```
# All scripts
python-dotenv>=1.0.0
argparse (stdlib)
logging (stdlib)

# Seed data generation
faker>=22.0.0
```

---

## ⚠️ Key Considerations

1. **All scripts use `.env`** — Never hardcode credentials; always load from environment
2. **Logging** — Every script logs to stdout and optionally to `logs/` directory
3. **Exit codes** — Return 0 on success, 1 on failure (important for N8N to detect errors)
4. **Idempotent** — Running any script multiple times should be safe

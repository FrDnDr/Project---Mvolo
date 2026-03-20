# 📂 load/ — Loading Layer

> Loads extracted data into the data warehouse — **DuckDB** (POC) or **PostgreSQL** (production).

---

## 🎯 Purpose

This module handles the **"L" in ELT** — taking the structured dictionaries from the `extract/` layer and inserting them into the warehouse. It manages schema creation, upserts (insert or update), and the dual-database strategy.

---

## 📁 Files

| File | Purpose |
|------|---------|
| `loader.py` | Abstract base loader — defines the loading interface |
| `duckdb_loader.py` | DuckDB-specific implementation (POC phase) |
| `postgres_loader.py` | PostgreSQL-specific implementation (production) |

---

## 🔧 Implementation Plan

### 1. `loader.py` — Base Loader Interface

```python
# Abstract interface that both DuckDB and Postgres loaders implement

class BaseLoader:
    def __init__(self, connection_config: dict):
        self.config = connection_config
        self.connection = None

    def connect(self):
        """Establish database connection."""
        raise NotImplementedError

    def create_raw_tables(self):
        """Create raw/staging tables if they don't exist."""
        raise NotImplementedError

    def load_records(self, table_name: str, records: list[dict]):
        """Insert or upsert records into the specified table."""
        raise NotImplementedError

    def close(self):
        """Close the database connection."""
        raise NotImplementedError
```

### 2. `duckdb_loader.py` — DuckDB Loader (POC Phase)

```python
# DuckDB is used for the proof-of-concept phase
# - No server setup required (embedded database, single file)
# - Excellent for analytical workloads
# - Easy to migrate to PostgreSQL later

import duckdb

class DuckDBLoader(BaseLoader):
    def connect(self):
        """Connect to local DuckDB file."""
        self.connection = duckdb.connect(self.config["DUCKDB_PATH"])

    def create_raw_tables(self):
        """Create raw tables using DuckDB SQL.
        Tables:
          - raw_bol_orders
          - raw_bol_offers
          - raw_bol_shipments
          - raw_bol_returns
          - raw_shopify_orders
          - raw_shopify_products
          - raw_shopify_customers
          - raw_shopify_inventory
        """
        pass

    def load_records(self, table_name: str, records: list[dict]):
        """Use DuckDB's native dict/JSON ingestion.
        Strategy:
          1. Convert list[dict] → pandas DataFrame
          2. Use INSERT OR REPLACE for upserts (based on primary key)
          3. Log: records inserted, records updated, duration
        """
        pass
```

**Why DuckDB first?**
- Zero infrastructure — just a file on disk
- Column-oriented — fast for analytical queries
- Native Python integration — `duckdb.connect()` and direct DataFrame ingestion
- SQL-compatible with PostgreSQL — migration is mostly copy-paste

### 3. `postgres_loader.py` — PostgreSQL Loader (Production)

```python
# PostgreSQL is used for the production deployment
# - Accessed via Docker container
# - Supports concurrent connections (N8N, Metabase, dbt all connect)
# - Robust data types, indexing, and constraints

import psycopg2

class PostgresLoader(BaseLoader):
    def connect(self):
        """Connect to PostgreSQL using config from .env."""
        self.connection = psycopg2.connect(
            host=self.config["POSTGRES_HOST"],
            port=self.config["POSTGRES_PORT"],
            dbname=self.config["POSTGRES_DB"],
            user=self.config["POSTGRES_USER"],
            password=self.config["POSTGRES_PASSWORD"],
        )

    def create_raw_tables(self):
        """Create raw schema and tables with proper data types.
        Schema: raw
        Tables:
          - raw.bol_orders
          - raw.bol_offers
          - raw.bol_shipments
          - raw.bol_returns
          - raw.shopify_orders
          - raw.shopify_products
          - raw.shopify_customers
          - raw.shopify_inventory
        """
        pass

    def load_records(self, table_name: str, records: list[dict]):
        """Use PostgreSQL COPY or INSERT ... ON CONFLICT for upserts.
        Strategy:
          1. COPY for bulk initial loads (fastest)
          2. INSERT ... ON CONFLICT DO UPDATE for incremental loads
          3. Wrap in transaction — rollback on failure
        """
        pass
```

---

## 🗄️ Database Schema Strategy

### Raw Tables (Landing Zone)

Raw tables store data exactly as received from APIs — minimal transformation:

```sql
-- Example: raw.bol_orders
CREATE TABLE IF NOT EXISTS raw.bol_orders (
    order_id        VARCHAR PRIMARY KEY,
    order_data      JSONB,             -- Full API response stored as JSON
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source          VARCHAR DEFAULT 'bol'
);

-- Example: raw.shopify_orders
CREATE TABLE IF NOT EXISTS raw.shopify_orders (
    order_id        BIGINT PRIMARY KEY,
    order_data      JSONB,
    extracted_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source          VARCHAR DEFAULT 'shopify'
);
```

**Why JSONB?**
- Stores the full API response — no data loss
- dbt can extract/transform specific fields later
- Flexible — if the API adds new fields, no schema change needed
- Both DuckDB and PostgreSQL support JSON querying

### Alternative: Flattened Raw Tables

If you prefer structured tables over JSONB:

```sql
CREATE TABLE IF NOT EXISTS raw.bol_orders (
    order_id            VARCHAR PRIMARY KEY,
    date_created        TIMESTAMP,
    customer_name       VARCHAR,
    total_price         DECIMAL(10, 2),
    currency            VARCHAR(3),
    status              VARCHAR,
    -- ... all relevant fields
    extracted_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

> **Recommendation**: Start with JSONB for flexibility during POC, then flatten once schemas stabilize.

---

## 🔄 Loading Strategies

| Strategy | When to Use | Method |
|----------|-------------|--------|
| **Full Load** | Initial setup, small tables | Truncate + insert all records |
| **Incremental Load** | Daily/weekly runs | Upsert only new/updated records (by `updated_at`) |
| **Append-Only** | Event/log data | Insert new records, never update |

### Incremental Load Flow

```
1. Query warehouse: SELECT MAX(extracted_at) FROM raw.bol_orders
2. Pass that timestamp to extractor: extract_orders(start_date=last_extracted)
3. Upsert returned records into raw table
4. Log: X new records, Y updated records
```

---

## 📦 Dependencies

```
# POC (DuckDB)
duckdb>=0.10.0
pandas>=2.0.0

# Production (PostgreSQL)
psycopg2-binary>=2.9.9
```

---

## ⚠️ Key Considerations

1. **Transaction safety** — Always wrap loads in transactions; rollback on failure
2. **Idempotency** — Running the loader twice with the same data should produce the same result (upsert, not duplicate)
3. **Type coercion** — API responses return strings; cast to proper types (dates, decimals) during load
4. **Logging** — Log every load operation: table name, record count, duration, success/failure

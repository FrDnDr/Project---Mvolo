# 🔒 Security, Risk Mitigation & Data Leakage Prevention

> Comprehensive security documentation for the Mvolo ETL pipeline — covering identified risks, implemented solutions, and data leakage prevention strategies.

---

## Table of Contents

1. [Overview](#-overview)
2. [Risk Register](#-risk-register)
3. [Solutions & Mitigations](#-solutions--mitigations)
   - [Credential & Secret Management](#1--credential--secret-management)
   - [Customer PII & GDPR Compliance](#2-️-customer-pii--gdpr-compliance)
   - [Data Integrity & Pipeline Resilience](#3--data-integrity--pipeline-resilience)
   - [API Security & Rate Limiting](#4--api-security--rate-limiting)
   - [Schema Drift Protection](#5--schema-drift-protection)
   - [Data Freshness & Completeness](#6--data-freshness--completeness)
   - [Google Sheets Export Safety](#7--google-sheets-export-safety)
   - [Docker & Infrastructure Hardening](#8--docker--infrastructure-hardening)
   - [Audit Trail & Observability](#9--audit-trail--observability)
   - [Pipeline Redundancy](#10--pipeline-redundancy)
4. [Data Leakage Prevention](#-data-leakage-prevention)
5. [Incident Response Plan](#-incident-response-plan)
6. [Security Checklist](#-security-checklist)

---

## 📋 Overview

Mvolo processes **sensitive e-commerce data** from Bol.com and Shopify, including:

| Data Type | Examples | Sensitivity |
|-----------|----------|-------------|
| **Customer PII** | Names, emails, addresses, phone numbers | 🔴 High |
| **Financial data** | Order totals, revenue, costs, margins | 🔴 High |
| **Business intelligence** | Sales trends, product performance, return rates | 🟡 Medium |
| **API credentials** | OAuth tokens, API keys, service accounts | 🔴 Critical |
| **Infrastructure secrets** | Database passwords, connection strings | 🔴 Critical |

This document identifies all known risks and provides **actionable solutions** for each.

---

## 🗂️ Risk Register

| ID | Risk | Category | Severity | Status |
|----|------|----------|----------|--------|
| R-01 | API credentials leaked via Git or logs | 🔐 Security | 🔴 Critical | ⬜ To implement |
| R-02 | Customer PII exposed to unauthorized access | 🛡️ Privacy | 🔴 Critical | ⬜ To implement |
| R-03 | Data loss from pipeline failures | 💾 Integrity | 🟠 High | ⬜ To implement |
| R-04 | API rate limiting causes data gaps | 🔌 Operational | 🟡 Medium | ⬜ To implement |
| R-05 | Schema drift corrupts data silently | 🔌 Operational | 🟡 Medium | ⬜ To implement |
| R-06 | Stale or incomplete data in reports | 💾 Integrity | 🟡 Medium | ⬜ To implement |
| R-07 | Google Sheets export overwrites stakeholder edits | 💾 Integrity | 🟡 Medium | ⬜ To implement |
| R-08 | Docker containers compromised | 🔐 Security | 🟠 High | ⬜ To implement |
| R-09 | No audit trail for pipeline runs | 📋 Compliance | 🟡 Medium | ⬜ To implement |
| R-10 | N8N single point of failure | 🔌 Operational | 🟡 Medium | ⬜ To implement |

> Update the **Status** column as mitigations are implemented: ⬜ To implement → 🟡 In progress → ✅ Done

---

## 🛡️ Solutions & Mitigations

### 1. 🔐 Credential & Secret Management

**Risk:** API keys, database passwords, and service account tokens could be accidentally committed to Git, printed in logs, or exposed through Docker environment variables.

#### Solution: Multi-Layer Secret Protection

**A. Pre-commit Secret Scanning**

Install `gitleaks` to automatically scan every commit for leaked secrets:

```bash
# Install gitleaks (Windows — via Scoop or Chocolatey)
scoop install gitleaks
# OR
choco install gitleaks

# Create a pre-commit hook
# File: .git/hooks/pre-commit
#!/bin/sh
gitleaks protect --staged --verbose
```

Alternatively, use a `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.18.0
    hooks:
      - id: gitleaks
```

**B. Environment Variable Hygiene**

```python
# ❌ BAD — Never do this in extract/utils.py or any logger
logger.info(f"Connecting with token: {api_token}")
logger.debug(f"Auth header: {headers}")

# ✅ GOOD — Mask secrets in logs
logger.info("Connecting to Bol.com API with token: ****" + api_token[-4:])
logger.debug("Auth header: [REDACTED]")
```

**C. Secret Rotation Schedule**

| Secret | Rotation Frequency | How to Rotate |
|--------|--------------------|---------------|
| Bol.com API keys | Every 90 days | Bol Partner Portal → API Settings |
| Shopify access token | Every 90 days | Shopify Admin → Apps → Regenerate |
| PostgreSQL password | Every 90 days | `ALTER USER mvolo_user PASSWORD 'new_pass';` |
| Google Service Account | Every 180 days | Google Cloud Console → IAM → Service Accounts |

**D. Production Secret Management**

For production deployment, migrate from `.env` files to Docker Secrets:

```yaml
# docker-compose.yml (production)
services:
  postgres:
    secrets:
      - db_password
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/db_password

secrets:
  db_password:
    file: ./secrets/db_password.txt  # Not committed to Git
```

---

### 2. 🛡️ Customer PII & GDPR Compliance

**Risk:** Customer names, emails, addresses, and phone numbers flow through every pipeline layer. As Bol.com operates in the EU, GDPR compliance is mandatory.

#### Solution: PII Classification & Protection Framework

**A. Data Classification Matrix**

| Field | Source | PII Level | Treatment |
|-------|--------|-----------|-----------|
| `customer_name` | Shopify, Bol | 🔴 Direct PII | Hash in staging, never export to Sheets |
| `customer_email` | Shopify, Bol | 🔴 Direct PII | Hash in staging, never export to Sheets |
| `shipping_address` | Shopify, Bol | 🔴 Direct PII | Keep only city/country for analytics |
| `phone_number` | Shopify, Bol | 🔴 Direct PII | Drop entirely — not needed for analytics |
| `ip_address` | Shopify | 🔴 Direct PII | Drop entirely |
| `order_id` | Shopify, Bol | 🟡 Indirect PII | Keep — needed for data integrity |
| `product_name` | Shopify, Bol | 🟢 Non-PII | Keep as-is |
| `order_total` | Shopify, Bol | 🟢 Non-PII | Keep as-is |
| `order_date` | Shopify, Bol | 🟢 Non-PII | Keep as-is |

**B. PII Hashing in dbt Staging Models**

Apply SHA-256 hashing to PII fields at the earliest stage:

```sql
-- transform/models/staging/stg_shopify_customers.sql

WITH source AS (
    SELECT * FROM {{ source('raw', 'shopify_customers') }}
),

cleaned AS (
    SELECT
        customer_id,

        -- 🔒 Hash PII fields
        {{ dbt_utils.generate_surrogate_key(['customer_email']) }} AS customer_email_hash,
        {{ dbt_utils.generate_surrogate_key(['customer_name']) }}  AS customer_name_hash,

        -- 🔒 Keep only geographic aggregation level
        city,
        country_code,
        -- ❌ Drop: full_address, phone_number, ip_address

        created_at,
        updated_at
    FROM source
)

SELECT * FROM cleaned
```

**C. Metabase Access Controls**

```
Metabase Roles:
├── Admin           → Full access (you only)
├── Analyst         → Access to marts/ (aggregated data, no PII)
└── Stakeholder     → Access to specific dashboards only (read-only)
```

Configure in Metabase:
1. Go to **Admin → Permissions**
2. Block access to `raw` and `staging` schemas for non-admin roles
3. Enable **column-level permissions** to hide hashed PII columns

**D. GDPR Compliance Checklist**

| Requirement | Implementation |
|-------------|----------------|
| **Lawful basis** | Legitimate interest for business analytics |
| **Data minimization** | Only extract fields needed for analytics |
| **Purpose limitation** | Data used only for dropshipping analytics |
| **Storage limitation** | Auto-delete raw data after 90 days |
| **Right to erasure** | Script to delete customer data by ID on request |
| **Data processing record** | Maintained in `docs/data_dictionary.md` |
| **Encryption at rest** | PostgreSQL TDE / encrypted Docker volumes |
| **Encryption in transit** | HTTPS-only API calls, SSL database connections |

**E. Data Retention Policy**

```python
# scripts/data_retention.py — Run weekly
import os
from datetime import datetime, timedelta

RAW_DATA_DIR = "data/raw/"
RETENTION_DAYS = 90

cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)

for file in os.listdir(RAW_DATA_DIR):
    filepath = os.path.join(RAW_DATA_DIR, file)
    modified = datetime.fromtimestamp(os.path.getmtime(filepath))
    if modified < cutoff:
        os.remove(filepath)
        print(f"🗑️ Deleted expired file: {file} (modified: {modified})")
```

---

### 3. 💾 Data Integrity & Pipeline Resilience

**Risk:** Pipeline failures mid-execution can result in partial data, duplicates, or corrupted warehouse state.

#### Solution: Idempotent, Transactional Pipeline

**A. Idempotent Extraction**

```python
# extract/base.py — BaseExtractor class

class BaseExtractor:
    def extract(self, start_date, end_date):
        """
        Idempotent extraction — safe to re-run.
        Uses date range as the natural key.
        """
        output_file = f"data/raw/{self.source}_{start_date}_{end_date}.json"

        # Skip if already extracted today
        if os.path.exists(output_file):
            modified = datetime.fromtimestamp(os.path.getmtime(output_file))
            if modified.date() == datetime.today().date():
                logger.info(f"⏭️ Skipping — already extracted today: {output_file}")
                return output_file

        data = self._fetch_from_api(start_date, end_date)
        self._save_with_checksum(data, output_file)
        return output_file

    def _save_with_checksum(self, data, filepath):
        """Save data with a SHA-256 checksum for integrity validation."""
        import hashlib, json

        content = json.dumps(data, sort_keys=True)
        checksum = hashlib.sha256(content.encode()).hexdigest()

        with open(filepath, 'w') as f:
            json.dump({"checksum": checksum, "data": data}, f)

        logger.info(f"✅ Saved {filepath} (checksum: {checksum[:12]}...)")
```

**B. Transactional Loading**

```python
# load/postgres_loader.py

import psycopg2

class PostgresLoader:
    def load(self, data, table_name):
        """Atomic load — all-or-nothing via transaction."""
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn:
                with conn.cursor() as cur:
                    # Delete existing records for this batch (upsert pattern)
                    cur.execute(f"""
                        DELETE FROM raw.{table_name}
                        WHERE extracted_date = %s
                    """, (data['extraction_date'],))

                    # Insert new records
                    for row in data['records']:
                        cur.execute(f"""
                            INSERT INTO raw.{table_name}
                            (order_id, order_data, extracted_at, source)
                            VALUES (%s, %s, NOW(), %s)
                        """, (row['id'], json.dumps(row), data['source']))

            logger.info(f"✅ Loaded {len(data['records'])} rows into {table_name}")

        except Exception as e:
            logger.error(f"❌ Load failed — transaction rolled back: {e}")
            raise
        finally:
            conn.close()
```

**C. Pipeline Checkpointing**

```python
# scripts/run_pipeline.py — Track pipeline state

import json
from datetime import datetime

CHECKPOINT_FILE = "data/.pipeline_checkpoint.json"

def save_checkpoint(stage, status, details=None):
    checkpoint = {
        "timestamp": datetime.now().isoformat(),
        "stage": stage,
        "status": status,
        "details": details or {}
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def get_last_checkpoint():
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

# Usage in pipeline
def run_pipeline():
    last = get_last_checkpoint()

    # Resume from failure point
    if last and last['status'] == 'failed':
        print(f"⚠️ Resuming from failed stage: {last['stage']}")
        start_stage = last['stage']
    else:
        start_stage = 'extract'

    stages = ['extract', 'load', 'transform', 'export']
    for stage in stages:
        if stages.index(stage) < stages.index(start_stage):
            continue
        try:
            save_checkpoint(stage, 'running')
            run_stage(stage)
            save_checkpoint(stage, 'completed')
        except Exception as e:
            save_checkpoint(stage, 'failed', {'error': str(e)})
            raise
```

**D. Automated Backup Strategy**

```bash
# scripts/backup_warehouse.sh — Run daily after pipeline

#!/bin/bash
BACKUP_DIR="./backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=30

mkdir -p $BACKUP_DIR

# PostgreSQL backup
pg_dump -h localhost -U mvolo_user -d mvolo \
  --format=custom \
  --file="$BACKUP_DIR/mvolo_$TIMESTAMP.dump"

echo "✅ Backup created: mvolo_$TIMESTAMP.dump"

# Clean old backups
find $BACKUP_DIR -name "*.dump" -mtime +$RETENTION_DAYS -delete
echo "🧹 Cleaned backups older than $RETENTION_DAYS days"
```

---

### 4. 🔌 API Security & Rate Limiting

**Risk:** Exceeding API rate limits leads to temporary blocks or permanent revocation. Misconfigured HTTP clients may leak data in transit.

#### Solution: Defensive API Client

```python
# extract/base.py — Rate-limiting with exponential backoff

import time
import random
import requests
from functools import wraps

class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, max_requests_per_second=2):
        self.max_rps = max_requests_per_second
        self.last_request_time = 0

    def wait(self):
        elapsed = time.time() - self.last_request_time
        min_interval = 1.0 / self.max_rps
        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()


def retry_with_backoff(max_retries=5, base_delay=1.0):
    """Decorator for exponential backoff with jitter."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        # Rate limited — back off exponentially
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        logger.warning(
                            f"⚠️ Rate limited (429). Retry {attempt+1}/{max_retries} "
                            f"in {delay:.1f}s"
                        )
                        time.sleep(delay)
                    elif e.response.status_code >= 500:
                        # Server error — retry
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"⚠️ Server error ({e.response.status_code}). Retrying...")
                        time.sleep(delay)
                    else:
                        raise  # Client error — don't retry
            raise Exception(f"❌ Max retries ({max_retries}) exceeded")
        return wrapper
    return decorator


class BaseExtractor:
    def __init__(self):
        # Bol.com: ~25 req/min, Shopify: 2 req/sec
        self.rate_limiter = RateLimiter(max_requests_per_second=2)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mvolo-ETL/1.0'
        })

    @retry_with_backoff(max_retries=5)
    def _make_request(self, url, params=None):
        self.rate_limiter.wait()
        response = self.session.get(url, params=params, timeout=30)

        # ✅ Log request (without sensitive headers)
        logger.info(f"📡 {response.status_code} {url} ({response.elapsed.total_seconds():.2f}s)")

        response.raise_for_status()
        return response.json()
```

**API Version Pinning:**

```yaml
# config/sources.yaml
bol:
  base_url: "https://api.bol.com/retailer"
  api_version: "v10"       # ← Pin the API version
  rate_limit_rps: 0.4       # 25 req/min = ~0.4 req/sec

shopify:
  base_url: "https://{store}.myshopify.com/admin/api"
  api_version: "2025-01"    # ← Pin the API version
  rate_limit_rps: 2
```

---

### 5. 🔌 Schema Drift Protection

**Risk:** API response structures change without warning. Hardcoded column names in extractors or dbt models silently break.

#### Solution: Schema Validation Layer

```python
# extract/validators.py — Validate API responses before loading

from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import datetime

class BolOrderItem(BaseModel):
    order_item_id: str = Field(..., alias="orderItemId")
    ean: str
    quantity: int
    unit_price: float = Field(..., alias="unitPrice")
    commission: float

class BolOrder(BaseModel):
    order_id: str = Field(..., alias="orderId")
    date_time_ordered: datetime = Field(..., alias="dateTimeOrdered")
    order_items: List[BolOrderItem] = Field(..., alias="orderItems")

    @validator('order_items')
    def must_have_items(cls, v):
        if len(v) == 0:
            raise ValueError("Order must have at least one item")
        return v

class SchemaValidator:
    """Validates API responses against expected schemas."""

    @staticmethod
    def validate_bol_orders(raw_data: list) -> tuple:
        valid, invalid = [], []
        for record in raw_data:
            try:
                validated = BolOrder(**record)
                valid.append(validated.dict())
            except Exception as e:
                invalid.append({"record": record, "error": str(e)})

        if invalid:
            logger.warning(
                f"⚠️ Schema validation: {len(invalid)} invalid records "
                f"out of {len(raw_data)} total"
            )
            # Save invalid records for investigation
            with open("data/raw/invalid_records.json", 'a') as f:
                json.dump(invalid, f, indent=2, default=str)

        return valid, invalid
```

**dbt Schema Tests (`transform/models/staging/_staging.yml`):**

```yaml
version: 2

models:
  - name: stg_bol_orders
    description: "Cleaned Bol.com orders"
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: order_placed_at
        tests:
          - not_null
      - name: total_price
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100000  # Flag likely data errors

  - name: stg_shopify_orders
    description: "Cleaned Shopify orders"
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: total_price
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
```

---

### 6. 📊 Data Freshness & Completeness

**Risk:** Dashboards or reports silently show stale or partial data, leading to bad business decisions.

#### Solution: Automated Freshness Monitoring

**dbt Source Freshness (`transform/models/staging/_sources.yml`):**

```yaml
version: 2

sources:
  - name: raw
    description: "Raw data loaded from APIs"
    freshness:
      warn_after: { count: 24, period: hour }
      error_after: { count: 48, period: hour }
    loaded_at_field: extracted_at

    tables:
      - name: bol_orders
      - name: bol_products
      - name: shopify_orders
      - name: shopify_products
```

Run with: `dbt source freshness`

**Row Count Validation:**

```python
# scripts/validate_pipeline.py

def validate_extraction(source, data, api_total_count):
    """Ensure we extracted the expected number of records."""
    extracted_count = len(data)
    completeness = (extracted_count / api_total_count) * 100

    if completeness < 95:
        logger.error(
            f"❌ INCOMPLETE: {source} — extracted {extracted_count}/{api_total_count} "
            f"({completeness:.1f}%)"
        )
        raise DataCompletenessError(f"{source} extraction below 95% threshold")

    logger.info(
        f"✅ {source}: {extracted_count}/{api_total_count} records ({completeness:.1f}%)"
    )
```

**Pipeline Metadata Table:**

```sql
-- docker/postgres/init.sql
CREATE TABLE IF NOT EXISTS pipeline.run_log (
    run_id          SERIAL PRIMARY KEY,
    run_started_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    run_finished_at TIMESTAMP,
    stage           VARCHAR(50) NOT NULL,     -- 'extract', 'load', 'transform', 'export'
    source          VARCHAR(50),              -- 'bol', 'shopify'
    status          VARCHAR(20) NOT NULL,     -- 'running', 'success', 'failed'
    rows_processed  INTEGER,
    error_message   TEXT,
    checksum        VARCHAR(64)               -- SHA-256 of extracted data
);
```

---

### 7. 📤 Google Sheets Export Safety

**Risk:** Automated exports overwrite manual edits. Sensitive data could be exposed via sheet sharing settings.

#### Solution: Safe Export Patterns

```python
# export/sheets_exporter.py

class SafeSheetsExporter:
    def export_weekly_report(self, data, spreadsheet_id):
        """
        Safe export — creates new dated tabs, never overwrites existing ones.
        """
        from datetime import datetime
        import gspread

        gc = gspread.service_account(filename='config/google_service_account.json')
        sheet = gc.open_by_key(spreadsheet_id)

        # ✅ Create unique tab per week (e.g., "Week_2026-12")
        week_number = datetime.now().strftime("%Y-%W")
        tab_name = f"Week_{week_number}"

        # Check if tab already exists
        existing_tabs = [ws.title for ws in sheet.worksheets()]
        if tab_name in existing_tabs:
            logger.warning(f"⚠️ Tab '{tab_name}' already exists — skipping export")
            return

        # Create new tab and populate
        worksheet = sheet.add_worksheet(title=tab_name, rows=len(data)+1, cols=len(data[0]))

        # ✅ Add metadata header
        worksheet.update('A1', f"Generated by Mvolo ETL — {datetime.now().isoformat()}")
        worksheet.update('A2', "⚠️ This tab is auto-generated. Do NOT edit manually.")

        # ✅ Write data starting from row 4
        worksheet.update('A4', data)

        logger.info(f"✅ Exported to tab: {tab_name}")

    def strip_pii(self, dataframe):
        """Remove PII columns before exporting to Sheets."""
        pii_columns = [
            'customer_name', 'customer_email', 'shipping_address',
            'phone_number', 'ip_address', 'billing_address'
        ]
        return dataframe.drop(columns=[
            col for col in pii_columns if col in dataframe.columns
        ])
```

**Sheet Permissions Lockdown:**
- Share with **specific email addresses only** — never "anyone with the link"
- Set automated tabs to **view-only** for stakeholders
- Create a separate **"Manual Notes"** tab that the exporter never touches

---

### 8. 🐳 Docker & Infrastructure Hardening

**Risk:** Default Docker configurations expose services to the network. Outdated images contain known vulnerabilities.

#### Solution: Secure `docker-compose.yml`

```yaml
# docker-compose.yml — Security-hardened configuration

version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    ports:
      - "127.0.0.1:5432:5432"    # ✅ Bind to localhost ONLY
    environment:
      POSTGRES_DB: mvolo
      POSTGRES_USER: mvolo_user
      POSTGRES_PASSWORD_FILE: /run/secrets/db_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docker/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    secrets:
      - db_password
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mvolo_user -d mvolo"]
      interval: 30s
      timeout: 10s
      retries: 3

  metabase:
    image: metabase/metabase:latest
    restart: unless-stopped
    ports:
      - "127.0.0.1:3000:3000"    # ✅ Bind to localhost ONLY
    environment:
      MB_DB_TYPE: postgres
      MB_DB_HOST: postgres
      MB_DB_PORT: 5432
      MB_DB_DBNAME: metabase
      MB_DB_USER: metabase_user
    depends_on:
      postgres:
        condition: service_healthy

  n8n:
    image: n8nio/n8n:latest
    restart: unless-stopped
    ports:
      - "127.0.0.1:5678:5678"    # ✅ Bind to localhost ONLY
    environment:
      N8N_BASIC_AUTH_ACTIVE: "true"
      N8N_BASIC_AUTH_USER: admin
      N8N_BASIC_AUTH_PASSWORD_FILE: /run/secrets/n8n_password
    volumes:
      - n8n_data:/home/node/.n8n
    secrets:
      - n8n_password
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:5678/healthz"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  postgres_data:
  n8n_data:

secrets:
  db_password:
    file: ./secrets/db_password.txt
  n8n_password:
    file: ./secrets/n8n_password.txt
```

**Additional Infrastructure Hardening:**

- Set PostgreSQL to **SSL-only connections** (`postgresql.conf → ssl = on`)
- Create **read-only database roles** for Metabase: `GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase_reader;`
- Run containers as **non-root users** where possible
- Enable Docker **resource limits** (memory, CPU) to prevent runaway processes

---

### 9. 📝 Audit Trail & Observability

**Risk:** Without proper logging, you can't trace data lineage, debug failures, or demonstrate compliance.

#### Solution: Structured Logging & Monitoring

**Structured Logging Configuration (`config/logging.yaml`):**

```yaml
version: 1
disable_existing_loggers: false

formatters:
  json:
    class: pythonjsonlogger.jsonlogger.JsonFormatter
    format: "%(asctime)s %(name)s %(levelname)s %(message)s"

handlers:
  console:
    class: logging.StreamHandler
    formatter: json
    level: INFO

  file:
    class: logging.handlers.RotatingFileHandler
    formatter: json
    filename: logs/mvolo_pipeline.log
    maxBytes: 10485760   # 10 MB
    backupCount: 10
    level: DEBUG

  error_file:
    class: logging.handlers.RotatingFileHandler
    formatter: json
    filename: logs/mvolo_errors.log
    maxBytes: 10485760
    backupCount: 5
    level: ERROR

loggers:
  mvolo:
    level: DEBUG
    handlers: [console, file, error_file]
    propagate: false

root:
  level: WARNING
  handlers: [console]
```

**What to Log:**

| Event | Log Level | Example |
|-------|-----------|---------|
| Pipeline start/stop | INFO | `Pipeline run started: 2026-03-20T06:00:00` |
| Rows extracted | INFO | `Bol orders extracted: 142 rows` |
| Rows loaded | INFO | `Loaded 142 rows into raw.bol_orders` |
| Rate limit hit | WARNING | `429 received from Bol API, backing off 4.2s` |
| Schema validation fail | WARNING | `3 records failed validation, saved to invalid_records.json` |
| Pipeline failure | ERROR | `Transform stage failed: column 'total_price' not found` |
| Auth/credential access | DEBUG | `Authenticated with Bol API (token: ****a1b2)` |
| **Never log** | — | Full API tokens, customer PII, passwords |

---

### 10. 🔄 Pipeline Redundancy

**Risk:** If N8N goes down, the entire pipeline stops with no fallback.

#### Solution: Dual Scheduling Strategy

```python
# scripts/run_pipeline.py — Works independently of N8N

"""
Mvolo Pipeline Runner
Can be triggered by:
  1. N8N workflow (primary)
  2. System cron/Task Scheduler (backup fallback)
  3. Manual execution (developer)

Usage:
  python scripts/run_pipeline.py --full        # Full ETL pipeline
  python scripts/run_pipeline.py --extract      # Extraction only
  python scripts/run_pipeline.py --transform    # dbt only
  python scripts/run_pipeline.py --export       # Google Sheets only
"""

import argparse
import sys
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='Mvolo Pipeline Runner')
    parser.add_argument('--full', action='store_true', help='Run full pipeline')
    parser.add_argument('--extract', action='store_true', help='Extract only')
    parser.add_argument('--transform', action='store_true', help='Transform only')
    parser.add_argument('--export', action='store_true', help='Export only')
    args = parser.parse_args()

    print(f"🚀 Mvolo Pipeline — {datetime.now().isoformat()}")

    try:
        if args.full or args.extract:
            run_extraction()
        if args.full or args.transform:
            run_transformation()
        if args.full or args.export:
            run_export()

        print("✅ Pipeline completed successfully")
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        # Send alert (email, Slack, etc.)
        send_failure_alert(str(e))
        sys.exit(1)
```

**Windows Task Scheduler Fallback:**

```powershell
# Create a scheduled task as backup (runs if N8N is down)
$action = New-ScheduledTaskAction `
    -Execute "python" `
    -Argument "scripts/run_pipeline.py --full" `
    -WorkingDirectory "C:\Users\doryu\Documents\Project - Mvolo"

$trigger = New-ScheduledTaskTrigger -Daily -At 6:30AM

Register-ScheduledTask `
    -TaskName "Mvolo-Pipeline-Fallback" `
    -Action $action `
    -Trigger $trigger `
    -Description "Backup pipeline trigger if N8N is unavailable"
```

---

## 🚨 Data Leakage Prevention

> Data leakage refers to the **unauthorized transmission of data** from within the system to an external destination. Below are all identified leakage vectors — both current and potential future scenarios.

### Current Leakage Vectors

| Vector | How It Could Happen | Severity | Mitigation |
|--------|---------------------|----------|------------|
| **Git repository** | `.env`, raw JSON, or log files accidentally committed | 🔴 Critical | `.gitignore`, pre-commit hooks, `gitleaks` |
| **Console/terminal logs** | API tokens or PII printed during debugging | 🟠 High | Structured logging with secret masking |
| **Google Sheets** | PII exported to sheets shared with "anyone with link" | 🔴 Critical | PII stripping in `formatters.py`, strict sharing |
| **Metabase dashboards** | Raw PII visible in queries or dashboards | 🟠 High | Role-based access, column-level permissions |
| **Docker volumes** | Database data persisted in unencrypted Docker volumes | 🟡 Medium | Encrypted volumes, host-level encryption |
| **Raw data files** | `data/raw/` contains unencrypted JSON with full customer data | 🟠 High | Data retention policy, encrypted storage |
| **N8N workflows** | Exported workflow JSON may contain embedded credentials | 🟡 Medium | Sanitize workflows before committing |

### Future Leakage Vectors (As the Project Scales)

| Vector | Scenario | When It Becomes a Risk | Prevention |
|--------|----------|------------------------|------------|
| **New team members** | Intern or colleague gets full database access | When team grows beyond 1 person | Role-based access control (RBAC), principle of least privilege |
| **Cloud migration** | Moving from local Docker to AWS/GCP/Azure | When deploying to cloud | VPC isolation, IAM policies, encrypted S3 buckets, no public endpoints |
| **Additional data sources** | Adding a new marketplace (Amazon, Etsy) | When expanding channels | Audit each new API for PII fields, extend PII classification matrix |
| **Third-party integrations** | Connecting to CRM, email marketing, accounting tools | When integrating with Salesforce, Mailchimp, etc. | Data processing agreements (DPAs), API scope restrictions |
| **Backup files** | Database dumps stored on unencrypted disks or cloud storage | When implementing backup strategy | Encrypt all backups, restrict access, auto-expire old backups |
| **CI/CD pipelines** | Secrets exposed in GitHub Actions logs or environment | When setting up automated deployments | Use GitHub Secrets, never echo env vars in CI scripts |
| **Log aggregation** | Shipping logs to ELK/Datadog/CloudWatch that contain PII | When adding centralized monitoring | Log sanitization before shipping, PII redaction filters |
| **Data sharing** | Stakeholders requesting raw data exports for ad-hoc analysis | When business users want custom reports | Create a governed self-service layer, never export raw data |
| **Model training** | Using customer data for ML models (e.g., demand forecasting) | When adding predictive analytics | Anonymize training data, GDPR right to object to profiling |
| **API endpoint exposure** | Building a REST API on top of the warehouse for partners | When building external integrations | Authentication, rate limiting, field-level filtering, audit logging |
| **Mobile/web dashboards** | Building a customer-facing analytics portal | When expanding beyond internal tools | OWASP compliance, input validation, CORS restrictions |
| **Data marketplace** | Sharing aggregated analytics with suppliers or partners | When monetizing data insights | Differential privacy, k-anonymity, legal review |

### Data Flow Leakage Map

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                         LEAKAGE BOUNDARY                          │
  │                                                                     │
  │   Bol API ──→ extract/ ──→ data/raw/ ──→ load/ ──→ PostgreSQL     │
  │                  │              │                        │           │
  │                  │              │                        │           │
  │                  ▼              ▼                        ▼           │
  │          [Terminal Logs]  [Raw JSON Files]       [Database Dump]    │
  │          ⚠️ May contain   ⚠️ Full PII           ⚠️ Full PII       │
  │             API tokens                                              │
  │                                                                     │
  │   PostgreSQL ──→ dbt (transform/) ──→ marts/ ──→ Metabase          │
  │                                          │           │              │
  │                                          │           ▼              │
  │                                          │    [Dashboard URLs]      │
  │                                          │    ⚠️ If shared publicly │
  │                                          ▼                          │
  │                                   Google Sheets                     │
  │                                   ⚠️ If link-shared               │
  │                                                                     │
  ├─── Future Vectors ──────────────────────────────────────────────────┤
  │                                                                     │
  │   CI/CD Logs ──→ GitHub Actions        Cloud Storage ──→ S3/GCS   │
  │   ⚠️ Secrets in logs                  ⚠️ Unencrypted backups     │
  │                                                                     │
  │   Third-party APIs ──→ CRM/Marketing   Log Aggregator ──→ ELK    │
  │   ⚠️ PII sent to external services    ⚠️ PII in shipped logs     │
  │                                                                     │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## 🚑 Incident Response Plan

If a data leak or security incident is detected:

### Step 1: Contain (Immediately)
- **Revoke** compromised API keys/tokens immediately
- **Rotate** all database passwords
- **Disable** the affected pipeline component
- **Restrict** access to affected Google Sheets

### Step 2: Assess (Within 1 Hour)
- Determine **what data was exposed** (PII? Financial? Credentials?)
- Determine **how long** the exposure lasted
- Identify **who may have accessed** the data
- Check Git history: `git log --all --diff-filter=A -- "*.env" "*.json"`

### Step 3: Remediate (Within 24 Hours)
- Remove exposed data from Git history: `git filter-branch` or `BFG Repo-Cleaner`
- Patch the vulnerability that caused the leak
- Verify no other similar leaks exist

### Step 4: Report (Within 72 Hours — GDPR Requirement)
- If EU personal data was exposed, you **must report** to the relevant Data Protection Authority (DPA) within 72 hours
- For the Netherlands: [Autoriteit Persoonsgegevens](https://autoriteitpersoonsgegevens.nl)
- Document the incident, impact, and remediation steps

### Step 5: Prevent (Ongoing)
- Add the specific leak vector to this document
- Update pre-commit hooks/CI checks to catch similar issues
- Conduct a post-mortem and share findings

---

## ✅ Security Checklist

Use this checklist before each major milestone or deployment:

### Pre-Development
- [ ] `.env.example` created with placeholder values
- [ ] `.gitignore` covers `.env`, `data/raw/`, `*.duckdb`, logs
- [ ] PII fields identified and documented in data dictionary
- [ ] GDPR lawful basis documented

### During Development
- [ ] No secrets hardcoded in source code
- [ ] Logging does not output PII or credentials
- [ ] API versions are pinned in `config/sources.yaml`
- [ ] Schema validation is implemented for all API responses
- [ ] Database operations use transactions

### Pre-Deployment
- [ ] `gitleaks` scan passes on full repository history
- [ ] Docker ports bound to `127.0.0.1` only
- [ ] Default passwords changed for PostgreSQL, Metabase, N8N
- [ ] dbt schema tests and freshness checks configured
- [ ] Google Sheets sharing restricted to specific emails
- [ ] Backup strategy tested and verified
- [ ] Pipeline can run independently of N8N (manual fallback)

### Ongoing
- [ ] API keys rotated on schedule
- [ ] Raw data files cleaned per retention policy
- [ ] Docker images updated monthly
- [ ] This risk document reviewed quarterly

---

> **Last Updated:** 2026-03-20
>
> **Owner:** Dreu — Intern Data Engineer
>
> **Review Frequency:** Quarterly or after any security incident

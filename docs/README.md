# 📂 docs/ — Documentation

> Detailed technical documentation for the Mvolo pipeline — architecture, setup, API reference, and data dictionary.

---

## 🎯 Purpose

This directory contains all project documentation beyond the root README. It serves as the single source of truth for:
- How the system works (architecture)
- How to set it up (setup guide)
- What APIs are used and how (API reference)
- What data fields exist and what they mean (data dictionary)

---

## 📁 Files

| File | Purpose |
|------|---------|
| `architecture.md` | System architecture, data flow, and design decisions |
| `setup_guide.md` | Step-by-step local development setup |
| `api_reference.md` | Bol.com & Shopify API endpoints, auth, and response schemas |
| `data_dictionary.md` | Every field in every table — name, type, description, source |
| `diagrams/` | Visual diagrams (architecture, data model, ETL flow) |

---

## 🔧 Implementation Plan

### 1. `architecture.md`

Document the following:
- **System overview** — High-level architecture diagram with all components
- **Data flow** — Step-by-step journey of data from API to dashboard
- **Design decisions** — Why DuckDB for POC, why dbt for transforms, why N8N over Airflow
- **Schema design** — Raw → Staging → Intermediate → Marts layer explanation
- **Security** — How secrets are managed, service account access, read-only roles

### 2. `setup_guide.md`

Step-by-step instructions:
1. Prerequisites (Python, Docker, API credentials)
2. Clone the repository
3. Configure `.env` from `.env.example`
4. Set up Google Cloud Service Account
5. Start Docker services
6. Initialize the warehouse
7. Run validation script
8. Execute first pipeline run
9. Access Metabase and build dashboards
10. Import N8N workflows

### 3. `api_reference.md`

For each API (Bol.com, Shopify):
- Authentication method and endpoint
- List of endpoints used with request/response examples
- Rate limit details
- Pagination strategy
- Common error codes and handling
- Links to official documentation

### 4. `data_dictionary.md`

For every table in the warehouse:

```markdown
## raw.bol_orders

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| order_id | VARCHAR | Unique order identifier | Bol API: orderId |
| order_data | JSONB | Full API response | Bol API response body |
| extracted_at | TIMESTAMP | When the record was extracted | System generated |
| source | VARCHAR | Always "bol" | System generated |

## core.fct_orders

| Column | Type | Description | Source Model |
|--------|------|-------------|-------------|
| order_key | VARCHAR | Surrogate key | Generated (dbt_utils) |
| order_id | VARCHAR | Original order ID | int_unified_orders |
| order_placed_at | TIMESTAMP | When the order was placed | stg_bol_orders / stg_shopify_orders |
| channel | VARCHAR | "bol" or "shopify" | stg_* models |
| total_price | DECIMAL | Order total in EUR | stg_* models |
```

### 5. `diagrams/`

Create visual diagrams for:
- `etl_flow.png` — Full pipeline flow from APIs to dashboards
- `data_model.png` — Star schema (dimensions + facts) entity relationship diagram

**Tools for creating diagrams:**
- [draw.io](https://draw.io) (free, exports to PNG)
- [Mermaid](https://mermaid.js.org) (can embed in markdown)
- [dbdiagram.io](https://dbdiagram.io) (for database schemas)

---

## ⚠️ Key Considerations

1. **Keep docs updated** — Update documentation whenever the pipeline changes
2. **Link from root README** — The root `README.md` should link to all docs
3. **Use diagrams** — Visual documentation is more impactful than text for architecture
4. **Data dictionary is critical** — This is the most important doc for business stakeholders

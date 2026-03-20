# 📂 extract/ — Extraction Layer

> Pulls raw data from **Bol.com** and **Shopify** REST APIs using Python.

---

## 🎯 Purpose

This module handles the **"E" in ELT** — connecting to external APIs, authenticating, paginating through results, and returning structured data ready for loading into the warehouse.

---

## 📁 Files

| File | Purpose |
|------|---------|
| `base.py` | Abstract base class for all extractors — shared retry logic, rate limiting, logging |
| `bol_extractor.py` | Bol.com API client — handles OAuth2, endpoints, response parsing |
| `shopify_extractor.py` | Shopify Admin API client — handles auth token, endpoints, response parsing |
| `config.py` | API endpoints, pagination settings, rate limit thresholds |
| `utils.py` | Shared helpers — timestamp formatting, response flattening, error handling |

---

## 🔧 Implementation Plan

### 1. `base.py` — Base Extractor Class

```python
# Key responsibilities:
# - HTTP session management with retry logic (exponential backoff)
# - Rate limit handling (respect API quotas)
# - Logging (each API call logged with timestamp, endpoint, status)
# - Pagination abstraction (cursor-based or offset-based)

class BaseExtractor:
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session = requests.Session()
        self.logger = logging.getLogger(source_name)

    def _request_with_retry(self, method, url, **kwargs) -> dict:
        """Makes HTTP request with exponential backoff retry."""
        pass

    def _handle_rate_limit(self, response):
        """Reads rate limit headers, sleeps if approaching limit."""
        pass

    def extract(self, start_date, end_date) -> list[dict]:
        """Override in subclasses. Returns list of records."""
        raise NotImplementedError
```

### 2. `bol_extractor.py` — Bol.com API Client

```python
# Authentication: OAuth2 Client Credentials flow
# Base URL: https://api.bol.com/retailer/
# Key endpoints:
#   - GET /orders           → Order data
#   - GET /offers           → Product/offer listings
#   - GET /shipments        → Shipment tracking
#   - GET /returns          → Return data
#   - GET /inventory        → Stock levels

class BolExtractor(BaseExtractor):
    TOKEN_URL = "https://login.bol.com/token"

    def _authenticate(self):
        """Fetches OAuth2 access token using client_id + client_secret."""
        pass

    def extract_orders(self, start_date, end_date) -> list[dict]:
        """Extracts orders within the given date range."""
        pass

    def extract_offers(self) -> list[dict]:
        """Extracts all active product offers."""
        pass

    def extract_shipments(self, start_date, end_date) -> list[dict]:
        """Extracts shipment data within the given date range."""
        pass

    def extract_returns(self, start_date, end_date) -> list[dict]:
        """Extracts return data within the given date range."""
        pass
```

**Bol.com API Notes:**
- Token expires every ~5 minutes — must refresh automatically
- Rate limit: ~25 requests/second (check `X-RateLimit-*` headers)
- Pagination: uses `page` parameter (offset-based)
- All dates in ISO 8601 format

### 3. `shopify_extractor.py` — Shopify Admin API Client

```python
# Authentication: Admin API access token (header: X-Shopify-Access-Token)
# Base URL: https://{store}.myshopify.com/admin/api/2024-01/
# Key endpoints:
#   - GET /orders.json         → Order data
#   - GET /products.json       → Product catalog
#   - GET /customers.json      → Customer data
#   - GET /inventory_levels.json → Stock levels

class ShopifyExtractor(BaseExtractor):
    API_VERSION = "2024-01"

    def extract_orders(self, start_date, end_date) -> list[dict]:
        """Extracts orders using created_at_min/max filters."""
        pass

    def extract_products(self) -> list[dict]:
        """Extracts full product catalog with variants."""
        pass

    def extract_customers(self, start_date, end_date) -> list[dict]:
        """Extracts customer data."""
        pass

    def extract_inventory(self) -> list[dict]:
        """Extracts current inventory levels per variant."""
        pass
```

**Shopify API Notes:**
- Rate limit: 2 requests/second (leaky bucket — check `X-Shopify-Shop-Api-Call-Limit` header)
- Pagination: uses **cursor-based** `Link` headers (not page numbers)
- Max 250 records per page
- Filter by date using `created_at_min`, `created_at_max`, `updated_at_min`, etc.

### 4. `config.py` — Configuration

```python
# Centralized API configuration
# Values loaded from environment variables (.env)

BOL_CONFIG = {
    "client_id": os.getenv("BOL_CLIENT_ID"),
    "client_secret": os.getenv("BOL_CLIENT_SECRET"),
    "base_url": "https://api.bol.com/retailer/",
    "rate_limit_per_second": 25,
    "page_size": 50,
}

SHOPIFY_CONFIG = {
    "store_url": os.getenv("SHOPIFY_STORE_URL"),
    "access_token": os.getenv("SHOPIFY_ACCESS_TOKEN"),
    "api_version": "2024-01",
    "rate_limit_per_second": 2,
    "page_size": 250,
}
```

### 5. `utils.py` — Shared Utilities

```python
# Helper functions used by all extractors:
# - flatten_nested_json()  → Flattens nested API responses for tabular loading
# - parse_iso_date()       → Standardizes date formats across APIs
# - build_date_ranges()    → Splits large date ranges into chunks (for rate limiting)
# - log_extraction()       → Logs extraction metadata (records pulled, duration, errors)
```

---

## 📦 Dependencies

```
requests>=2.31.0
python-dotenv>=1.0.0
```

---

## 🔄 Data Flow

```
Bol.com API ──→ bol_extractor.py ──→ list[dict] ──→ load/
Shopify API ──→ shopify_extractor.py ──→ list[dict] ──→ load/
```

Each extractor returns a **list of dictionaries** — one dict per record (order, product, etc.). The `load/` module then takes these and inserts them into the warehouse.

---

## ⚠️ Key Considerations

1. **Incremental extraction** — Use `updated_at` filters to only pull new/changed records after the initial full load
2. **Error handling** — Failed API calls should be logged but not crash the pipeline; retry 3x with backoff
3. **Secrets management** — Never hardcode API keys; always load from `.env`
4. **Rate limits** — Always check response headers; sleep when approaching limits

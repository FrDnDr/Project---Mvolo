# 📂 tests/ — Python Tests

> Unit and integration tests for the extraction, loading, and export modules.

---

## 🎯 Purpose

Tests ensure:
- API extractors correctly parse responses
- Loaders properly insert data into the warehouse
- Google Sheets exporter formats and pushes data correctly
- Pipeline doesn't break when APIs change response schemas

---

## 📁 Files

| File | Purpose |
|------|---------|
| `test_bol_extractor.py` | Tests for Bol.com API extraction logic |
| `test_shopify_extractor.py` | Tests for Shopify API extraction logic |
| `test_loader.py` | Tests for DuckDB and PostgreSQL loading |
| `fixtures/` | Sample API responses (JSON) for mocking |

---

## 🔧 Implementation Plan

### Test Strategy

| Test Type | What | How |
|-----------|------|-----|
| **Unit tests** | Individual functions (parsing, formatting, date calculations) | Mock API responses using `fixtures/` |
| **Integration tests** | Full extract → load flow | Use DuckDB in-memory database |
| **Data quality tests** | dbt model correctness | `dbt test` (separate from Python tests) |

### 1. `test_bol_extractor.py`

```python
"""Tests for the Bol.com extractor."""
import pytest
import json
from unittest.mock import patch, MagicMock
from extract.bol_extractor import BolExtractor

class TestBolExtractor:

    @pytest.fixture
    def sample_orders_response(self):
        """Load sample API response from fixtures."""
        with open("tests/fixtures/bol_orders_response.json") as f:
            return json.load(f)

    def test_authentication(self):
        """Test OAuth2 token retrieval succeeds with valid credentials."""
        pass

    def test_authentication_failure(self):
        """Test graceful handling of invalid credentials."""
        pass

    @patch("extract.bol_extractor.requests.Session.get")
    def test_extract_orders_parses_response(self, mock_get, sample_orders_response):
        """Test that API response is correctly parsed into records."""
        mock_get.return_value.json.return_value = sample_orders_response
        mock_get.return_value.status_code = 200

        extractor = BolExtractor()
        orders = extractor.extract_orders("2026-03-01", "2026-03-07")

        assert len(orders) > 0
        assert "order_id" in orders[0]

    def test_rate_limit_handling(self):
        """Test that rate limiting pauses extraction correctly."""
        pass

    def test_pagination(self):
        """Test that paginated results are fully collected."""
        pass

    def test_date_range_filter(self):
        """Test that only records within the date range are returned."""
        pass
```

### 2. `test_shopify_extractor.py`

```python
"""Tests for the Shopify extractor."""
import pytest
from unittest.mock import patch
from extract.shopify_extractor import ShopifyExtractor

class TestShopifyExtractor:

    @pytest.fixture
    def sample_orders_response(self):
        with open("tests/fixtures/shopify_orders_response.json") as f:
            return json.load(f)

    def test_authentication_header(self):
        """Test that X-Shopify-Access-Token header is set correctly."""
        pass

    @patch("extract.shopify_extractor.requests.Session.get")
    def test_extract_orders(self, mock_get, sample_orders_response):
        """Test order extraction and parsing."""
        pass

    def test_cursor_based_pagination(self):
        """Test Shopify's Link-header cursor pagination."""
        pass

    def test_handles_empty_response(self):
        """Test graceful handling of empty API response."""
        pass
```

### 3. `test_loader.py`

```python
"""Tests for the data loader."""
import pytest
import duckdb
from load.duckdb_loader import DuckDBLoader

class TestDuckDBLoader:

    @pytest.fixture
    def in_memory_db(self):
        """Create an in-memory DuckDB for testing."""
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_create_raw_tables(self, in_memory_db):
        """Test that all raw tables are created correctly."""
        pass

    def test_load_records_insert(self, in_memory_db):
        """Test inserting new records."""
        pass

    def test_load_records_upsert(self, in_memory_db):
        """Test that duplicate records are updated, not duplicated."""
        pass

    def test_load_empty_records(self, in_memory_db):
        """Test that loading an empty list doesn't crash."""
        pass

    def test_transaction_rollback_on_error(self, in_memory_db):
        """Test that failed loads rollback cleanly."""
        pass
```

### 4. `fixtures/` — Sample API Responses

These are **real API response structures** with **fake data** for testing without API keys:

**`fixtures/bol_orders_response.json`**
```json
{
  "orders": [
    {
      "orderId": "1234567890",
      "orderPlacedDateTime": "2026-03-10T14:30:00+01:00",
      "orderItems": [
        {
          "orderItemId": "item-001",
          "ean": "9781234567890",
          "title": "Test Product",
          "quantity": 1,
          "unitPrice": 29.99
        }
      ]
    }
  ]
}
```

**`fixtures/shopify_orders_response.json`**
```json
{
  "orders": [
    {
      "id": 9876543210,
      "created_at": "2026-03-10T14:30:00+01:00",
      "total_price": "49.99",
      "currency": "EUR",
      "financial_status": "paid",
      "line_items": [
        {
          "id": 11111,
          "title": "Test Product",
          "quantity": 1,
          "price": "49.99"
        }
      ]
    }
  ]
}
```

---

## 🏃 Running Tests

```bash
# Run all Python tests
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=extract --cov=load --cov=export --cov-report=html

# Run specific test file
pytest tests/test_bol_extractor.py -v

# Run dbt data quality tests (separate)
cd transform && dbt test
```

---

## 📦 Dependencies

```
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-mock>=3.12.0
```

---

## ⚠️ Key Considerations

1. **No real API calls in tests** — Always mock HTTP requests using `unittest.mock` or `pytest-mock`
2. **Fixtures are committed** — Sample JSONs in `fixtures/` should be tracked in Git
3. **In-memory databases** — Use `duckdb.connect(":memory:")` for loader tests — no cleanup needed
4. **CI/CD** — Tests run automatically on GitHub Actions (see `.github/workflows/ci.yml`)

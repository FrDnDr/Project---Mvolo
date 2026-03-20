# 📂 export/ — Export Layer (Google Sheets)

> Pushes weekly analytical summaries from the warehouse to **Google Sheets** with labeled columns and formatted tabs.

---

## 🎯 Purpose

Every **Monday**, this module queries the transformed data from the warehouse (specifically `marts/analytics/` models), formats it with proper labels and headers, and pushes it to a shared Google Sheets workbook. This gives business stakeholders a familiar, no-training-required view of the previous week's performance.

---

## 📁 Files

| File | Purpose |
|------|---------|
| `sheets_exporter.py` | Google Sheets API client — authentication, read/write operations |
| `formatters.py` | Queries warehouse marts, formats data into labeled DataFrames per tab |
| `config.py` | Spreadsheet ID, tab names, column label mappings |
| `templates/weekly_report.yaml` | YAML template defining sheet structure, tabs, columns, formatting |

---

## 🔧 Implementation Plan

### 1. `sheets_exporter.py` — Google Sheets API Client

```python
# Uses: gspread (Python wrapper for Google Sheets API v4)
# Auth: Google Cloud Service Account (JSON key file)

import gspread
from google.oauth2.service_account import Credentials

class SheetsExporter:
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    def __init__(self, credentials_path: str, spreadsheet_id: str):
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self.client = None
        self.spreadsheet = None

    def authenticate(self):
        """Authenticate using Service Account JSON key."""
        creds = Credentials.from_service_account_file(
            self.credentials_path, scopes=self.SCOPES
        )
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)

    def clear_and_write_tab(self, tab_name: str, data: list[list], header: list[str]):
        """Clear existing tab content, write header + data.
        Steps:
          1. Get or create the worksheet tab
          2. Clear all existing content
          3. Write header row (bold, frozen)
          4. Write data rows
          5. Auto-resize columns
        """
        pass

    def add_reporting_period_header(self, tab_name: str, start_date: str, end_date: str):
        """Adds a 'Reporting Period: Mar 10 - Mar 16, 2026' header to the top of the tab."""
        pass

    def export_weekly_report(self, formatted_data: dict):
        """Exports all tabs for the weekly report.
        Args:
            formatted_data: Dict of {tab_name: {header: [...], rows: [...]}}
        """
        for tab_name, tab_data in formatted_data.items():
            self.clear_and_write_tab(tab_name, tab_data["rows"], tab_data["header"])
```

### 2. `formatters.py` — Data Formatting

```python
# Queries the warehouse marts and formats the results into
# labeled, human-readable DataFrames for each Google Sheet tab.

class WeeklyReportFormatter:
    def __init__(self, db_connection, start_date, end_date):
        self.conn = db_connection
        self.start_date = start_date  # Last Monday
        self.end_date = end_date      # Last Sunday

    def format_revenue_summary(self) -> dict:
        """Queries marts.revenue_by_channel for the week.
        Returns:
            {
                "header": ["Date", "Channel", "Total Orders", "Revenue (€)", "Avg Order Value (€)"],
                "rows": [["2026-03-10", "Bol.com", 45, 2340.50, 52.01], ...]
            }
        """
        pass

    def format_product_performance(self) -> dict:
        """Queries marts.product_performance for the week.
        Returns:
            {
                "header": ["Product Name", "SKU", "Units Sold", "Revenue (€)", "Return Rate (%)"],
                "rows": [...]
            }
        """
        pass

    def format_fulfillment_metrics(self) -> dict:
        """Queries marts.fulfillment_metrics for the week."""
        pass

    def format_returns_analysis(self) -> dict:
        """Queries marts.return_rate_analysis for the week."""
        pass

    def format_all(self) -> dict:
        """Returns all tabs formatted and ready for export.
        Returns:
            {
                "Revenue Summary": {...},
                "Product Performance": {...},
                "Fulfillment": {...},
                "Returns": {...},
            }
        """
        return {
            "Revenue Summary": self.format_revenue_summary(),
            "Product Performance": self.format_product_performance(),
            "Fulfillment": self.format_fulfillment_metrics(),
            "Returns": self.format_returns_analysis(),
        }
```

### 3. `templates/weekly_report.yaml` — Sheet Template

```yaml
# Defines the structure of the weekly Google Sheets report
# Used by formatters.py to know what to query and how to label

report_name: "Weekly Dropshipping Report"

tabs:
  - name: "Revenue Summary"
    source_model: "marts.revenue_by_channel"
    columns:
      - db_column: "order_date"
        label: "Date"
        format: "date"
      - db_column: "channel"
        label: "Channel"
        format: "text"
      - db_column: "total_orders"
        label: "Total Orders"
        format: "number"
      - db_column: "total_revenue"
        label: "Revenue (€)"
        format: "currency_eur"
      - db_column: "avg_order_value"
        label: "Avg Order Value (€)"
        format: "currency_eur"

  - name: "Product Performance"
    source_model: "marts.product_performance"
    columns:
      - db_column: "product_name"
        label: "Product Name"
        format: "text"
      - db_column: "sku"
        label: "SKU"
        format: "text"
      - db_column: "units_sold"
        label: "Units Sold"
        format: "number"
      - db_column: "revenue"
        label: "Revenue (€)"
        format: "currency_eur"
      - db_column: "return_rate"
        label: "Return Rate (%)"
        format: "percentage"

  - name: "Fulfillment"
    source_model: "marts.fulfillment_metrics"
    columns:
      - db_column: "orders_shipped"
        label: "Orders Shipped"
        format: "number"
      - db_column: "avg_delivery_days"
        label: "Avg Delivery Days"
        format: "decimal"
      - db_column: "fulfillment_rate"
        label: "Fulfillment Rate (%)"
        format: "percentage"

  - name: "Returns"
    source_model: "marts.return_rate_analysis"
    columns:
      - db_column: "product_name"
        label: "Product"
        format: "text"
      - db_column: "return_count"
        label: "Return Count"
        format: "number"
      - db_column: "return_rate"
        label: "Return Rate (%)"
        format: "percentage"
      - db_column: "top_reason"
        label: "Top Reason"
        format: "text"
```

### 4. `config.py` — Export Configuration

```python
import os

GOOGLE_SHEETS_CONFIG = {
    "credentials_path": os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH"),
    "spreadsheet_id": os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID"),
}
```

---

## 🔑 Google Sheets API Setup

### Step 1: Create a Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project (e.g., "Mvolo Pipeline")
3. Enable the **Google Sheets API** and **Google Drive API**

### Step 2: Create a Service Account
1. Go to **IAM & Admin** → **Service Accounts**
2. Create a service account (e.g., `mvolo-pipeline@your-project.iam.gserviceaccount.com`)
3. Generate a JSON key → download it
4. Save the key as `config/google_service_account.json` (this file is **gitignored**)

### Step 3: Share the Spreadsheet
1. Create a Google Sheets spreadsheet manually (or programmatically)
2. Share it with the service account email (give **Editor** access)
3. Copy the spreadsheet ID from the URL and add it to `.env`

### API Limits (Free Tier)
| Limit | Value |
|-------|-------|
| Read requests | 300/min per project |
| Write requests | 300/min per project |
| Per-user limit | 60/min |
| Cells per spreadsheet | 10,000,000 |

> For a weekly batch export, you'll never come close to these limits.

---

## 📅 Weekly Export Flow

```
Monday 7:00 AM (triggered by N8N)
  │
  ├── 1. Calculate date range (last Mon → last Sun)
  ├── 2. Connect to warehouse
  ├── 3. Query marts/analytics/ models for the week
  ├── 4. Format data with labels (via formatters.py)
  ├── 5. Authenticate with Google Sheets API
  ├── 6. Clear existing sheet tabs
  ├── 7. Write new data with headers
  └── 8. Log success/failure
```

---

## 📦 Dependencies

```
gspread>=6.0.0
google-auth>=2.25.0
```

---

## ⚠️ Key Considerations

1. **Overwrite, don't append** — Each Monday clears and rewrites the sheet so data is always fresh and clean
2. **Labeled columns** — Use human-readable labels ("Revenue (€)"), not database column names ("total_revenue")
3. **Formatted values** — Dates as "Mar 10, 2026", currency with € symbol, percentages with % sign
4. **Error handling** — If Google Sheets API fails, log the error but don't crash the main pipeline
5. **Never commit credentials** — `google_service_account.json` must be in `.gitignore`

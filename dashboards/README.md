# 📂 dashboards/ — Metabase Dashboards

> Documentation and screenshots of the **Metabase** business intelligence dashboards connected to the warehouse.

---

## 🎯 Purpose

Metabase connects directly to the data warehouse (PostgreSQL / DuckDB) and provides:
- Interactive dashboards for real-time exploration
- Pre-built visualizations for key dropshipping KPIs
- Shareable links for stakeholders
- Scheduled email reports (built into Metabase)

---

## 📁 Files

| File | Purpose |
|------|---------|
| `README.md` | This file — dashboard descriptions and setup guide |
| `screenshots/` | Dashboard screenshots for the GitHub README |

---

## 🔧 Implementation Plan

### Metabase Setup

1. **Start Metabase** — `docker-compose up -d metabase`
2. **Access** — `http://localhost:3000`
3. **First-time wizard** — Connect to PostgreSQL (use credentials from `.env`)
4. **Connect to schemas** — Point Metabase to the `core` and `analytics` schemas

### Connecting to the Warehouse

```
Database type:  PostgreSQL
Host:           postgres       (Docker service name)
Port:           5432
Database name:  mvolo
Username:       metabase_reader  (read-only role from init.sql)
Password:       metabase_readonly
```

> Use the **read-only** `metabase_reader` role — Metabase should never write to the warehouse.

---

## 📊 Planned Dashboards

### Dashboard 1: Revenue Overview

**Purpose:** High-level revenue metrics across channels
**Source models:** `analytics.revenue_by_channel`, `core.fct_orders`

| Visualization | Type | Description |
|---------------|------|-------------|
| Total Revenue (This Week) | Number card | Single KPI — total revenue |
| Revenue by Channel | Bar chart | Bol vs Shopify side-by-side |
| Revenue Trend | Line chart | Daily/weekly revenue over time |
| Avg Order Value | Number card | Average order value across channels |
| Orders by Day | Bar chart | Order volume distribution across weekdays |

### Dashboard 2: Product Performance

**Purpose:** Identify best and worst performing products
**Source models:** `analytics.product_performance`, `core.dim_products`

| Visualization | Type | Description |
|---------------|------|-------------|
| Top 10 Products by Revenue | Horizontal bar | Best sellers ranked |
| Bottom 10 Products | Table | Worst performers (candidates for removal) |
| Product Revenue Distribution | Pie chart | Revenue share by product category |
| Units Sold vs Returns | Scatter plot | Identify high-return products |

### Dashboard 3: Channel Comparison (Bol vs Shopify)

**Purpose:** Compare performance across sales channels
**Source models:** `analytics.revenue_by_channel`, `analytics.fulfillment_metrics`

| Visualization | Type | Description |
|---------------|------|-------------|
| Channel Revenue Split | Donut chart | % split between Bol and Shopify |
| Avg Delivery Time by Channel | Number cards | Compare fulfillment speed |
| Return Rate by Channel | Bar chart | Which channel has more returns? |
| Revenue Growth by Channel | Line chart | Trend comparison over weeks/months |

### Dashboard 4: Fulfillment & Returns

**Purpose:** Logistics and return analysis
**Source models:** `analytics.fulfillment_metrics`, `analytics.return_rate_analysis`

| Visualization | Type | Description |
|---------------|------|-------------|
| Fulfillment Rate | Gauge | % of orders fulfilled on time |
| Avg Delivery Time | Number card | Average days from order to delivery |
| Return Rate Trend | Line chart | Return rate over time |
| Top Return Reasons | Bar chart | Most common reasons for returns |

---

## 📸 Taking Screenshots

For the GitHub README:
1. Build the dashboard in Metabase
2. Use the **fullscreen mode** for clean screenshots
3. Save to `screenshots/` with descriptive names
4. Reference in the root `README.md`

```markdown
<!-- In root README.md -->
## 📊 Dashboard Previews

![Revenue Overview](dashboards/screenshots/revenue_overview.png)
![Product Performance](dashboards/screenshots/product_performance.png)
```

---

## ⚠️ Key Considerations

1. **Read-only access** — Metabase should use the `metabase_reader` role, never the pipeline user
2. **Caching** — Metabase caches query results; set cache duration based on pipeline frequency (e.g., 24h for daily pipelines)
3. **Filters** — Add date range and channel filters to all dashboards for interactivity
4. **Mobile-friendly** — Metabase dashboards are responsive; test on mobile view for stakeholders
5. **Scheduled reports** — Metabase can email dashboard snapshots on a schedule (Pulse feature)

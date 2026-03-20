# 📂 data/ — Data Storage

> Local data storage — **raw** API dumps (gitignored) and **sample** anonymized data (tracked in Git).

---

## 🎯 Purpose

| Subdirectory | Tracked in Git? | Purpose |
|---|---|---|
| `raw/` | ❌ **No** (gitignored) | Stores actual API response dumps during extraction — contains real business data |
| `sample/` | ✅ **Yes** | Contains anonymized, fake data for testing and demos — safe to commit |

---

## 📁 Structure

```
data/
├── raw/                          # ⚠️ GITIGNORED — real data lives here
│   ├── .gitkeep                  # Keeps the directory in Git (even though contents are ignored)
│   ├── bol_orders_2026-03-10.json
│   ├── shopify_orders_2026-03-10.json
│   └── ...
│
└── sample/                       # ✅ TRACKED — fake data for testing
    ├── sample_bol_orders.json
    ├── sample_shopify_orders.json
    └── README.md
```

---

## 📂 raw/ — Raw API Dumps

### How It Works

During extraction, the pipeline **optionally** saves raw API responses to `data/raw/` before loading into the warehouse:

```python
# In extract/base.py
def save_raw_response(self, data, source, entity, date):
    filename = f"data/raw/{source}_{entity}_{date}.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
```

### Why Save Raw Files?

- **Debugging** — If something looks wrong in the warehouse, check the raw dump
- **Replay** — Re-run the load step without re-calling the API
- **Audit trail** — Know exactly what the API returned on a given day

### ⚠️ Important

- **Never commit** `raw/` files — they contain real business data
- The `.gitkeep` file ensures the `raw/` directory exists in Git
- Configure in `settings.yaml`: `save_raw_files: true/false`

---

## 📂 sample/ — Sample Test Data

### Purpose

Sample data allows anyone to:
1. **Clone the repo** and run the pipeline immediately (without API keys)
2. **Test transformations** — dbt models can run on sample data
3. **Demo the project** — Show dashboards with realistic-looking data

### How to Generate

```bash
python scripts/seed_sample_data.py --records 50
```

This uses the **Faker** library to generate realistic but anonymized data matching real API response schemas.

### Data Format

Sample files match the exact schema of real API responses, but with fake values:
- Order IDs are randomly generated
- Product names are fictitious
- Prices are realistic but fabricated
- Dates are within a sample range
- No real customer or business data

---

## ⚠️ Key Considerations

1. **Disk space** — Raw files can grow large over time; consider periodic cleanup
2. **Date-stamped filenames** — Every raw dump includes the extraction date for traceability
3. **Sample data should mirror reality** — Same JSON structure, same field names, realistic values

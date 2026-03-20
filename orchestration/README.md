# 📂 orchestration/ — N8N Pipeline Orchestration

> Schedules and coordinates the entire ETL pipeline using **N8N** — a visual, open-source workflow automation tool.

---

## 🎯 Purpose

N8N acts as the **scheduler and coordinator** for the pipeline. It triggers Python scripts on a schedule, handles dependencies between steps, and provides visibility into pipeline status.

---

## 📁 Files

| File | Purpose |
|------|---------|
| `README.md` | This file — setup and usage guide |
| `workflows/daily_full_pipeline.json` | Main daily ETL workflow (Extract → Load → Transform) |
| `workflows/weekly_sheets_export.json` | Monday weekly report (Extract last week → Push to Sheets) |
| `workflows/bol_extraction_only.json` | Standalone Bol.com extraction (for testing/debugging) |
| `workflows/shopify_extraction_only.json` | Standalone Shopify extraction (for testing/debugging) |

---

## 🔧 Implementation Plan

### N8N Setup

N8N runs as a **Docker container** (defined in `docker-compose.yml`):

```yaml
# In docker-compose.yml
n8n:
  image: n8nio/n8n:latest
  ports:
    - "5678:5678"
  environment:
    - N8N_BASIC_AUTH_ACTIVE=true
    - N8N_BASIC_AUTH_USER=admin
    - N8N_BASIC_AUTH_PASSWORD=${N8N_PASSWORD}
  volumes:
    - n8n_data:/home/node/.n8n
    - ./:/app     # Mount project directory so N8N can run Python scripts
```

Access N8N UI at: `http://localhost:5678`

---

### Workflow 1: Daily Full Pipeline

**File:** `workflows/daily_full_pipeline.json`
**Schedule:** Every day at 6:00 AM
**Purpose:** Full ETL — Extract from both APIs, Load into warehouse, Transform with dbt

```
┌─────────────┐
│  Cron Trigger│  (Daily 6:00 AM)
│  (Schedule)  │
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌──────────────┐
│  Extract Bol │────▶│ Extract      │
│  (Python)    │     │ Shopify      │
└──────┬───────┘     │ (Python)     │
       │             └──────┬───────┘
       │                    │
       ▼                    ▼
┌──────────────────────────────────┐
│        Load to Warehouse         │
│        (Python)                  │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│        dbt Run                   │
│        (dbt run && dbt test)     │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│        Notification              │
│        (Success/Failure alert)   │
└──────────────────────────────────┘
```

**N8N Nodes:**
1. **Cron** — Triggers daily at 6:00 AM
2. **Execute Command** — `python scripts/run_pipeline.py --mode extract --source bol`
3. **Execute Command** — `python scripts/run_pipeline.py --mode extract --source shopify`
4. **Execute Command** — `python scripts/run_pipeline.py --mode load`
5. **Execute Command** — `cd transform && dbt run && dbt test`
6. **IF Node** — Check if all steps succeeded
7. **Send Email / Webhook** — Notify on success or failure

---

### Workflow 2: Weekly Google Sheets Export

**File:** `workflows/weekly_sheets_export.json`
**Schedule:** Every Monday at 7:00 AM (1 hour after daily pipeline)
**Purpose:** Query last week's data (Mon-Sun) and push to Google Sheets

```
┌─────────────┐
│  Cron Trigger│  (Monday 7:00 AM)
│  (Schedule)  │
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────┐
│  Calculate Date Range            │
│  (Last Monday → Last Sunday)     │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  Run Weekly Export Script        │
│  python scripts/run_pipeline.py  │
│  --mode export-sheets            │
│  --start-date YYYY-MM-DD        │
│  --end-date YYYY-MM-DD          │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  Notification                    │
│  (Sheets updated / Error)        │
└──────────────────────────────────┘
```

**N8N Nodes:**
1. **Cron** — Monday 7:00 AM
2. **Function Node** — Calculate last week's Mon-Sun dates
3. **Execute Command** — Run the export Python script with date range arguments
4. **IF Node** — Check success
5. **Notification** — Email/Slack alert

---

### Workflow 3 & 4: Standalone Extractions

**For testing and debugging** — run Bol or Shopify extraction independently:

```bash
# Via N8N UI: manually trigger these workflows
# Or via command line:
python scripts/run_pipeline.py --mode extract --source bol
python scripts/run_pipeline.py --mode extract --source shopify
```

---

## 📤 Importing/Exporting Workflows

### Export from N8N (to save in Git)

1. Open N8N UI → Go to the workflow
2. Click **⋮** (menu) → **Download**
3. Save the JSON file to `orchestration/workflows/`
4. Commit to Git

### Import to N8N (from Git)

1. Open N8N UI → Click **Add Workflow**
2. Click **⋮** (menu) → **Import from File**
3. Select the JSON file from `orchestration/workflows/`

> **Important:** Always export workflows to JSON and commit them. N8N's internal database is not version-controlled.

---

## 🔔 Alerting & Monitoring

### Pipeline Failure Alerts

Configure N8N to send notifications on failure:
- **Email** — via SMTP node
- **Slack** — via Slack webhook node
- **Webhook** — custom HTTP callback

### Execution History

N8N automatically logs all workflow executions:
- View in N8N UI → **Executions**
- Shows: start time, duration, status (success/error), error details
- Retention: configurable (default: 30 days)

---

## ⚠️ Key Considerations

1. **Timing** — Weekly sheets export runs at 7:00 AM, 1 hour after the daily pipeline, to ensure data is fresh
2. **Error handling** — Each step should be wrapped in try/catch; failures shouldn't block subsequent independent steps
3. **Idempotency** — Re-running a workflow should be safe (upserts, not duplicates)
4. **Version control** — Always export workflow JSONs to Git after changes
5. **Secrets** — Use N8N's built-in credential store, not hardcoded values in workflows

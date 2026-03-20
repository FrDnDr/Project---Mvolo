# 🏗️ Mvolo

> **Automated Data Warehousing & ETL Pipeline for Dropshipping Analytics**
>
> Centralizes data from **Bol.com** and **Shopify** into a unified data warehouse, with automated weekly Google Sheets reporting and interactive Metabase dashboards.

---

## 🎯 Problem Statement

Manual data entry from multiple sales channels (Bol.com, Shopify) is:
- **Time-consuming** — hours spent copying data between platforms
- **Error-prone** — manual transcription introduces mistakes
- **Unscalable** — adding new channels multiplies the manual work
- **Delayed** — insights are always outdated by the time data is compiled

## 💡 Solution

Mvolo automates the entire data pipeline:

```
Bol.com API ──┐                                    ┌──→ Metabase Dashboards
              ├──→ Extract → Load → Transform ─────┤
Shopify API ──┘         (Python)    (dbt)          └──→ Google Sheets (Weekly)
```

---

## 🏛️ Architecture Overview

![Architecture Diagram](docs/diagrams/etl_flow.png)

| Layer | Technology | Description |
|-------|-----------|-------------|
| **Extract** | Python (`requests`) | Pulls data from Bol.com & Shopify REST APIs |
| **Load** | Python + DuckDB/PostgreSQL | Loads raw data into the data warehouse |
| **Transform** | dbt (data build tool) | Cleans, joins, and models data into analytics-ready tables |
| **Export** | Python + Google Sheets API | Pushes weekly summaries to Google Sheets every Monday |
| **Orchestrate** | N8N | Schedules and coordinates the pipeline |
| **Visualize** | Metabase | Interactive dashboards for business insights |
| **Containerize** | Docker | Runs the entire stack in isolated containers |

---

## 📂 Project Structure

```
mvolo/
├── extract/          # API clients for Bol.com & Shopify
├── load/             # Data warehouse loading logic
├── export/           # Google Sheets weekly export
├── transform/        # dbt models (staging → intermediate → marts)
├── orchestration/    # N8N workflow definitions
├── docker/           # Docker configurations
├── dashboards/       # Metabase dashboard documentation
├── scripts/          # Utility & setup scripts
├── tests/            # Python unit tests
├── docs/             # Architecture & API documentation
├── config/           # Pipeline configuration files
└── data/             # Raw (gitignored) & sample data
```

> Each directory contains its own `README.md` with detailed implementation instructions.

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Bol.com API credentials (Client ID + Secret)
- Shopify Admin API access token
- Google Cloud Service Account (for Sheets export)

### 1. Clone & Setup

```bash
git clone https://github.com/your-username/mvolo.git
cd mvolo
cp .env.example .env
# Edit .env with your API credentials
```

### 2. Start Infrastructure

```bash
docker-compose up -d
```

This starts PostgreSQL, N8N, and Metabase.

### 3. Install Python Dependencies

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 4. Initialize the Warehouse

```bash
python scripts/setup_warehouse.py
```

### 5. Run the Pipeline

```bash
python scripts/run_pipeline.py
```

Or use the Makefile:

```bash
make pipeline    # Full E-L-T pipeline
make extract     # Extraction only
make transform   # dbt transformations only
make export      # Google Sheets export only
```

---

## 📅 Pipeline Schedule

| Schedule | Pipeline | Description |
|----------|----------|-------------|
| **Daily** (6:00 AM) | Full ETL | Extract → Load → Transform → Warehouse |
| **Monday** (7:00 AM) | Weekly Report | Query last week's data → Push to Google Sheets |

Schedules are managed via N8N. See [`orchestration/README.md`](orchestration/README.md).

---

## 🛠️ Tech Stack

| Tool | Role | Why |
|------|------|-----|
| **Python** | Extract & Load | Rich ecosystem for API integration |
| **dbt** | Transform | Version-controlled SQL transformations |
| **DuckDB** | Warehouse (POC) | Embedded, fast, zero-config |
| **PostgreSQL** | Warehouse (Prod) | Battle-tested, scalable |
| **N8N** | Orchestration | Visual workflows, easy scheduling |
| **Metabase** | Visualization | Open-source BI, beautiful dashboards |
| **Google Sheets** | Reporting | Familiar interface for stakeholders |
| **Docker** | Infrastructure | Reproducible, isolated environments |

> All tools are **open-source** — zero licensing costs.

---

## 🧪 Testing

```bash
# Python tests
pytest tests/

# dbt data quality tests
cd transform && dbt test
```

See [`tests/README.md`](tests/README.md) for details.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 👤 Author

**Dreu** — Intern Data Engineer

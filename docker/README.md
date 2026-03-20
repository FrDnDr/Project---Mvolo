# 📂 docker/ — Docker Infrastructure

> Containerizes the entire Mvolo stack — **PostgreSQL**, **N8N**, and **Metabase** — for reproducible, isolated deployments.

---

## 🎯 Purpose

Docker ensures:
- **Reproducibility** — Same environment on every machine
- **Isolation** — Pipeline services don't interfere with the host system
- **One-command setup** — `docker-compose up -d` starts everything
- **Proof of concept safety** — Test freely without affecting production systems

---

## 📁 Files

| File | Purpose |
|------|---------|
| `postgres/init.sql` | Initial database schema, roles, and permissions |
| `metabase/` | Metabase persistent data (auto-generated, gitignored) |
| `n8n/Dockerfile` | Custom N8N image with Python pre-installed (if needed) |

---

## 🔧 Implementation Plan

### `docker-compose.yml` (Root Directory)

```yaml
version: "3.8"

services:
  # ── DATA WAREHOUSE ──
  postgres:
    image: postgres:16-alpine
    container_name: mvolo-postgres
    restart: unless-stopped
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docker/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ── PIPELINE ORCHESTRATOR ──
  n8n:
    image: n8nio/n8n:latest
    container_name: mvolo-n8n
    restart: unless-stopped
    ports:
      - "5678:5678"
    environment:
      - N8N_BASIC_AUTH_ACTIVE=true
      - N8N_BASIC_AUTH_USER=admin
      - N8N_BASIC_AUTH_PASSWORD=${N8N_PASSWORD}
      - GENERIC_TIMEZONE=Europe/Amsterdam
    volumes:
      - n8n_data:/home/node/.n8n
      - ./:/app  # Mount project for running Python scripts
    depends_on:
      postgres:
        condition: service_healthy

  # ── BI / VISUALIZATION ──
  metabase:
    image: metabase/metabase:latest
    container_name: mvolo-metabase
    restart: unless-stopped
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: ${POSTGRES_DB}
      MB_DB_PORT: 5432
      MB_DB_USER: ${POSTGRES_USER}
      MB_DB_PASS: ${POSTGRES_PASSWORD}
      MB_DB_HOST: postgres
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  postgres_data:
  n8n_data:
```

### `postgres/init.sql` — Initial Database Setup

```sql
-- Create schemas for the ELT layers
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS snapshots;

-- Create read-only role for Metabase
CREATE ROLE metabase_reader WITH LOGIN PASSWORD 'metabase_readonly';
GRANT USAGE ON SCHEMA core, analytics TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA core, analytics TO metabase_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA core, analytics
    GRANT SELECT ON TABLES TO metabase_reader;

-- Grant full access to the pipeline user
GRANT ALL PRIVILEGES ON ALL SCHEMAS TO mvolo_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw, staging, intermediate, core, analytics, snapshots TO mvolo_user;
```

### `n8n/Dockerfile` — Custom N8N Image (Optional)

Only needed if N8N needs Python pre-installed to run scripts directly:

```dockerfile
FROM n8nio/n8n:latest

# Install Python (for running pipeline scripts inside N8N)
USER root
RUN apk add --no-cache python3 py3-pip
RUN pip3 install --no-cache-dir requests duckdb psycopg2-binary gspread google-auth pandas

USER node
```

> **Alternative:** Instead of installing Python in the N8N container, N8N can call Python scripts on the host via Docker socket or a separate Python container. Choose based on your comfort level.

---

## 🏃 Running the Stack

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f postgres
docker-compose logs -f n8n
docker-compose logs -f metabase

# Stop all services
docker-compose down

# Stop and remove all data (⚠️ destructive)
docker-compose down -v
```

### Accessing Services

| Service | URL | Default Credentials |
|---------|-----|-------------------|
| **PostgreSQL** | `localhost:5432` | User/pass from `.env` |
| **N8N** | `http://localhost:5678` | admin / (from `.env`) |
| **Metabase** | `http://localhost:3000` | Setup wizard on first run |

---

## ⚠️ Key Considerations

1. **Data persistence** — Named volumes (`postgres_data`, `n8n_data`) survive container restarts. Only `docker-compose down -v` deletes them
2. **Networking** — All services share a Docker network; reference by service name (e.g., `postgres` not `localhost`)
3. **Health checks** — PostgreSQL has a health check; N8N and Metabase wait for it before starting
4. **Timezone** — Set `GENERIC_TIMEZONE` in N8N to match your business timezone (important for scheduling)
5. **Security** — Change all default passwords in `.env` before any shared deployment

# Mvolo — Data Leakage Prevention (Mermaid Charts)

> Render these at [mermaid.live](https://mermaid.live) or paste into any Mermaid-compatible tool.

---

## Chart 1: Data Flow & Leakage Points

```mermaid
flowchart LR
    subgraph Sources["🌐 Data Sources"]
        BOL["Bol.com API"]
        SHOP["Shopify API"]
    end

    subgraph Extract["⚙️ Extract Layer"]
        EXT["Python Extractors"]
    end

    subgraph Storage["💾 Storage"]
        RAW["data/raw/ Files"]
        DB["PostgreSQL"]
    end

    subgraph Transform["🔄 Transform Layer"]
        DBT["dbt Models"]
        MARTS["Marts"]
    end

    subgraph Output["📤 Output Layer"]
        META["Metabase"]
        SHEETS["Google Sheets"]
    end

    BOL -->|"HTTPS + OAuth"| EXT
    SHOP -->|"HTTPS + Token"| EXT
    EXT -->|"JSON dump"| RAW
    RAW -->|"Batch load"| DB
    DB -->|"SQL transform"| DBT
    DBT --> MARTS
    MARTS --> META
    MARTS --> SHEETS

    subgraph Leak1["⚠️ Leak: Terminal Logs"]
        L1["API tokens printed"]
    end
    EXT -.->|"risk"| Leak1

    subgraph Leak2["⚠️ Leak: Raw Files"]
        L2["Full PII on disk"]
    end
    RAW -.->|"risk"| Leak2

    subgraph Leak3["⚠️ Leak: Database"]
        L3["PII in raw schema"]
    end
    DB -.->|"risk"| Leak3

    subgraph Leak4["⚠️ Leak: Dashboards"]
        L4["Shared publicly"]
    end
    META -.->|"risk"| Leak4

    subgraph Leak5["⚠️ Leak: Sheets"]
        L5["Link-shared PII"]
    end
    SHEETS -.->|"risk"| Leak5

    subgraph Leak6["⚠️ Leak: Git"]
        L6[".env committed"]
    end
    EXT -.->|"risk"| Leak6

    style Leak1 fill:#ff4444,color:#fff
    style Leak2 fill:#ff4444,color:#fff
    style Leak3 fill:#ff4444,color:#fff
    style Leak4 fill:#ff4444,color:#fff
    style Leak5 fill:#ff4444,color:#fff
    style Leak6 fill:#ff4444,color:#fff
    style Sources fill:#4a90d9,color:#fff
    style Extract fill:#f5a623,color:#fff
    style Storage fill:#7b68ee,color:#fff
    style Transform fill:#50c878,color:#fff
    style Output fill:#e84393,color:#fff
```

---

## Chart 2: Leakage Vectors — Current & Future

```mermaid
flowchart TB
    subgraph Current["🔴 Current Leakage Vectors"]
        direction TB
        C1["🗂️ Git Repository<br/>.env, raw JSON committed"]
        C2["📝 Terminal Logs<br/>API tokens, PII in output"]
        C3["📊 Google Sheets<br/>PII in link-shared reports"]
        C4["📈 Metabase<br/>Raw PII in dashboard queries"]
        C5["📁 Raw Data Files<br/>Unencrypted JSON on disk"]
        C6["🐳 Docker Volumes<br/>DB data on unencrypted volume"]
        C7["🔄 N8N Workflows<br/>Credentials in exported JSON"]
    end

    subgraph Future["🟡 Future Leakage Vectors"]
        direction TB
        F1["👥 New Team Members<br/>Over-privileged DB access"]
        F2["☁️ Cloud Migration<br/>Public buckets, exposed endpoints"]
        F3["🔗 New Data Sources<br/>Unknown PII in new APIs"]
        F4["🔄 CI/CD Pipelines<br/>Secrets in GitHub Actions logs"]
        F5["📦 Database Backups<br/>Unencrypted dumps"]
        F6["📈 Log Aggregation<br/>PII shipped to ELK/Datadog"]
    end

    subgraph Mitigations["🛡️ Prevention Measures"]
        direction TB
        M1["gitleaks + .gitignore"]
        M2["Structured logging + masking"]
        M3["PII stripping + restricted sharing"]
        M4["RBAC + column permissions"]
        M5["90-day retention + encryption"]
        M6["Encrypted volumes"]
        M7["Sanitize before commit"]
        M8["Least-privilege roles"]
        M9["VPC + IAM + encrypted storage"]
        M10["Extend PII classification"]
        M11["GitHub Secrets, no echo"]
        M12["Encrypt dumps, auto-expire"]
        M13["PII redaction filters"]
    end

    C1 --> M1
    C2 --> M2
    C3 --> M3
    C4 --> M4
    C5 --> M5
    C6 --> M6
    C7 --> M7
    F1 --> M8
    F2 --> M9
    F3 --> M10
    F4 --> M11
    F5 --> M12
    F6 --> M13

    style Current fill:#ffcccc,color:#000
    style Future fill:#fff3cd,color:#000
    style Mitigations fill:#d4edda,color:#000
```

---

## Chart 3: Defense Layers

```mermaid
flowchart TB
    subgraph Layer1["🔐 Layer 1: Source Protection"]
        A1["Pre-commit hooks<br/>gitleaks scanning"]
        A2[".gitignore<br/>.env, raw data, logs"]
        A3["API key rotation<br/>every 90 days"]
    end

    subgraph Layer2["🛡️ Layer 2: Pipeline Protection"]
        B1["Secret masking<br/>in all log output"]
        B2["Schema validation<br/>Pydantic on every response"]
        B3["Transactions<br/>atomic loads, rollback on fail"]
    end

    subgraph Layer3["🔒 Layer 3: Storage Protection"]
        C1["PII hashing<br/>SHA-256 at staging layer"]
        C2["Encrypted volumes<br/>PostgreSQL TDE"]
        C3["Data retention<br/>auto-delete after 90 days"]
    end

    subgraph Layer4["👁️ Layer 4: Access Protection"]
        D1["Metabase RBAC<br/>column-level permissions"]
        D2["Sheets restrictions<br/>specific emails only"]
        D3["Docker localhost<br/>127.0.0.1 binding"]
    end

    subgraph Layer5["📋 Layer 5: Monitoring"]
        E1["Structured logging<br/>JSON format, rotation"]
        E2["Pipeline metadata<br/>run status tracking"]
        E3["dbt freshness<br/>warn 24h, error 48h"]
    end

    Layer1 --> Layer2 --> Layer3 --> Layer4 --> Layer5

    style Layer1 fill:#e74c3c,color:#fff
    style Layer2 fill:#e67e22,color:#fff
    style Layer3 fill:#f1c40f,color:#000
    style Layer4 fill:#2ecc71,color:#fff
    style Layer5 fill:#3498db,color:#fff
```

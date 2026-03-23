"""
DuckDB Setup & Test Script
Tests local DuckDB database for Mvolo ETL development
"""

import duckdb
import os
from pathlib import Path

# ── Create data directory if it doesn't exist ──
DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

# ── DuckDB database path ──
DB_PATH = DATA_DIR / "mvolo.duckdb"

print("=" * 60)
print("🦆 DuckDB Setup & Test")
print("=" * 60)

# ── Connect to DuckDB ──
print(f"\n✓ Connecting to DuckDB: {DB_PATH}")
conn = duckdb.connect(str(DB_PATH))
print(f"✓ DuckDB version: {duckdb.__version__}")

# ── Create schemas ──
print("\n📁 Creating schemas...")
schemas = ["raw", "staging", "intermediate", "marts", "pipeline"]
for schema in schemas:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"   ✓ {schema}")

# ── Create pipeline log table ──
print("\n📋 Creating pipeline.run_log table...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS pipeline.run_log (
        run_id          INTEGER,
        run_started_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        run_finished_at TIMESTAMP,
        stage           VARCHAR NOT NULL,
        source          VARCHAR,
        status          VARCHAR NOT NULL DEFAULT 'running',
        rows_processed  INTEGER DEFAULT 0,
        error_message   VARCHAR,
        checksum        VARCHAR
    )
""")
print("   ✓ pipeline.run_log created")

# ── Test insert ──
print("\n✍️  Inserting test record...")
conn.execute("""
    INSERT INTO pipeline.run_log (run_id, stage, source, status, rows_processed)
    VALUES (1, 'extract', 'shopify', 'completed', 150)
""")
print("   ✓ Test record inserted")

# ── Verify data ──
print("\n🔍 Verifying data...")
result = conn.execute("SELECT * FROM pipeline.run_log").fetchall()
print(f"   ✓ Found {len(result)} records")
for row in result:
    print(f"      Run ID: {row[0]}, Stage: {row[3]}, Status: {row[5]}")

# ── List all tables ──
print("\n📊 All tables created:")
tables = conn.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'system', 'pg_catalog')
    ORDER BY table_schema, table_name
""").fetchall()

for schema_name, table_name in tables:
    print(f"   ✓ {schema_name}.{table_name}")

# ── Test sample data ──
print("\n📦 Creating sample raw data...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.shopify_orders (
        order_id       INTEGER,
        customer_id    INTEGER,
        order_date     DATE,
        total_amount   DECIMAL(10, 2),
        currency       VARCHAR,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# Insert sample records
conn.execute("""
    INSERT INTO raw.shopify_orders 
    VALUES 
        (1001, 101, '2026-03-20', 150.50, 'USD', CURRENT_TIMESTAMP),
        (1002, 102, '2026-03-21', 225.00, 'USD', CURRENT_TIMESTAMP),
        (1003, 103, '2026-03-22', 89.99, 'USD', CURRENT_TIMESTAMP)
""")

sample_data = conn.execute("SELECT COUNT(*) as total FROM raw.shopify_orders").fetchone()
print(f"   ✓ Inserted {sample_data[0]} sample orders")

# ── Display sample data ──
print("\n📋 Sample orders:")
orders = conn.execute("""
    SELECT order_id, customer_id, order_date, total_amount 
    FROM raw.shopify_orders
""").fetchall()
for order in orders:
    print(f"   Order {order[0]}: Customer {order[1]}, Date {order[2]}, ${order[3]}")

# ── Test SQL query ──
print("\n💰 Test query - Total revenue:")
revenue = conn.execute("SELECT SUM(total_amount) as total_revenue FROM raw.shopify_orders").fetchone()
print(f"   ✓ Total Revenue: ${revenue[0]:.2f}")

# ── Close connection ──
conn.close()

print("\n" + "=" * 60)
print("✅ DuckDB Setup Complete!")
print("=" * 60)
print("\n📝 Database file location:")
print(f"   {DB_PATH}")
print("\n🚀 Next steps:")
print("   1. Use this database for local ETL testing")
print("   2. Connect dbt to DuckDB for transformations")
print("   3. Export data to PostgreSQL when ready for production")
print("\n")

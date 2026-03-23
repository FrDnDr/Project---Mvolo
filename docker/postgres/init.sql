-- ============================================
-- Mvolo — Database Initialization
-- ============================================
-- This runs automatically when PostgreSQL starts
-- for the first time.

-- ── Create schemas ──
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS pipeline;

-- ── Create Metabase database ──
CREATE DATABASE metabase;

-- ── Pipeline run log table ──
CREATE TABLE IF NOT EXISTS pipeline.run_log (
    run_id          SERIAL PRIMARY KEY,
    run_started_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    run_finished_at TIMESTAMP,
    stage           VARCHAR(50) NOT NULL,
    source          VARCHAR(50),
    status          VARCHAR(20) NOT NULL DEFAULT 'running',
    rows_processed  INTEGER DEFAULT 0,
    error_message   TEXT,
    checksum        VARCHAR(64)
);

-- ── Create read-only roles (from ACCESS_CONTROL.md) ──

-- CEO: marts + pipeline (read-only)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ceo_reader') THEN
        CREATE ROLE ceo_reader WITH LOGIN PASSWORD 'change_me_ceo';
    END IF;
END $$;

GRANT USAGE ON SCHEMA marts TO ceo_reader;
GRANT USAGE ON SCHEMA pipeline TO ceo_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO ceo_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA pipeline GRANT SELECT ON TABLES TO ceo_reader;

-- Data Team: marts only (read-only)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'data_analyst') THEN
        CREATE ROLE data_analyst WITH LOGIN PASSWORD 'change_me_data';
    END IF;
END $$;

GRANT USAGE ON SCHEMA marts TO data_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO data_analyst;

-- Finance: marts only, limited tables (read-only)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'finance_reader') THEN
        CREATE ROLE finance_reader WITH LOGIN PASSWORD 'change_me_finance';
    END IF;
END $$;

GRANT USAGE ON SCHEMA marts TO finance_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT SELECT ON TABLES TO finance_reader;

-- ── Lock down sensitive schemas ──
REVOKE ALL ON SCHEMA raw FROM PUBLIC;
REVOKE ALL ON SCHEMA staging FROM PUBLIC;
REVOKE ALL ON SCHEMA intermediate FROM PUBLIC;

-- ✅ Database initialized!

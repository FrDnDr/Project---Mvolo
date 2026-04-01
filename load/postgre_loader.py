"""
PostgreSQL Loader — loads extracted data into a PostgreSQL warehouse.

This is a scaffold for transitioning from DuckDB to a server-based PostgreSQL.
Requires: pip install psycopg2-binary
"""

import os
import logging
from typing import List, Dict

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None

from dotenv import load_dotenv

logger = logging.getLogger("mvolo.load.postgre")

# ── SQL: Schema & Table Creation (PostgreSQL Dialect) ────────────────

CREATE_SCHEMAS_SQL = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS analytics;
"""

CREATE_BOL_ORDERS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.bol_orders (
        order_id            TEXT NOT NULL,
        order_item_id       TEXT NOT NULL,
        order_placed_at     TIMESTAMPTZ,
        fulfillment_method  TEXT,
        ean                 TEXT,
        title               TEXT,
        quantity            INTEGER,
        unit_price          NUMERIC(10,2),
        total_price         NUMERIC(10,2),
        commission          NUMERIC(10,2),
        fetched_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (order_id, order_item_id)
    );
"""

CREATE_BOL_OFFERS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.bol_offers (
        offer_id            TEXT PRIMARY KEY,
        ean                 TEXT,
        product_title       TEXT,
        stock_amount        INTEGER,
        corrected_stock     INTEGER,
        managed_by_retailer BOOLEAN,
        unit_price          NUMERIC(10,2),
        fulfillment_method  TEXT,
        delivery_code       TEXT,
        fetched_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
"""

CREATE_BOL_PRODUCT_COSTS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.bol_product_costs (
        ean                 TEXT PRIMARY KEY,
        product_name        TEXT,
        cogs                NUMERIC(10,2) DEFAULT 0.00,
        estimated_ad_cost   NUMERIC(10,2) DEFAULT 0.00,
        updated_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
"""

CREATE_PROFITABILITY_VIEW_SQL = """
    CREATE OR REPLACE VIEW analytics.bol_profitability AS
    SELECT
        -- From Orders (API)
        o.order_id,
        o.order_item_id,
        o.order_placed_at                                           AS date,
        o.title                                                     AS product_name,
        o.ean,
        o.quantity                                                  AS units_sold,
        o.unit_price                                                AS selling_price,
        o.commission,

        -- From Offers (API) — joined on EAN
        off.unit_price                                              AS original_price,

        -- From Manual Costs — joined on EAN
        COALESCE(c.cogs, 0)                                        AS cogs,
        COALESCE(c.estimated_ad_cost, 0)                           AS estimated_ad_cost,

        -- Calculated Fields
        COALESCE(c.cogs, 0) * o.quantity                           AS cogs_total,
        ROUND(o.unit_price / 1.21, 2)                              AS net_selling_price,
        CASE
            WHEN off.unit_price IS NOT NULL AND off.unit_price > 0
            THEN ROUND((off.unit_price - o.unit_price) / off.unit_price * 100, 1)
            ELSE 0
        END                                                         AS discount_used_pct,
        ROUND(o.unit_price / 1.21 - COALESCE(c.cogs, 0)
              - COALESCE(c.estimated_ad_cost, 0), 2)               AS net_margin_eur,
        CASE
            WHEN o.unit_price > 0
            THEN ROUND((o.unit_price / 1.21 - COALESCE(c.cogs, 0)
                  - COALESCE(c.estimated_ad_cost, 0))
                  / (o.unit_price / 1.21) * 100, 1)
            ELSE 0
        END                                                         AS net_margin_pct,
        o.unit_price * o.quantity                                   AS revenue,
        ROUND((o.unit_price / 1.21 - COALESCE(c.cogs, 0)
              - COALESCE(c.estimated_ad_cost, 0))
              * o.quantity, 2)                                      AS profit

    FROM raw.bol_orders o
    LEFT JOIN raw.bol_offers off ON o.ean = off.ean
    LEFT JOIN raw.bol_product_costs c ON o.ean = c.ean;
"""

class PostgreLoader:
    """
    Loads extracted data into a remote/local PostgreSQL database.
    """

    def __init__(self, connection_dsn: str = None):
        """
        Connect to PostgreSQL via DSN (Data Source Name).
        Example DSN: "dbname=mvolo user=postgres password=secret host=localhost port=5432"
        """
        if not psycopg2:
            raise ImportError("psycopg2-binary not installed. Run 'pip install psycopg2-binary'")

        if not connection_dsn:
            load_dotenv()
            connection_dsn = os.getenv("POSTGRES_DSN")

        self.dsn = connection_dsn
        self._conn = None
        self.connect()

    def connect(self):
        """Estimate connection and return it."""
        try:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = True
            logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def setup_schema(self):
        """Initialize the database schema and views."""
        with self._conn.cursor() as cur:
            cur.execute(CREATE_SCHEMAS_SQL)
            cur.execute(CREATE_BOL_ORDERS_SQL)
            cur.execute(CREATE_BOL_OFFERS_SQL)
            cur.execute(CREATE_BOL_PRODUCT_COSTS_SQL)
            cur.execute(CREATE_PROFITABILITY_VIEW_SQL)
        logger.info("PostgreSQL Schema setup complete ✓")

    def load_orders(self, orders: List[Dict]):
        """Upsert orders using ON CONFLICT (order_id, order_item_id)."""
        if not orders: return 0
        
        sql = """
            INSERT INTO raw.bol_orders 
                (order_id, order_item_id, order_placed_at, fulfillment_method, ean, title, quantity, unit_price, total_price, commission)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, order_item_id) DO UPDATE SET
                order_placed_at = EXCLUDED.order_placed_at,
                fulfillment_method = EXCLUDED.fulfillment_method,
                ean = EXCLUDED.ean,
                title = EXCLUDED.title,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_price = EXCLUDED.total_price,
                commission = EXCLUDED.commission,
                fetched_at = CURRENT_TIMESTAMP;
        """
        
        count = 0
        with self._conn.cursor() as cur:
            for row in orders:
                cur.execute(sql, (
                    row.get("order_id"), row.get("order_item_id"), row.get("order_placed_at"),
                    row.get("fulfillment_method"), row.get("ean"), row.get("title"),
                    row.get("quantity"), row.get("unit_price"), row.get("total_price"),
                    row.get("commission")
                ))
                count += 1
        return count

    def load_offers(self, offers: List[Dict]):
        """Upsert offers using ON CONFLICT (offer_id)."""
        if not offers: return 0

        sql = """
            INSERT INTO raw.bol_offers
                (offer_id, ean, product_title, stock_amount, corrected_stock, managed_by_retailer, unit_price, fulfillment_method, delivery_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (offer_id) DO UPDATE SET
                ean = EXCLUDED.ean,
                product_title = EXCLUDED.product_title,
                stock_amount = EXCLUDED.stock_amount,
                corrected_stock = EXCLUDED.corrected_stock,
                managed_by_retailer = EXCLUDED.managed_by_retailer,
                unit_price = EXCLUDED.unit_price,
                fulfillment_method = EXCLUDED.fulfillment_method,
                delivery_code = EXCLUDED.delivery_code,
                fetched_at = CURRENT_TIMESTAMP;
        """

        count = 0
        with self._conn.cursor() as cur:
            for row in offers:
                cur.execute(sql, (
                    row.get("offer_id"), row.get("ean"), row.get("product_title"),
                    row.get("stock_amount"), row.get("corrected_stock"),
                    row.get("managed_by_retailer"), row.get("unit_price"),
                    row.get("fulfillment_method"), row.get("delivery_code")
                ))
                count += 1
        return count

    def close(self):
        if self._conn:
            self._conn.close()
            logger.info("PostgreSQL connection closed")

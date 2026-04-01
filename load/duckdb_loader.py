"""
DuckDB Loader — loads extracted data into the local DuckDB warehouse.

Creates schemas, tables, and the unified profitability view.
Uses INSERT OR REPLACE for idempotent upserts.
"""

import os
import logging
from pathlib import Path

import duckdb
from dotenv import load_dotenv

logger = logging.getLogger("mvolo.load")

# ── SQL: Schema & Table Creation ─────────────────────────────────────

CREATE_SCHEMAS_SQL = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS analytics;
"""

CREATE_BOL_ORDERS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.bol_orders (
        order_id            VARCHAR NOT NULL,
        order_item_id       VARCHAR NOT NULL,
        order_placed_at     VARCHAR,
        fulfillment_method  VARCHAR,
        ean                 VARCHAR,
        title               VARCHAR,
        quantity            INTEGER,
        unit_price          DOUBLE,
        total_price         DOUBLE,
        commission          DOUBLE,
        fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (order_id, order_item_id)
    );
"""

CREATE_BOL_OFFERS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.bol_offers (
        offer_id            VARCHAR PRIMARY KEY,
        ean                 VARCHAR,
        product_title       VARCHAR,
        stock_amount        INTEGER,
        corrected_stock     INTEGER,
        managed_by_retailer BOOLEAN,
        unit_price          DOUBLE,
        fulfillment_method  VARCHAR,
        delivery_code       VARCHAR,
        fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""

CREATE_BOL_PRODUCT_COSTS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.bol_product_costs (
        ean                 VARCHAR PRIMARY KEY,
        product_name        VARCHAR,
        original_price      DOUBLE DEFAULT 0.0,
        cogs                DOUBLE DEFAULT 0.0,
        estimated_ad_cost   DOUBLE DEFAULT 0.0,
        updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        -- Manual original_price takes priority over the Offers API price
        COALESCE(NULLIF(c.original_price, 0), off.unit_price)   AS original_price,

        -- From Manual Costs — joined on EAN
        COALESCE(c.cogs, 0)                                        AS cogs,
        COALESCE(c.estimated_ad_cost, 0)                           AS estimated_ad_cost,

        -- Calculated Fields
        COALESCE(c.cogs, 0) * o.quantity                           AS cogs_total,
        ROUND(o.unit_price / 1.21, 2)                              AS net_selling_price,
        CASE
            WHEN COALESCE(NULLIF(c.original_price, 0), off.unit_price) IS NOT NULL 
                 AND COALESCE(NULLIF(c.original_price, 0), off.unit_price) > 0
            THEN ROUND((COALESCE(NULLIF(c.original_price, 0), off.unit_price) - o.unit_price) 
                       / COALESCE(NULLIF(c.original_price, 0), off.unit_price) * 100, 1)
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


class DuckDBLoader:
    """
    Loads extracted data into the local DuckDB warehouse.
    
    Usage:
        loader = DuckDBLoader()
        loader.setup_schema()
        loader.load_orders(order_items)
        loader.load_offers(offers)
        loader.close()
    """

    def __init__(self, db_path: str = None):
        """
        Connect to DuckDB at the specified path (or from .env DUCKDB_PATH).
        """
        if not db_path:
            env_path = Path(__file__).parent.parent / ".env"
            load_dotenv(dotenv_path=env_path)
            db_path = os.getenv("DUCKDB_PATH", "./data/mvolo.duckdb")

        # Resolve relative paths from project root
        self._db_path = Path(__file__).parent.parent / db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Connecting to DuckDB at {self._db_path}")
        self._con = duckdb.connect(str(self._db_path))

    def setup_schema(self):
        """Creates all schemas, tables, and views if they don't exist."""
        logger.info("Setting up DuckDB schema...")
        self._con.execute(CREATE_SCHEMAS_SQL)
        self._con.execute(CREATE_BOL_ORDERS_SQL)
        self._con.execute(CREATE_BOL_OFFERS_SQL)
        self._con.execute(CREATE_BOL_PRODUCT_COSTS_SQL)

        # Migration: add original_price column if table was created before this update
        try:
            self._con.execute("ALTER TABLE raw.bol_product_costs ADD COLUMN original_price DOUBLE DEFAULT 0.0")
        except Exception:
            pass  # Column already exists

        self._con.execute(CREATE_PROFITABILITY_VIEW_SQL)
        logger.info("Schema setup complete ✓")

    def load_orders(self, orders: list[dict]) -> int:
        """
        Upsert order items into raw.bol_orders.
        Returns the number of rows loaded.
        """
        if not orders:
            logger.warning("No orders to load")
            return 0

        count = 0
        for row in orders:
            self._con.execute("""
                INSERT OR REPLACE INTO raw.bol_orders 
                    (order_id, order_item_id, order_placed_at, fulfillment_method,
                     ean, title, quantity, unit_price, total_price, commission, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                row.get("order_id"),
                row.get("order_item_id"),
                row.get("order_placed_at"),
                row.get("fulfillment_method"),
                row.get("ean"),
                row.get("title"),
                row.get("quantity"),
                row.get("unit_price"),
                row.get("total_price"),
                row.get("commission"),
            ])
            count += 1

        logger.info(f"Loaded {count} order items into raw.bol_orders ✓")
        return count

    def load_offers(self, offers: list[dict]) -> int:
        """
        Upsert offers into raw.bol_offers.
        Returns the number of rows loaded.
        """
        if not offers:
            logger.warning("No offers to load")
            return 0

        count = 0
        for row in offers:
            self._con.execute("""
                INSERT OR REPLACE INTO raw.bol_offers
                    (offer_id, ean, product_title, stock_amount, corrected_stock,
                     managed_by_retailer, unit_price, fulfillment_method,
                     delivery_code, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                row.get("offer_id"),
                row.get("ean"),
                row.get("product_title"),
                row.get("stock_amount"),
                row.get("corrected_stock"),
                row.get("managed_by_retailer"),
                row.get("unit_price"),
                row.get("fulfillment_method"),
                row.get("delivery_code"),
            ])
            count += 1

        logger.info(f"Loaded {count} offers into raw.bol_offers ✓")
        return count

    def load_product_costs(self, costs: list[dict]) -> int:
        """
        Upsert product cost data (COGS + ad cost) into raw.bol_product_costs.
        Returns the number of rows loaded.
        """
        if not costs:
            logger.warning("No product costs to load")
            return 0

        count = 0
        for row in costs:
            # Fix: Remove COALESCE(?, 0.0) from VALUES so EXCLUDED knows if a value is truly missing (NULL)
            # This allows the DO UPDATE SET part to correctly preserve existing values.
            self._con.execute("""
                INSERT INTO raw.bol_product_costs (ean, product_name, original_price, cogs, estimated_ad_cost, updated_at)
                VALUES (?, ?, ?, ?, ?, now())
                ON CONFLICT (ean) DO UPDATE SET
                    product_name = CASE WHEN EXCLUDED.product_name != '' THEN EXCLUDED.product_name ELSE raw.bol_product_costs.product_name END,
                    original_price = COALESCE(EXCLUDED.original_price, raw.bol_product_costs.original_price),
                    cogs = COALESCE(EXCLUDED.cogs, raw.bol_product_costs.cogs),
                    estimated_ad_cost = COALESCE(EXCLUDED.estimated_ad_cost, raw.bol_product_costs.estimated_ad_cost),
                    updated_at = now()
            """, [
                row.get("ean"),
                row.get("product_name", ""),
                row.get("original_price"),
                row.get("cogs"),
                row.get("estimated_ad_cost"),
            ])
            count += 1

        logger.info(f"Loaded {count} product cost entries into raw.bol_product_costs ✓")
        return count

    def query_profitability(self, limit: int = 100) -> list[tuple]:
        """Query the unified profitability view."""
        result = self._con.execute(f"""
            SELECT * FROM analytics.bol_profitability
            ORDER BY date DESC
            LIMIT {limit}
        """).fetchall()
        return result

    def query_profitability_summary(self) -> list[tuple]:
        """Get per-product profitability summary."""
        result = self._con.execute("""
            SELECT
                product_name,        -- 0
                ean,                 -- 1
                MAX(date)            AS latest_order, -- 2
                SUM(units_sold)      AS total_units,  -- 3
                ROUND(AVG(cogs), 2)  AS avg_cogs,     -- 4
                ROUND(AVG(selling_price), 2) AS avg_selling_price, -- 5
                ROUND(AVG(original_price), 2) AS avg_original_price, -- 6
                ROUND(AVG(discount_used_pct), 1) AS avg_discount_pct, -- 7
                ROUND(AVG(net_margin_eur), 2) AS avg_net_margin, -- 8
                ROUND(AVG(net_margin_pct), 1) AS avg_margin_pct, -- 9
                ROUND(SUM(revenue), 2) AS total_revenue, -- 10
                ROUND(SUM(profit), 2)  AS total_profit -- 11
            FROM analytics.bol_profitability
            GROUP BY product_name, ean
            ORDER BY latest_order DESC
        """).fetchall()
        return result

    def get_table_counts(self) -> dict:
        """Get row counts for all raw tables."""
        counts = {}
        for table in ["raw.bol_orders", "raw.bol_offers", "raw.bol_product_costs"]:
            try:
                result = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = result[0] if result else 0
            except Exception:
                counts[table] = 0
        return counts

    def export_profitability_summary(self, output_path: str):
        """Export the profitability summary to a CSV file."""
        logger.info(f"Exporting profitability summary to {output_path}...")
        
        # Ensure output directory exists
        out_file = Path(output_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)

        self._con.execute(f"""
            COPY (
                SELECT
                    product_name        AS Product,
                    ean                 AS EAN,
                    MAX(date)           AS Latest_Order,
                    SUM(units_sold)     AS Units_Sold,
                    ROUND(AVG(cogs), 2) AS COGS_Avg,
                    ROUND(AVG(selling_price), 2) AS Sell_Price_Avg,
                    ROUND(AVG(original_price), 2) AS Original_Price_Avg,
                    ROUND(AVG(discount_used_pct), 1) AS Discount_Pct_Avg,
                    ROUND(AVG(net_margin_eur), 2) AS Margin_EUR_Avg,
                    ROUND(AVG(net_margin_pct), 1) AS Margin_PCT_Avg,
                    ROUND(SUM(revenue), 2) AS Total_Revenue,
                    ROUND(SUM(profit), 2)  AS Total_Profit
                FROM analytics.bol_profitability
                GROUP BY product_name, ean
                ORDER BY Latest_Order DESC
            ) TO '{str(out_file).replace('\\', '/')}' (HEADER, DELIMITER ',');
        """)
        logger.info(f"Export complete ✓")
    
    def close(self):
        """Close the DuckDB connection."""
        if self._con:
            self._con.close()
            logger.info("DuckDB connection closed")

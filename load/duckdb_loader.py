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

CREATE_SHOPIFY_ORDERS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.shopify_orders (
        order_id            VARCHAR PRIMARY KEY,
        created_at          TIMESTAMP WITH TIME ZONE,
        total_price         DOUBLE,
        subtotal_price      DOUBLE,
        total_tax           DOUBLE,
        currency            VARCHAR,
        landing_site        VARCHAR,
        fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""

CREATE_SHOPIFY_ORDER_ITEMS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.shopify_order_items (
        order_id            VARCHAR,
        line_item_id        VARCHAR PRIMARY KEY,
        sku                 VARCHAR,
        name                VARCHAR,
        quantity            INTEGER,
        price               DOUBLE,
        total_discount      DOUBLE,
        fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (order_id) REFERENCES raw.shopify_orders(order_id)
    );
"""

CREATE_SHOPIFY_PRODUCT_COSTS_SQL = """
    CREATE TABLE IF NOT EXISTS raw.shopify_product_costs (
        sku                 VARCHAR PRIMARY KEY,
        product_name        VARCHAR,
        original_price      DOUBLE DEFAULT 0.0,
        cogs                DOUBLE DEFAULT 0.0,
        estimated_ad_cost   DOUBLE DEFAULT 0.0,
        updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""

CREATE_BOL_PROFITABILITY_VIEW_SQL = """
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
        o.commission                                                AS bol_commission,

        -- From Offers (API) — joined on EAN
        COALESCE(NULLIF(c.original_price, 0), off.unit_price)       AS original_price,

        -- From Manual Costs — joined on EAN
        COALESCE(c.cogs, 0)                                         AS cogs,
        COALESCE(c.estimated_ad_cost, 0)                            AS estimated_ad_cost,

        -- Calculated Fields
        COALESCE(c.cogs, 0) * o.quantity                            AS cogs_total,
        ROUND(o.unit_price / 1.21, 2)                               AS net_selling_price,
        CASE
            WHEN COALESCE(NULLIF(c.original_price, 0), off.unit_price) > 0
            THEN ROUND((COALESCE(NULLIF(c.original_price, 0), off.unit_price) - o.unit_price) 
                       / COALESCE(NULLIF(c.original_price, 0), off.unit_price) * 100, 1)
            ELSE 0
        END                                                         AS discount_used_pct,
        ROUND(o.unit_price / 1.21 - COALESCE(c.cogs, 0)
              - COALESCE(c.estimated_ad_cost, 0) - o.commission, 2)  AS net_margin_eur,
        CASE
            WHEN o.unit_price > 0
            THEN ROUND((o.unit_price / 1.21 - COALESCE(c.cogs, 0)
                  - COALESCE(c.estimated_ad_cost, 0) - o.commission)
                  / (o.unit_price / 1.21) * 100, 1)
            ELSE 0
        END                                                         AS net_margin_pct,
        o.unit_price * o.quantity                                   AS revenue,
        ROUND((o.unit_price / 1.21 - COALESCE(c.cogs, 0)
              - COALESCE(c.estimated_ad_cost, 0) - o.commission)
              * o.quantity, 2)                                      AS profit
    FROM raw.bol_orders o
    LEFT JOIN raw.bol_offers off ON o.ean = off.ean
    LEFT JOIN raw.bol_product_costs c ON o.ean = c.ean;
"""

CREATE_SHOPIFY_PROFITABILITY_VIEW_SQL = """
    CREATE OR REPLACE VIEW analytics.shopify_profitability AS
    WITH line_item_base AS (
        SELECT 
            o.created_at as date,
            o.landing_site,
            l.sku,
            l.name as product_name,
            l.quantity as units_sold,
            COALESCE(c.original_price, l.price) as original_price,
            (l.price * l.quantity - l.total_discount) / NULLIF(l.quantity, 0) as selling_price,
            COALESCE(c.cogs, 0) as cogs,
            COALESCE(c.estimated_ad_cost, 0) as estimated_ad_cost,
            o.order_id,
            l.line_item_id
        FROM raw.shopify_order_items l
        JOIN raw.shopify_orders o ON l.order_id = o.order_id
        LEFT JOIN raw.shopify_product_costs c ON l.sku = c.sku
    )
    SELECT 
        order_id,
        line_item_id,
        date,
        product_name,
        sku,
        units_sold,
        selling_price,
        CASE WHEN landing_site IS NOT NULL THEN ROUND(selling_price * 0.12, 2) ELSE 0 END as shopify_affiliate_fee,
        original_price,
        cogs,
        estimated_ad_cost,
        cogs * units_sold as cogs_total,
        ROUND(selling_price / 1.21, 2) as net_selling_price,
        ROUND(CASE WHEN original_price > 0 THEN (original_price - selling_price) / original_price ELSE 0 END, 3) * 100 as discount_used_pct,
        ROUND((selling_price / 1.21) - cogs - (CASE WHEN landing_site IS NOT NULL THEN selling_price * 0.12 ELSE 0 END), 2) as net_margin_eur,
        selling_price * units_sold as revenue,
        ROUND(((selling_price / 1.21) - cogs - (CASE WHEN landing_site IS NOT NULL THEN selling_price * 0.12 ELSE 0 END)) * units_sold, 2) as profit
    FROM line_item_base;
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

        # Shopify schema setup with fallback for PK/FK constraint errors
        try:
            self._con.execute(CREATE_SHOPIFY_ORDERS_SQL)
            self._con.execute(CREATE_SHOPIFY_ORDER_ITEMS_SQL)
        except Exception as e:
            if "primary key" in str(e).lower() or "unique constraint" in str(e).lower():
                logger.warning("Re-initializing Shopify tables due to schema mismatch...")
                self._con.execute("DROP TABLE IF EXISTS raw.shopify_order_items CASCADE")
                self._con.execute("DROP TABLE IF EXISTS raw.shopify_orders CASCADE")
                self._con.execute(CREATE_SHOPIFY_ORDERS_SQL)
                self._con.execute(CREATE_SHOPIFY_ORDER_ITEMS_SQL)
            else:
                raise
        
        self._con.execute(CREATE_SHOPIFY_PRODUCT_COSTS_SQL)

        # Migration: add original_price column if table was created before this update
        try:
            self._con.execute("ALTER TABLE raw.bol_product_costs ADD COLUMN original_price DOUBLE DEFAULT 0.0")
        except Exception:
            pass  # Column already exists

        self._con.execute(CREATE_BOL_PROFITABILITY_VIEW_SQL)
        self._con.execute(CREATE_SHOPIFY_PROFITABILITY_VIEW_SQL)
        logger.info("Schema setup complete ✓")

    def load_shopify_orders(self, orders: list[dict]) -> int:
        """Upsert Shopify orders into raw.shopify_orders."""
        if not orders:
            return 0
        
        count = 0
        for row in orders:
            self._con.execute("""
                INSERT OR REPLACE INTO raw.shopify_orders 
                    (order_id, created_at, total_price, subtotal_price, total_tax, currency, landing_site, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                str(row.get("id")),
                row.get("created_at"),
                float(row.get("total_price") or 0),
                float(row.get("subtotal_price") or 0),
                float(row.get("total_tax") or 0),
                row.get("currency"),
                row.get("landing_site")
            ])
            count += 1
        return count

    def load_shopify_items(self, items: list[dict]) -> int:
        """Upsert Shopify line items into raw.shopify_order_items."""
        if not items:
            return 0
        
        count = 0
        for row in items:
            self._con.execute("""
                INSERT OR REPLACE INTO raw.shopify_order_items
                    (order_id, line_item_id, sku, name, quantity, price, total_discount, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                row.get("order_id"),
                row.get("line_item_id"),
                row.get("sku"),
                row.get("name"),
                row.get("quantity"),
                row.get("price"),
                row.get("total_discount")
            ])
            count += 1
        return count

    def load_shopify_product_costs(self, costs: list[dict]) -> int:
        """Upsert product cost data into raw.shopify_product_costs."""
        if not costs:
            return 0

        count = 0
        for row in costs:
            self._con.execute("""
                INSERT INTO raw.shopify_product_costs (sku, product_name, original_price, cogs, estimated_ad_cost, updated_at)
                VALUES (?, ?, ?, ?, ?, now())
                ON CONFLICT (sku) DO UPDATE SET
                    product_name = CASE WHEN EXCLUDED.product_name != '' THEN EXCLUDED.product_name ELSE raw.shopify_product_costs.product_name END,
                    original_price = COALESCE(EXCLUDED.original_price, raw.shopify_product_costs.original_price),
                    cogs = COALESCE(EXCLUDED.cogs, raw.shopify_product_costs.cogs),
                    estimated_ad_cost = COALESCE(EXCLUDED.estimated_ad_cost, raw.shopify_product_costs.estimated_ad_cost),
                    updated_at = now()
            """, [
                row.get("sku"),
                row.get("product_name", ""),
                row.get("original_price"),
                row.get("cogs"),
                row.get("estimated_ad_cost"),
            ])
            count += 1
        return count

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

    def query_profitability(self, platform: str = "bol", limit: int = 100) -> list[tuple]:
        """Query platform-specific profitability view."""
        table = "analytics.bol_profitability" if platform == "bol" else "analytics.shopify_profitability"
        result = self._con.execute(f"""
            SELECT * FROM {table}
            ORDER BY date DESC
            LIMIT {limit}
        """).fetchall()
        return result

    def query_profitability_summary(self, platform: str = "bol") -> list[tuple]:
        """Get per-product profitability summary for a specific platform."""
        if platform == "bol":
            return self._con.execute("""
                SELECT
                    product_name,        -- 0
                    ean,                 -- 1
                    MAX(date)            AS latest_order, -- 2
                    SUM(units_sold)      AS total_units, -- 3
                    ROUND(AVG(cogs), 2)  AS avg_cogs, -- 4
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
        else:
            return self._con.execute("""
                SELECT
                    product_name,        -- 0
                    sku,                 -- 1
                    MAX(date)            AS latest_order, -- 2
                    SUM(units_sold)      AS total_units,  -- 3
                    ROUND(AVG(cogs), 2)  AS avg_cogs,     -- 4
                    ROUND(AVG(selling_price), 2) AS avg_selling_price, -- 5
                    ROUND(AVG(original_price), 2) AS avg_original_price, -- 6
                    ROUND(AVG(discount_used_pct), 1) AS avg_discount_pct, -- 7
                    ROUND(AVG(net_margin_eur), 2) AS avg_net_margin, -- 8
                    ROUND(AVG(net_margin_pct), 1) AS avg_margin_pct, -- 9
                    ROUND(SUM(revenue), 2) AS total_revenue, -- 10
                    ROUND(SUM(profit), 2)  AS total_profit   -- 11
                FROM analytics.shopify_profitability
                GROUP BY product_name, sku
                ORDER BY latest_order DESC
            """).fetchall()

    def get_table_counts(self) -> dict:
        """Get row counts for all raw tables."""
        counts = {}
        for table in ["raw.bol_orders", "raw.bol_offers", "raw.bol_product_costs", 
                      "raw.shopify_orders", "raw.shopify_order_items", "raw.shopify_product_costs"]:
            try:
                result = self._con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = result[0] if result else 0
            except Exception:
                counts[table] = 0
        return counts

    def export_profitability_summary(self, output_path: str, platform: str = "bol"):
        """Export the platform-specific profitability summary to a CSV file."""
        logger.info(f"Exporting {platform} profitability summary to {output_path}...")
        
        # Ensure output directory exists
        out_file = Path(output_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)

        if platform == "bol":
            sql = """
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
            """
        else:
            sql = """
                SELECT
                    product_name        AS Product,
                    sku                 AS SKU,
                    MAX(date)           AS Latest_Order,
                    SUM(units_sold)     AS Units_Sold,
                    ROUND(AVG(cogs), 2) AS COGS_Avg,
                    ROUND(AVG(selling_price), 2) AS Sell_Price_Avg,
                    ROUND(AVG(original_price), 2) AS Original_Price_Avg,
                    ROUND(AVG(discount_used_pct), 1) AS Discount_Pct_Avg,
                    ROUND(AVG(net_margin_eur), 2) AS Margin_EUR_Avg,
                    ROUND(AVG(net_margin_eur / NULLIF(selling_price / 1.21, 0)) * 100, 1) AS Margin_PCT_Avg,
                    ROUND(SUM(revenue), 2) AS Total_Revenue,
                    ROUND(SUM(profit), 2)  AS Total_Profit
                FROM analytics.shopify_profitability
                GROUP BY product_name, sku
                ORDER BY Latest_Order DESC
            """

        self._con.execute(f"COPY ({sql}) TO '{str(out_file).replace('\\', '/')}' (HEADER, DELIMITER ',');")
        logger.info(f"Export complete ✓")
    
    def close(self):
        """Close the DuckDB connection."""
        if self._con:
            self._con.close()
            logger.info("DuckDB connection closed")

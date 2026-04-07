#!/usr/bin/env python3
"""
Shopify → DuckDB Pipeline Runner

Extracts orders and line items from the Shopify API and loads them into
the local DuckDB warehouse at data/mvolo.duckdb.

Usage:
    python scripts/run_shopify_to_duckdb.py                  # Last 90 days
    python scripts/run_shopify_to_duckdb.py --view            # Print profitability table
    python scripts/run_shopify_to_duckdb.py --export          # Grouped weekly CSV export
    python scripts/run_shopify_to_duckdb.py --status          # Show row counts
"""

import sys
import argparse
import logging
import csv
from pathlib import Path
from datetime import datetime, date, timedelta

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from extract.utils import setup_logging
from extract.shopify_extractor import ShopifyExtractor
from load.duckdb_loader import DuckDBLoader

def parse_input_date(value: str) -> date:
    """Parse date in YYYY-MM-DD or MM-DD-YYYY format."""
    for fmt in ("%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD.")

def format_human_date_range(start_date, end_date) -> str:
    """Format date range like: January 19-January 25, 2026."""
    start_label = start_date.strftime("%B %#d") if sys.platform == "win32" else start_date.strftime("%B %-d")
    end_label = end_date.strftime("%B %#d, %Y") if sys.platform == "win32" else end_date.strftime("%B %-d, %Y")
    return f"{start_label}-{end_label}"

def run_extraction(args):
    """Run the Shopify extract → load pipeline."""
    setup_logging()
    logger = logging.getLogger("mvolo.scripts.shopify")

    loader = DuckDBLoader()
    loader.setup_schema()

    try:
        extractor = ShopifyExtractor()
        
        # Calculate date range
        if args.start and args.end:
            start_date = parse_input_date(args.start)
            end_date = parse_input_date(args.end)
        else:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=args.days)
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        logger.info(f"Starting Shopify sync from {start_str} to {end_str}...")

        # 1. Extract
        raw_orders = extractor.extract_orders(start_date=start_str, end_date=end_str)
        if not raw_orders:
            logger.info("No orders found in the specified range.")
            return

        # 2. Flatten
        items = extractor.flatten_order_items(raw_orders)

        # 3. Load
        orders_count = loader.load_shopify_orders(raw_orders)
        items_count = loader.load_shopify_items(items)

        # 4. Export for Dashboard
        export_path = PROJECT_ROOT / "visualization" / "data" / "shopify_profitability.csv"
        loader.export_profitability_summary(str(export_path), platform="shopify")

        # Summary
        print("\n" + "=" * 60)
        print("  🛍️  Shopify → DuckDB Pipeline Complete")
        print("=" * 60)
        print(f"  Orders loaded:      {orders_count}")
        print(f"  Line items loaded:  {items_count}")
        print(f"  Summary exported:   {export_path.name}")
        
        counts = loader.get_table_counts()
        print(f"\n  Database totals:")
        for table, count in counts.items():
            if "shopify" in table:
                print(f"    {table}: {count} rows")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        loader.close()

def export_weekly_summary(args):
    """Export profitability summary grouped by week (Monday-Sunday)."""
    setup_logging()
    loader = DuckDBLoader()
    loader.setup_schema()

    try:
        rows = loader._con.execute("""
            SELECT
                DATE_TRUNC('week', CAST(date AS DATE))::DATE                           AS week_start,
                (DATE_TRUNC('week', CAST(date AS DATE))::DATE + INTERVAL 6 DAY)::DATE  AS week_end,
                EXTRACT('week' FROM CAST(date AS DATE))::INTEGER                        AS iso_week,
                product_name,
                sku,
                MAX(CAST(date AS DATE))                                                 AS latest_order,
                SUM(units_sold)                                                         AS total_units,
                ROUND(AVG(cogs), 2)                                                     AS avg_cogs,
                ROUND(AVG(estimated_ad_cost), 2)                                        AS avg_ad_cost,
                ROUND(AVG(selling_price), 2)                                            AS avg_selling_price,
                ROUND(AVG(original_price), 2)                                           AS avg_original_price,
                ROUND(AVG(discount_used_pct), 1)                                        AS avg_discount_pct,
                ROUND(AVG(net_margin_eur), 2)                                           AS avg_net_margin,
                ROUND(AVG(net_margin_eur / NULLIF(selling_price / 1.21, 0)) * 100, 1)  AS avg_margin_pct,
                ROUND(SUM(revenue), 2)                                                  AS total_revenue,
                ROUND(SUM(profit), 2)                                                   AS total_profit
            FROM analytics.shopify_profitability
            GROUP BY 1, 2, 3, 4, 5
            ORDER BY 1 ASC, total_revenue DESC
        """).fetchall()

        if not rows:
            print("\n  ⚠️ No Shopify data found to export.")
            return

        output_dir = PROJECT_ROOT / "visualization" / "shopify-dashboard" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)

        header = ["Week", "Range", "Product", "SKU", "Latest", "Units", "COGS", "Ad", "Sell", "Original", "Disc%", "Margin", "Margin%", "Revenue", "Profit"]
        
        # Group by week and write CSVs
        weeks = {}
        for row in rows:
            key = (row[0], row[1], row[2]) # (start, end, iso_week)
            weeks.setdefault(key, []).append(row)

        for (start, end, iso), data in weeks.items():
            filename = f"shopify_week_{iso:02d}_{start}_to_{end}.csv"
            path = output_dir / filename
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                for r in data:
                    writer.writerow([f"Week {iso}", format_human_date_range(start, end)] + list(r[3:]))
            print(f"    - Created: {filename}")

    finally:
        loader.close()

def show_profitability(args):
    """Print the Shopify profitability summary table."""
    setup_logging()
    loader = DuckDBLoader()
    loader.setup_schema()

    try:
        summary = loader.query_profitability_summary(platform="shopify")

        if not summary:
            print("\n  ⚠️  No Shopify data yet.")
            return

        print("\n" + "=" * 130)
        print("  📊 Shopify Product Profitability Summary")
        print("=" * 130)
        
        print(f"  {'Product':<35} {'SKU':<16} {'Latest':<11} {'Units':>5} {'COGS':>7} {'Sell':>7} "
              f"{'Disc %':>7} {'Margin':>7} {'%':>5} {'Revenue':>10} {'Profit':>10}")
        print("  " + "-" * 128)

        for row in summary:
            print(f"  {(row[0] or 'N/A')[:33]:<35} {(row[1] or 'N/A')[:15]:<16} {str(row[2] or 'N/A')[:10]:<11} "
                  f"{(row[3] or 0):>5} {(row[4] or 0.0):>7.2f} {(row[5] or 0.0):>7.2f} {(row[7] or 0.0):>6.1f}% "
                  f"{(row[8] or 0.0):>7.2f} {(row[9] or 0.0):>4.1f}% {(row[10] or 0.0):>10.2f} {(row[11] or 0.0):>10.2f}")
        print("=" * 130)

    finally:
        loader.close()

def show_status(args):
    """Show Shopify table row counts."""
    setup_logging()
    loader = DuckDBLoader()
    try:
        counts = loader.get_table_counts()
        print("\n  📋 Shopify Database Status")
        print("  " + "-" * 40)
        for table, count in counts.items():
            if "shopify" in table:
                print(f"    {table}: {count} rows")
        print()
    finally:
        loader.close()

def main():
    parser = argparse.ArgumentParser(description="Shopify → DuckDB Pipeline")
    parser.add_argument("--view", action="store_true", help="Print profitability summary")
    parser.add_argument("--export", action="store_true", help="Grouped weekly CSV export")
    parser.add_argument("--status", action="store_true", help="Show table row counts")
    parser.add_argument("--days", type=int, default=90, help="Days lookback (default: 90)")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    
    args = parser.parse_args()

    if args.view:
        show_profitability(args)
    elif args.status:
        show_status(args)
    elif args.export:
        export_weekly_summary(args)
    else:
        run_extraction(args)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Bol.com → DuckDB Pipeline Runner

Extracts orders and offers from the Bol.com API and loads them into
the local DuckDB warehouse at data/mvolo.duckdb.

Usage:
    python scripts/run_bol_to_duckdb.py                     # Last 5 days, orders + offers
    python scripts/run_bol_to_duckdb.py --days 14            # Last 14 days
    python scripts/run_bol_to_duckdb.py --start 2026-03-20 --end 2026-03-27
    python scripts/run_bol_to_duckdb.py --offers-only        # Refresh offers/stock only
    python scripts/run_bol_to_duckdb.py --orders-only        # Refresh orders only
    python scripts/run_bol_to_duckdb.py --view               # Print profitability summary
    python scripts/run_bol_to_duckdb.py --status             # Show table row counts
"""

import sys
import argparse
import csv
from pathlib import Path
from datetime import datetime, date, timedelta

# Add project root to path so we can import extract/ and load/
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from extract.utils import setup_logging
from extract.bol_extractor import BolExtractor
from load.duckdb_loader import DuckDBLoader


def parse_input_date(value: str) -> date:
    """Parse date in YYYY-MM-DD or MM-DD-YYYY format."""
    for fmt in ("%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(
        f"Invalid date '{value}'. Use YYYY-MM-DD or MM-DD-YYYY."
    )


def format_human_date_range(start_date, end_date) -> str:
    """Format date range like: January 19-January 25, 2026."""
    start_label = start_date.strftime("%B %-d") if sys.platform != "win32" else start_date.strftime("%B %#d")
    end_label = end_date.strftime("%B %-d, %Y") if sys.platform != "win32" else end_date.strftime("%B %#d, %Y")
    return f"{start_label}-{end_label}"


def validate_export_date_args(args):
    """Validate --start/--end behavior for export mode and return parsed range."""
    if args.start and not args.end:
        raise ValueError("--start requires --end when used with --export")
    if args.end and not args.start:
        raise ValueError("--end requires --start when used with --export")

    if args.start and args.end:
        start_date = parse_input_date(args.start)
        end_date = parse_input_date(args.end)
    else:
        start_date = datetime(2026, 1, 1).date()
        end_date = datetime.now().date()

    if start_date > end_date:
        raise ValueError("--start must be on or before --end")

    return start_date, end_date


def iso_monday(date_value):
    """Return Monday for the date's ISO week."""
    return date_value - timedelta(days=date_value.weekday())


def export_weekly_summary(args):
    """Export profitability summary as one CSV per Monday-Sunday week."""
    setup_logging()
    start_date, end_date = validate_export_date_args(args)

    loader = DuckDBLoader()
    loader.setup_schema()

    try:
        rows = loader._con.execute(
            """
            SELECT
                DATE_TRUNC('week', CAST(date AS DATE))::DATE                           AS week_start,
                (DATE_TRUNC('week', CAST(date AS DATE))::DATE + INTERVAL 6 DAY)::DATE  AS week_end,
                EXTRACT('week' FROM CAST(date AS DATE))::INTEGER                        AS iso_week,
                product_name,
                ean,
                MAX(CAST(date AS DATE))                                                 AS latest_order,
                SUM(units_sold)                                                         AS total_units,
                ROUND(AVG(cogs), 2)                                                     AS avg_cogs,
                ROUND(AVG(estimated_ad_cost), 2)                                        AS avg_ad_cost,
                ROUND(AVG(selling_price), 2)                                            AS avg_selling_price,
                ROUND(AVG(original_price), 2)                                           AS avg_original_price,
                ROUND(AVG(discount_used_pct), 1)                                        AS avg_discount_pct,
                ROUND(AVG(net_margin_eur), 2)                                           AS avg_net_margin,
                ROUND(AVG(net_margin_pct), 1)                                           AS avg_margin_pct,
                ROUND(SUM(revenue), 2)                                                  AS total_revenue,
                ROUND(SUM(profit), 2)                                                   AS total_profit
            FROM analytics.bol_profitability
            WHERE CAST(date AS DATE) BETWEEN ? AND ?
            GROUP BY 1, 2, 3, 4, 5
            ORDER BY 1 ASC, total_revenue DESC
            """,
            [start_date, end_date],
        ).fetchall()

        if not rows:
            print("\n  ⚠️  No profitability data found for selected date range.")
            print(f"      Range: {start_date} to {end_date}")
            return

        output_dir = PROJECT_ROOT / "visualization" / "bol-dashboard" / "data"
        output_dir.mkdir(parents=True, exist_ok=True)

        header = [
            "Week",
            "Date Range",
            "Product",
            "EAN",
            "Latest_Order",
            "Units_Sold",
            "COGS_Avg",
            "Ad_Cost_Avg",
            "Sell_Price_Avg",
            "Original_Price_Avg",
            "Discount_Pct_Avg",
            "Margin_EUR_Avg",
            "Margin_PCT_Avg",
            "Total_Revenue",
            "Total_Profit",
        ]

        weekly_groups = {}
        for row in rows:
            week_start, week_end, iso_week = row[0], row[1], int(row[2])
            key = (week_start, week_end, iso_week)
            weekly_groups.setdefault(key, []).append(row)

        created_files = []
        for week_start, week_end, iso_week in sorted(weekly_groups.keys()):
            filename = (
                f"profitability_{week_start}_to_{week_end}_"
                f"Week{iso_week:02d}.csv"
            )
            output_path = output_dir / filename

            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(header)

                for row in weekly_groups[(week_start, week_end, iso_week)]:
                    writer.writerow([
                        f"Week {iso_week}",
                        format_human_date_range(week_start, week_end),
                        row[3] or "",
                        row[4] or "",
                        str(row[5]) if row[5] else "",
                        int(row[6] or 0),
                        float(row[7] or 0),
                        float(row[8] or 0),
                        float(row[9] or 0),
                        float(row[10] or 0),
                        float(row[11] or 0),
                        float(row[12] or 0),
                        float(row[13] or 0),
                        float(row[14] or 0),
                        float(row[15] or 0),
                    ])

            created_files.append(output_path)

        print(f"\n  ✅ Created {len(created_files)} weekly export file(s) in: {output_dir}")
        print(f"  📅 Requested range: {start_date} to {end_date}")
        for file_path in created_files:
            print(f"    - {file_path.name}")

    finally:
        loader.close()


def run_extraction(args):
    """Run the full extract → load pipeline."""
    setup_logging()

    loader = DuckDBLoader()
    loader.setup_schema()

    try:
        extractor = BolExtractor()
        orders_count = 0
        offers_count = 0

        # ── Extract & Load Orders ──
        if not args.offers_only:
            if args.start and args.end:
                start_date = parse_input_date(args.start).strftime("%Y-%m-%d")
                end_date = parse_input_date(args.end).strftime("%Y-%m-%d")
            else:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=args.days)
                start_date = start_date.strftime("%Y-%m-%d")
                end_date = end_date.strftime("%Y-%m-%d")

            orders = extractor.extract_orders(start_date, end_date)
            orders_count = loader.load_orders(orders)

        # ── Extract & Load Offers ──
        if not args.orders_only:
            offers = extractor.extract_offers()
            offers_count = loader.load_offers(offers)

        # ── Summary ──
        print("\n" + "=" * 60)
        print("  📦 Bol.com → DuckDB Pipeline Complete")
        print("=" * 60)
        if not args.offers_only:
            print(f"  Orders loaded:  {orders_count} items")
        if not args.orders_only:
            print(f"  Offers loaded:  {offers_count} items")
        
        counts = loader.get_table_counts()
        print(f"\n  Database totals:")
        for table, count in counts.items():
            print(f"    {table}: {count} rows")
        print("=" * 60)

    finally:
        loader.close()


def show_profitability(args):
    """Print the profitability summary table."""
    setup_logging()

    loader = DuckDBLoader()
    loader.setup_schema()

    try:
        summary = loader.query_profitability_summary()

        if not summary:
            print("\n  ⚠️  No data yet. Run the pipeline first:")
            print("      python scripts/run_bol_to_duckdb.py --days 7")
            return

        print("\n" + "=" * 120)
        print("  📊 Bol.com Product Profitability Summary")
        print("=" * 120)
        
        # Header
        print(f"  {'Product':<30} {'EAN':<14} {'Latest':<11} {'Units':>5} {'COGS':>7} {'Sell':>7} "
              f"{'Disc %':>7} {'Margin':>7} {'%':>5} {'Revenue':>10} {'Profit':>10}")
        print("  " + "-" * 123)

        total_revenue = 0
        total_profit = 0
        total_units = 0

        for row in summary:
            name = (row[0] or "Unknown")[:28]
            ean = row[1] or "N/A"
            latest_date = (row[2] or "N/A")[:10]  # Just YYYY-MM-DD
            units = row[3] or 0
            cogs = row[4] or 0
            sell = row[5] or 0
            original = row[6] or 0
            discount = row[7] or 0
            margin = row[8] or 0
            margin_pct = row[9] or 0
            revenue = row[10] or 0
            profit = row[11] or 0

            total_revenue += revenue
            total_profit += profit
            total_units += units

            print(f"  {name:<30} {ean:<14} {latest_date:<11} {units:>5} {cogs:>7.2f} {sell:>7.2f} "
                  f"{discount:>6.1f}% {margin:>7.2f} {margin_pct:>4.1f}% {revenue:>10.2f} {profit:>10.2f}")

        print("  " + "-" * 123)
        print(f"  {'TOTAL':<30} {'':<14} {'':<11} {total_units:>5} {'':>7} {'':>7} "
              f"{'':>7} {'':>7} {'':>5} {total_revenue:>10.2f} {total_profit:>10.2f}")
        print("=" * 123)

        # Formula Summary
        print("\n  📐 Formulas used:")
        print("      • Net Sell       = Sell / 1.21 (excl. VAT)")
        print("      • Orig Price     = Current offer price on Bol.com")
        print("      • Disc %         = ((Orig - Sell) / Orig) * 100")
        print("      • Margin €       = Net Sell - COGS - Estimated Ad Cost")
        print("      • Margin %       = (Margin € / Net Sell) * 100")
        print("      • Profit         = Margin € * Units Sold")

        # COGS warning
        if any(row[3] == 0 for row in summary):
            print("\n  ⚠️  Some products have COGS = 0. Import costs with:")
            print("      python scripts/manage_cogs.py --import scripts/bol_product_costs.csv")

    finally:
        loader.close()


def show_status(args):
    """Show table row counts."""
    setup_logging()
    
    loader = DuckDBLoader()
    loader.setup_schema()

    try:
        counts = loader.get_table_counts()
        print("\n  📋 DuckDB Table Status")
        print("  " + "-" * 40)
        for table, count in counts.items():
            print(f"    {table}: {count} rows")
        print()
    finally:
        loader.close()


def main():
    parser = argparse.ArgumentParser(
        description="Bol.com → DuckDB Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Mode selection
    parser.add_argument("--view", action="store_true",
                        help="Print profitability summary table")
    parser.add_argument("--export", action="store_true",
                        help="Export profitability summary CSV grouped by week")
    parser.add_argument("--status", action="store_true",
                        help="Show table row counts")

    # Date range
    parser.add_argument("--days", type=int, default=5,
                        help="Number of days to look back (default: 5)")
    parser.add_argument("--start", type=str, default=None,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="End date (YYYY-MM-DD)")

    # Selective extraction
    parser.add_argument("--offers-only", action="store_true",
                        help="Only refresh offers (skip orders)")
    parser.add_argument("--orders-only", action="store_true",
                        help="Only refresh orders (skip offers)")

    args = parser.parse_args()

    try:
        if args.view:
            show_profitability(args)
        elif args.export:
            export_weekly_summary(args)
        elif args.status:
            show_status(args)
        else:
            run_extraction(args)
    except ValueError as err:
        parser.error(str(err))


if __name__ == "__main__":
    main()

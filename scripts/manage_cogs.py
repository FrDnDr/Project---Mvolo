#!/usr/bin/env python3
"""
COGS & Ad Cost Manager — Import/manage product costs for profitability calculations.

Usage:
    python scripts/manage_cogs.py --import data/bol_product_costs.csv
    python scripts/manage_cogs.py --list
    python scripts/manage_cogs.py --set 8720892083647 --cogs 35.00 --ad-cost 5.00
"""

import sys
import csv
import argparse
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from extract.utils import setup_logging
from load.duckdb_loader import DuckDBLoader


def import_from_csv(csv_path: str):
    """Import COGS data from a CSV file."""
    setup_logging()
    
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"  ❌ File not found: {csv_path}")
        print(f"  Create it with columns: ean, product_name, cogs, estimated_ad_cost")
        sys.exit(1)

    costs = []
    with open(csv_file, "r", encoding="utf-8") as f:
        raw_text = f.read()

    # Normalize: replace ",\t" or "\t," with just "," so DictReader works cleanly
    cleaned_text = raw_text.replace(",\t", ",").replace("\t,", ",").replace("\t", ",")
    
    import io
    reader = csv.DictReader(io.StringIO(cleaned_text))
    for row in reader:
        # Normalize headers (remove spaces, underscores, lowercase) — skip None keys
        row_clean = {}
        for k, v in row.items():
            if k is None:
                continue
            normalized = k.strip().lower().replace(" ", "").replace("_", "")
            row_clean[normalized] = (v or "").strip()
        
        # Extract EAN (mandatory)
        ean = row_clean.get("ean", "").strip()
        if not ean:
            continue
            
        # Clean values (remove currency symbols, commas, etc)
        def clean_float(val):
            if not val:
                return None
            # Remove common non-numeric chars except decimal point
            cleaned = "".join(c for c in val if c.isdigit() or c in ".-")
            try:
                result = float(cleaned)
                return result if result != 0 else None
            except ValueError:
                return None

        costs.append({
            "ean":               ean,
            "product_name":      row_clean.get("productname", "").strip(),
            "original_price":    clean_float(row_clean.get("originalprice")),
            "cogs":              clean_float(row_clean.get("cogs")),
            "estimated_ad_cost": clean_float(row_clean.get("estimatedadcost")),
        })

    if not costs:
        print("  ⚠️  CSV file is empty or has no valid rows")
        return

    loader = DuckDBLoader()
    loader.setup_schema()
    try:
        count = loader.load_product_costs(costs)
        print(f"\n  ✅ Imported {count} product cost entries from {csv_file.name}")
        print(f"  Run 'python scripts/run_bol_to_duckdb.py --view' to see profitability")
    finally:
        loader.close()


def list_costs():
    """List all product costs currently in the database."""
    setup_logging()

    loader = DuckDBLoader()
    loader.setup_schema()
    try:
        result = loader._con.execute("""
            SELECT ean, product_name, original_price, cogs, estimated_ad_cost, updated_at
            FROM raw.bol_product_costs
            ORDER BY product_name
        """).fetchall()

        if not result:
            print("\n  Warning: No product costs in database yet.")
            print("  Import with: python scripts/manage_cogs.py --import 'Current COGS list.csv'")
            return

        print(f"\n  Product Costs ({len(result)} entries)")
        print("  " + "-" * 95)
        print(f"  {'EAN':<18} {'Product':<30} {'Orig':<10} {'COGS':>8} {'Ad Cost':>8}")
        print("  " + "-" * 95)
        for row in result:
            orig = f"{row[2]:.2f}" if row[2] else "  -"
            print(f"  {row[0]:<18} {(row[1] or 'N/A')[:28]:<30} {orig:>10} {row[3]:>8.2f} {row[4]:>8.2f}")
        print()
    finally:
        loader.close()


def set_cost(ean: str, cogs: float, ad_cost: float):
    """Set COGS and ad cost for a specific EAN."""
    setup_logging()

    loader = DuckDBLoader()
    loader.setup_schema()
    try:
        loader.load_product_costs([{
            "ean":               ean,
            "product_name":      "",  # Will use existing name if already set
            "cogs":              cogs,
            "estimated_ad_cost": ad_cost,
        }])
        
        # Format values for display: Use original value or 'No Change'
        cogs_str = f"{cogs:.2f}" if cogs is not None else "(No Change)"
        ad_str = f"{ad_cost:.2f}" if ad_cost is not None else "(No Change)"
        
        print(f"\n  ✅ Updated EAN {ean}: COGS={cogs_str}, Ad Cost={ad_str}")
    finally:
        loader.close()


def delete_cost(ean: str):
    """Delete a specific EAN from the costs database."""
    setup_logging()

    if not ean.strip():
        # Handle the case where they want to delete the empty one
        sql_cond = "ean = '' OR ean IS NULL"
    else:
        sql_cond = f"ean = '{ean}'"

    loader = DuckDBLoader()
    loader.setup_schema()
    try:
        count = loader._con.execute(f"DELETE FROM raw.bol_product_costs WHERE {sql_cond}").rowcount
        print(f"\n  ✅ Deleted {count} row(s) from database for EAN '{ean}'")
    finally:
        loader.close()


def main():
    parser = argparse.ArgumentParser(
        description="COGS & Ad Cost Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Import mode
    parser.add_argument("--import", dest="import_csv", type=str, metavar="CSV",
                        help="Import COGS from CSV file")

    # List mode
    parser.add_argument("--list", action="store_true",
                        help="List all product costs")

    # Set mode
    parser.add_argument("--set", type=str, metavar="EAN",
                        help="Set COGS for a specific EAN")
    parser.add_argument("--cogs", type=float, default=None,
                        help="Cost of goods sold per unit")
    parser.add_argument("--ad-cost", type=float, default=None,
                        help="Estimated ad cost per unit")

    # Delete mode
    parser.add_argument("--delete", type=str, metavar="EAN",
                        help="Remove a product from the costs database")

    args = parser.parse_args()

    if args.import_csv:
        import_from_csv(args.import_csv)
    elif args.list:
        list_costs()
    elif args.set is not None:
        if not args.set.strip():
            print("Error: EAN cannot be empty")
            sys.exit(1)
        set_cost(args.set, args.cogs, args.ad_cost)
    elif args.delete is not None:
        delete_cost(args.delete)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Shopify COGS Manager — Import/manage product costs for Shopify products.

Usage:
    python scripts/manage_shopify_cogs.py --import scripts/shopify_product_costs.csv
    python scripts/manage_shopify_cogs.py --list
"""

import sys
import csv
import argparse
import logging
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from extract.utils import setup_logging
from load.duckdb_loader import DuckDBLoader

def import_from_csv(csv_path: str):
    """Import Shopify COGS data from a CSV file."""
    setup_logging()
    logger = logging.getLogger("mvolo.scripts.shopify_cogs")
    
    csv_file = Path(csv_path)
    if not csv_file.exists():
        logger.error(f"File not found: {csv_path}")
        return

    costs = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize keys to lowercase/no space
            row_clean = {k.strip().lower().replace(" ", ""): v for k, v in row.items() if k}
            
            sku = row_clean.get("sku", "").strip()
            if not sku:
                continue

            def clean_float(val):
                if not val: return 0.0
                try:
                    return float("".join(c for c in val if c.isdigit() or c in ".-"))
                except:
                    return 0.0

            costs.append({
                "sku": sku,
                "product_name": row_clean.get("productname", ""),
                "original_price": clean_float(row_clean.get("originalprice")),
                "cogs": clean_float(row_clean.get("cogs")),
                "estimated_ad_cost": clean_float(row_clean.get("estimatedadcost")),
            })

    if not costs:
        logger.warning("No valid rows found in CSV.")
        return

    loader = DuckDBLoader()
    loader.setup_schema()
    try:
        count = loader.load_shopify_product_costs(costs)
        logger.info(f"Imported {count} Shopify cost entries.")
    finally:
        loader.close()

def list_costs():
    """List all Shopify product costs."""
    setup_logging()
    loader = DuckDBLoader()
    try:
        result = loader._con.execute("SELECT sku, product_name, cogs, updated_at FROM raw.shopify_product_costs ORDER BY sku").fetchall()
        print(f"\n  Shopify Product Costs ({len(result)} entries)")
        print("  " + "-" * 80)
        print(f"  {'SKU':<20} {'Product':<40} {'COGS':>10}")
        print("  " + "-" * 80)
        for row in result:
            print(f"  {row[0]:<20} {(row[1] or 'N/A')[:38]:<40} {row[2]:>10.2f}")
    finally:
        loader.close()

def main():
    parser = argparse.ArgumentParser(description="Shopify COGS Manager")
    parser.add_argument("--import", dest="import_csv", type=str, help="Import CSV")
    parser.add_argument("--list", action="store_true", help="List costs")
    args = parser.parse_args()

    if args.import_csv:
        import_from_csv(args.import_csv)
    elif args.list:
        list_costs()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

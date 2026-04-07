#!/usr/bin/env python3
"""
Load Shopify Product Data — Populates the product_costs table from collected JSON data.
"""

import sys
import json
import logging
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from extract.utils import setup_logging
from load.duckdb_loader import DuckDBLoader

def load_from_json(json_path: str):
    """Load products from Shopify JSON file and upsert into product_costs table."""
    setup_logging()
    logger = logging.getLogger("mvolo.scripts.load_products")
    
    json_file = Path(json_path)
    if not json_file.exists():
        logger.error(f"File not found: {json_path}")
        return

    logger.info(f"Loading products from {json_path}...")
    
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            products = data.get("products", [])
    except Exception as e:
        logger.error(f"Failed to parse JSON: {e}")
        return

    if not products:
        logger.warning("No products found in JSON.")
        return

    costs_to_load = []
    for p in products:
        product_name = p.get("title", "")
        # Variants contain the SKUs
        for v in p.get("variants", []):
            sku = v.get("sku")
            if not sku:
                continue
            
            # Use variant title if it's not "Default Title"
            variant_title = v.get("title", "")
            full_name = f"{product_name} ({variant_title})" if variant_title != "Default Title" else product_name
            
            costs_to_load.append({
                "ean": None, # Shopify products match by SKU
                "sku": sku,
                "product_name": full_name,
                "original_price": float(v.get("price") or 0),
                "cogs": 0, # To be filled manually
                "estimated_ad_cost": 0
            })

    if not costs_to_load:
        logger.warning("No valid SKUs found in products.")
        return

    loader = DuckDBLoader()
    loader.setup_schema()
    try:
        count = loader.load_product_costs(costs_to_load)
        logger.info(f"Upserted {count} SKUs into raw.product_costs table.")
        logger.info("Next step: Run 'python scripts/manage_cogs.py --list' to see them.")
    finally:
        loader.close()

if __name__ == "__main__":
    # Load all files mentioned by user
    files = [
        "api testing/shopify/shopify_products_all.json",
        "api testing/shopify/shopify_products_filtered.json",
        "api testing/shopify/shopify_products_filtered_active.json"
    ]
    
    for f in files:
        load_from_json(f)

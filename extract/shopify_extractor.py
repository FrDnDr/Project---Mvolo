import os
import requests
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

from extract.base import BaseExtractor

logger = logging.getLogger("mvolo.extract.shopify")

class ShopifyExtractor(BaseExtractor):
    """
    Extractor for Shopify Order API data.
    """

    def __init__(self):
        super().__init__()
        # Load env explicitly if not already loaded
        env_path = Path(__file__).parent.parent / ".env"
        load_dotenv(dotenv_path=env_path)

        self.shop_url = os.getenv("SHOPIFY_STORE_URL")
        self.access_token = os.getenv("SHOPIFY_ACCESS_TOKEN")
        self.api_version = os.getenv("SHOPIFY_API_VERSION", "2026-01")

        if not self.shop_url or not self.access_token:
            logger.error("Missing Shopify credentials in .env")
            raise ValueError("SHOPIFY_STORE_URL or SHOPIFY_ACCESS_TOKEN not set")

        # Clean shop URL
        self.base_url = self.shop_url.replace("https://", "").replace("http://", "").replace(".myshopify.com", "")
        self.api_base = f"https://{self.base_url}.myshopify.com/admin/api/{self.api_version}"

    def _get_headers(self):
        return {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json"
        }

    def extract_orders(self, start_date: str = None, end_date: str = None) -> list[dict]:
        """
        Fetches orders from Shopify API within a date range.
        Dates should be in ISO 8601 format (YYYY-MM-DD).
        """
        # Shopify uses created_at_min and created_at_max
        params = {
            "status": "any",
            "limit": 250
        }

        if start_date:
            params["created_at_min"] = f"{start_date}T00:00:00Z"
        if end_date:
            params["created_at_max"] = f"{end_date}T23:59:59Z"

        url = f"{self.api_base}/orders.json"
        all_orders = []

        logger.info(f"Fetching Shopify orders from {start_date} to {end_date}...")

        while url:
            response = self._request("GET", url, params=params, headers=self._get_headers())
            if not response:
                break

            data = response.json()
            orders = data.get("orders", [])
            all_orders.extend(orders)

            # Handle pagination via Link header
            url = None
            if "Link" in response.headers:
                links = response.headers["Link"].split(",")
                for link in links:
                    if 'rel="next"' in link:
                        url = link.split(";")[0].strip("< >")
                        params = None # Params are already in the next link
                        break

        logger.info(f"Extracted {len(all_orders)} orders from Shopify.")
        return all_orders

    def flatten_order_items(self, orders: list[dict]) -> list[dict]:
        """
        Flattens order line items into a list of dictionaries suitable for the loader.
        """
        flattened_items = []
        for order in orders:
            order_id = str(order.get("id"))
            landing_site = order.get("landing_site") # Crucial for affiliate logic
            created_at = order.get("created_at")

            for item in order.get("line_items", []):
                flattened_items.append({
                    "order_id": order_id,
                    "line_item_id": str(item.get("id")),
                    "sku": item.get("sku"),
                    "name": item.get("name"),
                    "quantity": item.get("quantity"),
                    "price": float(item.get("price") or 0),
                    "total_discount": float(item.get("total_discount") or 0),
                    "landing_site": landing_site,
                    "created_at": created_at
                })
        
        return flattened_items

    def flatten_product_variants(self, products: list[dict]) -> list[dict]:
        """
        Flattens Shopify product variants into a list suitable for the loader.
        """
        flattened = []
        for product in products:
            product_id = str(product.get("id"))
            product_title = product.get("title")
            
            for variant in product.get("variants", []):
                flattened.append({
                    "id": str(variant.get("id")),
                    "product_id": product_id,
                    "title": f"{product_title} - {variant.get('title')}" if variant.get('title') != "Default Title" else product_title,
                    "sku": variant.get("sku"),
                    "price": float(variant.get("price") or 0),
                    "compare_at_price": float(variant.get("compare_at_price") or 0),
                    "inventory_quantity": variant.get("inventory_quantity", 0),
                    "updated_at": variant.get("updated_at")
                })
        return flattened

    def load_local_products(self, file_path: str) -> list[dict]:
        """
        Loads product data from a local JSON file (collected by user).
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Handle both { "products": [...] } and direct list formats
                products = data.get("products", []) if isinstance(data, dict) else data
                logger.info(f"Loaded {len(products)} products from {file_path}")
                return products
        except Exception as e:
            logger.error(f"Failed to load local products from {file_path}: {e}")
            return []

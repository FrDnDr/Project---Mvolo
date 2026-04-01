"""
Bol.com Retailer API extractor.

Extracts:
- Orders (v10 API): order list + per-order details with line items
- Offers (v11 API): product listings with stock, pricing, fulfilment
"""

import logging
from datetime import datetime, timedelta

from extract.base import BaseExtractor
from extract.utils import BolAuthManager
from extract.config import (
    BOL_API_VERSIONS,
    BOL_ENDPOINTS,
    BOL_RATE_LIMIT,
    BOL_OFFERS_MAX_PAGES,
)

logger = logging.getLogger("mvolo.extract")


class BolExtractor(BaseExtractor):
    """
    Extracts order and offer data from the Bol.com Retailer API.
    
    Usage:
        extractor = BolExtractor()
        orders = extractor.extract_orders(start_date, end_date)
        offers = extractor.extract_offers(max_pages=5)
    """

    def __init__(self):
        self._auth = BolAuthManager()
        self._base_url = self._auth.base_url

    def _headers(self, api_version: str = "v10") -> dict:
        """Build request headers with auth token and API version."""
        return {
            "Authorization": f"Bearer {self._auth.get_token()}",
            "Accept": BOL_API_VERSIONS[api_version],
        }

    # ── Orders Extraction (v10) ──────────────────────────────────────

    def extract_orders(
        self,
        start_date: datetime | str,
        end_date: datetime | str,
    ) -> list[dict]:
        """
        Fetches order list + details for a date range.
        
        Calls the order list endpoint for each day, then fetches full details
        per order to get line-item data (EAN, price, quantity, etc.).
        
        Args:
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)
            
        Returns:
            Flat list of order-item dicts ready for DuckDB loading
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

        logger.info(f"Extracting Bol orders from {start_date.date()} to {end_date.date()}")

        all_items = []
        processed_order_ids = set()
        current_date = start_date

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            logger.info(f"Fetching order list for {date_str}...")

            order_summaries = self._fetch_order_list(date_str)
            logger.info(f"  Found {len(order_summaries)} orders for {date_str}")

            for summary in order_summaries:
                order_id = summary.get("orderId")
                if not order_id or order_id in processed_order_ids:
                    continue

                details = self._fetch_order_details(order_id)
                if not details:
                    continue

                items = self._flatten_order_items(order_id, details)
                all_items.extend(items)
                processed_order_ids.add(order_id)

                self._rate_limit_sleep(BOL_RATE_LIMIT["order_detail_delay"])

            current_date += timedelta(days=1)
            self._rate_limit_sleep(BOL_RATE_LIMIT["orders_list_delay"])

        logger.info(
            f"Extracted {len(all_items)} order items "
            f"from {len(processed_order_ids)} unique orders"
        )
        return all_items

    def _fetch_order_list(self, change_date: str) -> list[dict]:
        """Fetches order summaries for a specific date."""
        url = f"{self._base_url}{BOL_ENDPOINTS['orders']}"
        params = {
            "latest-change-date": change_date,
            "status": "ALL",
            "fulfillment-method": "ALL",
        }
        response = self._request("GET", url, headers=self._headers("v10"), params=params)
        if response and response.status_code == 200:
            return response.json().get("orders", [])
        return []

    def _fetch_order_details(self, order_id: str) -> dict | None:
        """Fetches full order details for a specific order ID."""
        url = f"{self._base_url}{BOL_ENDPOINTS['order_detail'].format(order_id=order_id)}"
        response = self._request("GET", url, headers=self._headers("v10"))
        if response and response.status_code == 200:
            return response.json()
        
        # Handle 401 — token might have expired
        if response and response.status_code == 401:
            logger.info("Token expired, refreshing...")
            self._auth.refresh_token()
            response = self._request("GET", url, headers=self._headers("v10"))
            if response and response.status_code == 200:
                return response.json()
        
        logger.warning(f"Could not fetch details for order {order_id}")
        return None

    @staticmethod
    def _flatten_order_items(order_id: str, details: dict) -> list[dict]:
        """Flatten order details into per-item rows for DuckDB."""
        placed_date = details.get("orderPlacedDateTime")
        items = []

        for item in details.get("orderItems", []):
            product = item.get("product", {})
            fulfilment = item.get("fulfilment", {})

            items.append({
                "order_id":           order_id,
                "order_item_id":      item.get("orderItemId"),
                "order_placed_at":    placed_date,
                "fulfillment_method": fulfilment.get("method"),
                "ean":                product.get("ean"),
                "title":              product.get("title"),
                "quantity":           item.get("quantity"),
                "unit_price":         item.get("unitPrice"),
                "total_price":        item.get("totalPrice"),
                "commission":         item.get("commission"),
            })

        return items

    # ── Offers Extraction (v11) ──────────────────────────────────────

    def extract_offers(self, max_pages: int = BOL_OFFERS_MAX_PAGES) -> list[dict]:
        """
        Fetches all product offers with stock, pricing, and fulfilment details.
        
        Paginates through all available pages (up to max_pages).
        
        Args:
            max_pages: Maximum number of pages to fetch (safety cap)
            
        Returns:
            Flat list of offer dicts ready for DuckDB loading
        """
        logger.info(f"Extracting Bol offers (max {max_pages} pages)...")
        all_offers = []

        for page in range(1, max_pages + 1):
            logger.info(f"  Fetching offers page {page}...")

            data = self._fetch_offers_page(page)
            if not data or "offers" not in data or not data["offers"]:
                logger.info(f"  No more offers after page {page - 1}")
                break

            offers = data["offers"]
            for offer in offers:
                all_offers.append(self._flatten_offer(offer))

            logger.info(f"  Page {page}: {len(offers)} offers")

            # If we got fewer than expected, we've likely hit the last page
            if len(offers) < 50:
                break

            self._rate_limit_sleep(BOL_RATE_LIMIT["offers_page_delay"])

        logger.info(f"Extracted {len(all_offers)} total offers")
        return all_offers

    def _fetch_offers_page(self, page: int) -> dict | None:
        """Fetches a single page of offers."""
        url = f"{self._base_url}{BOL_ENDPOINTS['offers']}"
        params = {"page": page}
        response = self._request("GET", url, headers=self._headers("v11"), params=params)
        
        if response and response.status_code == 200:
            return response.json()
        
        # Handle 401
        if response and response.status_code == 401:
            logger.info("Token expired, refreshing...")
            self._auth.refresh_token()
            response = self._request("GET", url, headers=self._headers("v11"), params=params)
            if response and response.status_code == 200:
                return response.json()
        
        return None

    @staticmethod
    def _flatten_offer(offer: dict) -> dict:
        """Flatten a single offer into a row for DuckDB."""
        stock = offer.get("stock", {})
        pricing = offer.get("pricing", {})
        bundle_prices = pricing.get("bundlePrices", [])
        fulfilment = offer.get("fulfilment", {})

        return {
            "offer_id":            offer.get("offerId"),
            "ean":                 offer.get("ean"),
            "product_title":       offer.get("unknownProductTitle"),
            "stock_amount":        stock.get("amount", 0),
            "corrected_stock":     stock.get("correctedStock", 0),
            "managed_by_retailer": stock.get("managedByRetailer", False),
            "unit_price":          bundle_prices[0].get("unitPrice") if bundle_prices else None,
            "fulfillment_method":  fulfilment.get("method"),
            "delivery_code":       fulfilment.get("deliveryCode"),
        }

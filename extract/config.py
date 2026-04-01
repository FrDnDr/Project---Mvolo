"""
Extract layer configuration — API endpoints, headers, and rate-limit settings.
Centralizes all Bol.com (and future Shopify) API constants.
"""

# ── Bol.com Authentication ──
BOL_TOKEN_URL = "https://login.bol.com/token"

# ── Bol.com Retailer API ──
BOL_API_VERSIONS = {
    "v10": "application/vnd.retailer.v10+json",
    "v11": "application/vnd.retailer.v11+json",
}

BOL_ENDPOINTS = {
    "orders":       "/retailer/orders",
    "order_detail": "/retailer/orders/{order_id}",
    "offers":       "/retailer/offers",
}

# ── Rate Limiting ──
BOL_RATE_LIMIT = {
    "orders_list_delay":   1.0,    # seconds between daily order-list requests
    "order_detail_delay":  0.4,    # seconds between individual order detail requests
    "offers_page_delay":   1.0,    # seconds between offer pagination requests
}

# ── Retry Settings ──
RETRY_MAX_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 2          # exponential backoff: 2^attempt seconds
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}  # HTTP codes that trigger retry

# ── Pagination ──
BOL_OFFERS_MAX_PAGES = 50       # safety cap for offers pagination

# ── VAT ──
NL_VAT_RATE = 1.21              # Netherlands standard VAT rate (21%)

#!/usr/bin/env python3
"""
Bol.com Product Conversion Pipeline

Fetches PRODUCT_VISITS from Bol.com Insights API per offer and stores weekly/monthly
conversion metrics in DuckDB. Also exports dashboard-friendly CSV files.

Usage:
    python scripts/run_bol_conversion_pipeline.py --mode weekly
    python scripts/run_bol_conversion_pipeline.py --mode monthly
    python scripts/run_bol_conversion_pipeline.py --mode both
    python scripts/run_bol_conversion_pipeline.py --mode both --reference-date 2026-04-09

Scheduling (cron):
    10 5 * * * /path/to/python /path/to/scripts/run_bol_conversion_pipeline.py --mode both
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import duckdb
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "mvolo.duckdb"
DEFAULT_EXPORT_DIR = PROJECT_ROOT / "visualization" / "data"

BASE_URL = "https://api.bol.com"
TOKEN_URL = "https://login.bol.com/token"
API_VERSION_V10_JSON = "application/vnd.retailer.v10+json"

MAX_REQUESTS_PER_SECOND = 4
MIN_REQUEST_INTERVAL_SECONDS = 1.0 / MAX_REQUESTS_PER_SECOND
TOKEN_REFRESH_SAFETY_SECONDS = 30

LOGGER = logging.getLogger("mvolo.bol_conversion")


CREATE_METRICS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.bol_offer_conversion_metrics (
    offer_id         VARCHAR,
    product_name     VARCHAR,
    ean              VARCHAR,
    date             DATE,
    period_type      VARCHAR,
    period_label     VARCHAR,
    visits_nl        INTEGER,
    visits_be        INTEGER,
    visits_total     INTEGER,
    orders           INTEGER,
    conversion_rate  DECIMAL(10, 4),
    fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (offer_id, period_type, period_label)
);
"""

CREATE_DAILY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.bol_offer_conversion_daily (
    offer_id         VARCHAR,
    product_name     VARCHAR,
    ean              VARCHAR,
    date             DATE,
    period_type      VARCHAR,
    period_label     VARCHAR,
    visits_nl        INTEGER,
    visits_be        INTEGER,
    visits_total     INTEGER,
    orders           INTEGER,
    conversion_rate  DECIMAL(10, 4),
    fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (offer_id, date, period_type, period_label)
);
"""


@dataclass
class OfferRecord:
    offer_id: str
    product_name: str
    ean: str


@dataclass
class PeriodRange:
    period_type: str
    period_label: str
    start_date: date
    end_date: date


class BolTokenManager:
    """Caches and refreshes OAuth token only when near expiry."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: str | None = None
        self._expires_at_epoch: float = 0.0

    def get_token(self) -> str:
        now = time.time()
        if self._token and now < self._expires_at_epoch - TOKEN_REFRESH_SAFETY_SECONDS:
            return self._token

        response = requests.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 600))
        self._expires_at_epoch = now + expires_in

        LOGGER.info("Fetched new Bol token (expires in %ss)", expires_in)
        return payload["access_token"]


class BolInsightsClient:
    """Thin API client with rate limiting + token reuse."""

    def __init__(self, token_manager: BolTokenManager):
        self.token_manager = token_manager
        self._last_request_ts = 0.0

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Accept": API_VERSION_V10_JSON,
            "Content-Type": API_VERSION_V10_JSON,
        }

    def _respect_rate_limit(self):
        elapsed = time.perf_counter() - self._last_request_ts
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)

    def fetch_product_visits(self, offer_id: str, period: str, number_of_periods: int) -> dict:
        self._respect_rate_limit()

        params = {
            "offer-id": offer_id,
            "period": period,
            "number-of-periods": number_of_periods,
            "name": "PRODUCT_VISITS",
        }
        response = requests.get(
            f"{BASE_URL}/retailer/insights/offer",
            headers=self._headers(),
            params=params,
            timeout=60,
        )
        self._last_request_ts = time.perf_counter()

        if response.status_code in (401, 403):
            # Force token refresh once, then retry.
            self.token_manager._expires_at_epoch = 0.0
            response = requests.get(
                f"{BASE_URL}/retailer/insights/offer",
                headers=self._headers(),
                params=params,
                timeout=60,
            )
            self._last_request_ts = time.perf_counter()

        response.raise_for_status()
        return response.json()


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(CREATE_METRICS_TABLE_SQL)
    con.execute(CREATE_DAILY_TABLE_SQL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bol.com conversion insights pipeline")
    parser.add_argument(
        "--mode",
        choices=["weekly", "monthly", "both"],
        default="both",
        help="Which period metrics to refresh",
    )
    parser.add_argument(
        "--reference-date",
        default=None,
        help="Reference date in YYYY-MM-DD (defaults to today in Europe/Amsterdam)",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--export-dir",
        default=str(DEFAULT_EXPORT_DIR),
        help="Folder for dashboard CSV exports",
    )
    parser.add_argument(
        "--skip-daily",
        action="store_true",
        help="Skip daily history fetch for charting",
    )
    return parser.parse_args()


def parse_reference_date(reference_date: str | None) -> date:
    if reference_date:
        return datetime.strptime(reference_date, "%Y-%m-%d").date()
    return datetime.now(ZoneInfo("Europe/Amsterdam")).date()


def week_range(ref: date) -> PeriodRange:
    start = ref - timedelta(days=ref.weekday())
    end = start + timedelta(days=6)
    label = f"W{start.isocalendar().week:02d}-{start.year}"
    return PeriodRange(period_type="WEEK", period_label=label, start_date=start, end_date=end)


def month_range(ref: date) -> PeriodRange:
    start = ref.replace(day=1)
    if start.month == 12:
        month_after = start.replace(year=start.year + 1, month=1, day=1)
    else:
        month_after = start.replace(month=start.month + 1, day=1)
    end = month_after - timedelta(days=1)
    label = start.strftime("%Y-%m")
    return PeriodRange(period_type="MONTH", period_label=label, start_date=start, end_date=end)


def get_period_ranges(mode: str, ref_date: date) -> list[PeriodRange]:
    if mode == "weekly":
        return [week_range(ref_date)]
    if mode == "monthly":
        return [month_range(ref_date)]
    return [week_range(ref_date), month_range(ref_date)]


def read_offer_catalog(con: duckdb.DuckDBPyConnection) -> list[OfferRecord]:
    rows = con.execute(
        """
        SELECT
            o.offer_id,
            COALESCE(NULLIF(c.product_name, ''), NULLIF(o.product_title, ''), 'Unknown Product') AS product_name,
            COALESCE(o.ean, '') AS ean
        FROM raw.bol_offers o
        LEFT JOIN raw.bol_product_costs c ON c.ean = o.ean
        WHERE o.offer_id IS NOT NULL
        ORDER BY product_name, o.offer_id
        """
    ).fetchall()

    return [OfferRecord(offer_id=row[0], product_name=row[1], ean=row[2]) for row in rows]


def order_counts_by_offer(
    con: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
) -> dict[str, int]:
    rows = con.execute(
        """
        SELECT
            off.offer_id,
            COALESCE(SUM(o.quantity), 0) AS units_sold
        FROM raw.bol_offers off
        LEFT JOIN raw.bol_orders o
            ON o.ean = off.ean
           AND CAST(o.order_placed_at AS DATE) BETWEEN ? AND ?
        WHERE off.offer_id IS NOT NULL
        GROUP BY off.offer_id
        """,
        [start_date, end_date],
    ).fetchall()
    return {row[0]: int(row[1] or 0) for row in rows}


def daily_orders_by_offer(
    con: duckdb.DuckDBPyConnection,
    start_date: date,
    end_date: date,
) -> dict[tuple[str, date], int]:
    rows = con.execute(
        """
        SELECT
            off.offer_id,
            CAST(o.order_placed_at AS DATE) AS order_date,
            COALESCE(SUM(o.quantity), 0) AS units_sold
        FROM raw.bol_offers off
        JOIN raw.bol_orders o
            ON o.ean = off.ean
        WHERE off.offer_id IS NOT NULL
          AND CAST(o.order_placed_at AS DATE) BETWEEN ? AND ?
        GROUP BY off.offer_id, CAST(o.order_placed_at AS DATE)
        """,
        [start_date, end_date],
    ).fetchall()
    return {(row[0], row[1]): int(row[2] or 0) for row in rows}


def parse_country_visits(payload: dict) -> tuple[int, int, int]:
    """
    Extract NL/BE/total visits from multiple likely response shapes.
    Handles the 'offerInsights' wrapper used in Bol Retailer API v10.
    """

    def visit_value(node: dict) -> int:
        for key in ("total", "value", "count", "visits", "numberOfVisits"):
            val = node.get(key)
            if isinstance(val, (int, float)):
                return int(val)
        return 0

    nl = 0
    be = 0

    # Bol v10 Offer Insights often wraps data in 'offerInsights'
    nodes_to_check = [payload]
    if "offerInsights" in payload and isinstance(payload["offerInsights"], list):
        nodes_to_check.extend(payload["offerInsights"])

    for node in nodes_to_check:
        if not isinstance(node, dict):
            continue

        # Check for direct country breakdown
        for key in ("countries", "country"):
            collection = node.get(key)
            if isinstance(collection, list):
                for item in collection:
                    code = (item.get("countryCode") or item.get("country") or "").upper()
                    val = visit_value(item)
                    if code == "NL": nl += val
                    elif code == "BE": be += val

        # Check for nested insights lists
        for key in ("insights", "items", "results"):
            collection = node.get(key)
            if isinstance(collection, list):
                for sub in collection:
                    if not isinstance(sub, dict): continue
                    # Recursive-like check for countries in sub-items
                    for c_key in ("countries", "country"):
                        c_list = sub.get(c_key)
                        if isinstance(c_list, list):
                            for c_item in c_list:
                                code = (c_item.get("countryCode") or c_item.get("country") or "").upper()
                                val = visit_value(c_item)
                                if code == "NL": nl += val
                                elif code == "BE": be += val

    total = nl + be
    if total == 0:
        # Fallback to top-level total in any of the nodes
        for node in nodes_to_check:
            total = visit_value(node)
            if total > 0: break

    return nl, be, total


def parse_daily_series(payload: dict) -> dict[date, tuple[int, int, int]]:
    """
    Parse daily visits from daily insights response.
    Returns mapping: day -> (nl, be, total)
    """
    out: dict[date, tuple[int, int, int]] = {}

    # Extract items from common wrappers
    items_to_process = []
    
    # 1. Check top-level lists
    for key in ("insights", "results", "items", "days", "periods"):
        val = payload.get(key)
        if isinstance(val, list):
            items_to_process.extend(val)

    # 2. Check offerInsights -> periods
    if "offerInsights" in payload and isinstance(payload["offerInsights"], list):
        for offer in payload["offerInsights"]:
            for key in ("periods", "days", "insights"):
                val = offer.get(key)
                if isinstance(val, list):
                    items_to_process.extend(val)

    for item in items_to_process:
        if not isinstance(item, dict):
            continue

        day_str = item.get("day") or item.get("date") or item.get("timestamp")
        parsed_day: date | None = None
        if isinstance(day_str, str):
            try:
                parsed_day = datetime.fromisoformat(day_str.replace("Z", "+00:00")).date()
            except ValueError:
                try:
                    parsed_day = datetime.strptime(day_str[:10], "%Y-%m-%d").date()
                except ValueError:
                    parsed_day = None

        if parsed_day is None:
            continue

        nl, be, total = parse_country_visits(item)
        # If parse_country_visits (direct) fails, check the item itself for a total
        if total == 0:
            for key in ("total", "value", "count"):
                if isinstance(item.get(key), (int, float)):
                    total = int(item[key])
                    break

        out[parsed_day] = (nl, be, total)

    return out


def upsert_metric(
    con: duckdb.DuckDBPyConnection,
    offer: OfferRecord,
    period: PeriodRange,
    visits_nl: int,
    visits_be: int,
    visits_total: int,
    orders: int,
) -> None:
    conversion = 0.0 if visits_total <= 0 else (orders / visits_total) * 100.0
    con.execute(
        """
        INSERT OR REPLACE INTO raw.bol_offer_conversion_metrics
            (offer_id, product_name, ean, date, period_type, period_label,
             visits_nl, visits_be, visits_total, orders, conversion_rate, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            offer.offer_id,
            offer.product_name,
            offer.ean,
            period.end_date,
            period.period_type,
            period.period_label,
            visits_nl,
            visits_be,
            visits_total,
            orders,
            round(conversion, 4),
        ],
    )


def upsert_daily_records(
    con: duckdb.DuckDBPyConnection,
    offer: OfferRecord,
    period: PeriodRange,
    daily_visits: dict[date, tuple[int, int, int]],
    daily_orders: dict[tuple[str, date], int],
) -> None:
    for day, (nl, be, total) in daily_visits.items():
        if day < period.start_date or day > period.end_date:
            continue

        orders = daily_orders.get((offer.offer_id, day), 0)
        conversion = 0.0 if total <= 0 else (orders / total) * 100.0

        con.execute(
            """
            INSERT OR REPLACE INTO raw.bol_offer_conversion_daily
                (offer_id, product_name, ean, date, period_type, period_label,
                 visits_nl, visits_be, visits_total, orders, conversion_rate, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                offer.offer_id,
                offer.product_name,
                offer.ean,
                day,
                period.period_type,
                period.period_label,
                nl,
                be,
                total,
                orders,
                round(conversion, 4),
            ],
        )


def export_dashboard_csvs(con: duckdb.DuckDBPyConnection, export_dir: Path) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)

    metrics_path = export_dir / "bol_conversion_metrics.csv"
    daily_path = export_dir / "bol_conversion_daily.csv"

    metric_rows = con.execute(
        """
        SELECT
            offer_id,
            product_name,
            ean,
            CAST(date AS VARCHAR) AS date,
            period_type,
            period_label,
            visits_nl,
            visits_be,
            visits_total,
            orders,
            conversion_rate
        FROM raw.bol_offer_conversion_metrics
        ORDER BY period_type, period_label, conversion_rate DESC, product_name
        """
    ).fetchall()

    daily_rows = con.execute(
        """
        SELECT
            offer_id,
            product_name,
            ean,
            CAST(date AS VARCHAR) AS date,
            period_type,
            period_label,
            visits_nl,
            visits_be,
            visits_total,
            orders,
            conversion_rate
        FROM raw.bol_offer_conversion_daily
        ORDER BY date, product_name
        """
    ).fetchall()

    with metrics_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "offer_id",
            "product_name",
            "ean",
            "date",
            "period_type",
            "period_label",
            "visits_nl",
            "visits_be",
            "visits_total",
            "orders",
            "conversion_rate",
        ])
        writer.writerows(metric_rows)

    with daily_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "offer_id",
            "product_name",
            "ean",
            "date",
            "period_type",
            "period_label",
            "visits_nl",
            "visits_be",
            "visits_total",
            "orders",
            "conversion_rate",
        ])
        writer.writerows(daily_rows)

    LOGGER.info("Exported dashboard data: %s", metrics_path)
    LOGGER.info("Exported dashboard data: %s", daily_path)


def iter_period_requests(period: PeriodRange, skip_daily: bool) -> Iterable[tuple[str, int]]:
    yield period.period_type, 1

    if not skip_daily:
        days = (period.end_date - period.start_date).days + 1
        yield "DAY", days


def run_pipeline(args: argparse.Namespace) -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    client_id = os.getenv("BOL_CLIENT_ID")
    client_secret = os.getenv("BOL_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("Missing BOL_CLIENT_ID / BOL_CLIENT_SECRET in .env")

    ref_date = parse_reference_date(args.reference_date)

    if datetime.now(ZoneInfo("Europe/Amsterdam")).hour < 5:
        LOGGER.warning("Running before 05:00 CET/CEST may return incomplete insights due to API lag.")

    periods = get_period_ranges(args.mode, ref_date)
    con = duckdb.connect(str(Path(args.db_path).resolve()))

    try:
        ensure_schema(con)

        offers = read_offer_catalog(con)
        if not offers:
            LOGGER.warning("No offers found in raw.bol_offers; nothing to process.")
            return

        token_manager = BolTokenManager(client_id=client_id, client_secret=client_secret)
        insights_client = BolInsightsClient(token_manager=token_manager)

        LOGGER.info("Processing %s offers across %s period mode(s)", len(offers), len(periods))

        for period in periods:
            LOGGER.info(
                "Refreshing %s metrics for %s (%s to %s)",
                period.period_type,
                period.period_label,
                period.start_date,
                period.end_date,
            )

            orders_totals = order_counts_by_offer(con, period.start_date, period.end_date)
            orders_daily = daily_orders_by_offer(con, period.start_date, period.end_date)

            for idx, offer in enumerate(offers, start=1):
                aggregate_payload = insights_client.fetch_product_visits(
                    offer_id=offer.offer_id,
                    period=period.period_type,
                    number_of_periods=1,
                )
                visits_nl, visits_be, visits_total = parse_country_visits(aggregate_payload)
                orders = orders_totals.get(offer.offer_id, 0)

                upsert_metric(
                    con=con,
                    offer=offer,
                    period=period,
                    visits_nl=visits_nl,
                    visits_be=visits_be,
                    visits_total=visits_total,
                    orders=orders,
                )

                if not args.skip_daily:
                    daily_payload = insights_client.fetch_product_visits(
                        offer_id=offer.offer_id,
                        period="DAY",
                        number_of_periods=(period.end_date - period.start_date).days + 1,
                    )
                    daily_visits = parse_daily_series(daily_payload)
                    upsert_daily_records(
                        con=con,
                        offer=offer,
                        period=period,
                        daily_visits=daily_visits,
                        daily_orders=orders_daily,
                    )

                if idx % 25 == 0 or idx == len(offers):
                    LOGGER.info("  %s/%s offers processed for %s", idx, len(offers), period.period_label)

        export_dashboard_csvs(con, Path(args.export_dir).resolve())
        LOGGER.info("Conversion pipeline finished successfully")

    finally:
        con.close()


def main() -> None:
    setup_logging()
    args = parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()

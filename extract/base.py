"""
Base extractor — abstract base class with retry logic, rate-limit handling, and logging.
All API extractors (Bol, Shopify) inherit from this.
"""

import time
import logging
from abc import ABC, abstractmethod

import requests

from extract.config import RETRY_MAX_ATTEMPTS, RETRY_BACKOFF_BASE, RETRY_STATUS_CODES

logger = logging.getLogger("mvolo.extract")


class BaseExtractor(ABC):
    """
    Abstract base class for API extractors.
    
    Provides:
    - _request(): HTTP GET/POST with automatic retry and exponential backoff
    - Rate-limit aware sleeping
    - Structured logging
    """

    def _request(
        self,
        method: str,
        url: str,
        headers: dict = None,
        params: dict = None,
        max_attempts: int = RETRY_MAX_ATTEMPTS,
    ) -> requests.Response | None:
        """
        Make an HTTP request with automatic retry on transient failures.
        
        Args:
            method: HTTP method ("GET" or "POST")
            url: Full URL to request
            headers: Request headers (including auth)
            params: Query parameters
            max_attempts: Max retry attempts
            
        Returns:
            Response object on success, None on persistent failure
        """
        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=30,
                )

                # Success
                if response.status_code == 200:
                    return response

                # Not found — no retry
                if response.status_code == 404:
                    logger.warning(f"404 Not Found: {url}")
                    return response

                # Unauthorized — no retry (caller should refresh token)
                if response.status_code == 401:
                    logger.warning(f"401 Unauthorized: {url}")
                    return response

                # Retryable error
                if response.status_code in RETRY_STATUS_CODES:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        f"Retryable error {response.status_code} on {url} "
                        f"(attempt {attempt}/{max_attempts}). Waiting {wait}s..."
                    )
                    time.sleep(wait)
                    continue

                # Other error — no retry
                logger.error(f"HTTP {response.status_code} on {url}: {response.text[:200]}")
                return response

            except requests.exceptions.Timeout:
                wait = RETRY_BACKOFF_BASE ** attempt
                logger.warning(
                    f"Timeout on {url} (attempt {attempt}/{max_attempts}). Waiting {wait}s..."
                )
                time.sleep(wait)

            except requests.exceptions.ConnectionError as e:
                wait = RETRY_BACKOFF_BASE ** attempt
                logger.warning(
                    f"Connection error on {url} (attempt {attempt}/{max_attempts}): {e}. "
                    f"Waiting {wait}s..."
                )
                time.sleep(wait)

            except Exception as e:
                logger.error(f"Unexpected error on {url}: {e}")
                return None

        logger.error(f"All {max_attempts} attempts failed for {url}")
        return None

    @staticmethod
    def _rate_limit_sleep(seconds: float):
        """Sleep to respect API rate limits."""
        if seconds > 0:
            time.sleep(seconds)

"""
Shared utilities for the extract layer — authentication, logging, helpers.
"""

import os
import logging
from pathlib import Path
from requests.auth import HTTPBasicAuth
import requests
from dotenv import load_dotenv

from extract.config import BOL_TOKEN_URL

logger = logging.getLogger("mvolo.extract")


def setup_logging(level=logging.INFO):
    """Configure structured logging for the extract layer."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(name)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_env():
    """Load .env from project root."""
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)


class BolAuthManager:
    """
    Handles Bol.com API token management.
    
    Strategy:
    1. Try reading the latest token from recorded_tokens.txt (shared with test scripts)
    2. If not available, fetch a fresh token via client credentials
    3. Cache the token in memory for the session
    """

    def __init__(self):
        load_env()
        self._client_id = os.getenv("BOL_CLIENT_ID")
        self._client_secret = os.getenv("BOL_CLIENT_SECRET")
        self._base_url = os.getenv("BOL_BASE_URL", "https://api.bol.com").rstrip("/")
        self._token = None
        self._token_file = Path(__file__).parent.parent / "api testing" / "recorded_tokens.txt"

        if not self._client_id or not self._client_secret:
            raise ValueError("BOL_CLIENT_ID and BOL_CLIENT_SECRET must be set in .env")

    @property
    def base_url(self) -> str:
        return self._base_url

    def get_token(self) -> str:
        """Returns a valid access token, fetching one if necessary."""
        if self._token:
            return self._token

        # Try file-based token first
        token = self._read_token_file()
        if token:
            logger.info("Using existing token from recorded_tokens.txt")
            self._token = token
            return token

        # Fetch fresh token
        token = self._fetch_fresh_token()
        if token:
            self._token = token
            return token

        raise RuntimeError("Failed to obtain Bol.com access token")

    def refresh_token(self) -> str:
        """Force-fetch a new token (e.g., after a 401)."""
        logger.info("Refreshing Bol.com access token...")
        self._token = None
        token = self._fetch_fresh_token()
        if token:
            self._token = token
            return token
        raise RuntimeError("Failed to refresh Bol.com access token")

    def _read_token_file(self) -> str | None:
        """Reads the latest token from recorded_tokens.txt if available."""
        if not self._token_file.exists():
            return None
        try:
            with open(self._token_file, "r") as f:
                lines = [l.strip() for l in f.readlines() if l.strip()]
                if not lines:
                    return None
                last_line = lines[-1]
                if "Token: " in last_line:
                    return last_line.split("Token: ")[1].strip()
        except Exception as e:
            logger.warning(f"Could not read token file: {e}")
        return None

    def _fetch_fresh_token(self) -> str | None:
        """Fetches a new Bearer token from Bol.com API."""
        logger.info("Fetching new token from Bol.com API...")
        try:
            response = requests.post(
                BOL_TOKEN_URL,
                auth=HTTPBasicAuth(self._client_id, self._client_secret),
                params={"grant_type": "client_credentials"},
            )
            if response.status_code == 200:
                token = response.json().get("access_token")
                logger.info("Successfully obtained new access token")
                return token
            else:
                logger.error(f"Token request failed: {response.status_code} — {response.text}")
                return None
        except Exception as e:
            logger.error(f"Token fetch exception: {e}")
            return None

"""
base_scraper.py
===============
Abstract base class for all store scrapers in the Supermarket Pricing Project.

Provides:
  - requests.Session with realistic headers (connection pooling, cookie jar)
  - Configurable rate-limiting delay
  - File + console logging
  - Retry / exponential-backoff helper for HTTP GET requests
  - Abstract scrape() contract

Subclasses only need to implement scrape() and optionally override close().
"""

import os
import time
import logging
import random
from abc import ABC, abstractmethod

import requests


class BaseScraper(ABC):
    """Base class for all store scrapers.

    Handles HTTP session setup, logging, rate limiting, and retry logic.
    """

    def __init__(self, store_name: str, delay: float = 2.0):
        self.store_name = store_name
        self.delay = delay

        # --- Logging (file + console) ---
        self.setup_logging()

        # --- HTTP session with realistic browser headers ---
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "en-US,en;q=0.9",
        })

        self.logger.info(
            f"{self.store_name} scraper initialized (delay={self.delay}s)"
        )

    # ------------------------------------------------------------------ #
    # Logging                                                             #
    # ------------------------------------------------------------------ #

    def setup_logging(self):
        """Configure dual logging: rotating file + console."""
        log_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "logs"
        )
        os.makedirs(log_dir, exist_ok=True)

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(self.store_name)
        self.logger.setLevel(logging.INFO)

        # Avoid duplicate handlers when a scraper is re-instantiated
        if not self.logger.handlers:
            fh = logging.FileHandler(
                os.path.join(log_dir, f"{self.store_name}.log"),
                encoding="utf-8",
            )
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
            self.logger.addHandler(sh)

    # ------------------------------------------------------------------ #
    # HTTP helpers                                                        #
    # ------------------------------------------------------------------ #

    def fetch_page(self, url: str, max_retries: int = 3, **kwargs) -> str | None:
        """
        GET *url* and return the response text (HTML / JSON string).

        Applies:
          - rate-limiting delay before each attempt
          - exponential backoff on failure
          - up to *max_retries* attempts

        Any extra ``kwargs`` are forwarded to ``session.get()``.
        Returns ``None`` if every attempt fails.
        """
        for attempt in range(max_retries):
            # Rate limiting — randomised to reduce detection risk
            time.sleep(self.delay + random.uniform(0.3, 1.5))

            try:
                self.logger.info(
                    f"Fetch attempt {attempt + 1}/{max_retries}: {url}"
                )
                resp = self.session.get(url, timeout=20, **kwargs)
                resp.raise_for_status()
                self.logger.info(
                    f"OK — {len(resp.text):,} chars, status {resp.status_code}"
                )
                return resp.text

            except requests.exceptions.RequestException as exc:
                self.logger.warning(
                    f"Request failed (attempt {attempt + 1}): {exc}"
                )

            # Exponential backoff before next retry
            if attempt < max_retries - 1:
                backoff = 2 ** attempt
                self.logger.info(f"Retrying in {backoff}s …")
                time.sleep(backoff)

        self.logger.error(f"All {max_retries} attempts failed: {url}")
        return None

    # ------------------------------------------------------------------ #
    # Abstract interface                                                  #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def scrape(self):
        """Main scraping entry point — must be implemented by each store."""
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Cleanup                                                             #
    # ------------------------------------------------------------------ #

    def close(self):
        """Close the HTTP session and release resources."""
        if self.session:
            self.session.close()
            self.logger.info("HTTP session closed.")
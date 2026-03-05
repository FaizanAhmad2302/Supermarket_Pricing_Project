import time
import logging
import random
import os
from abc import ABC, abstractmethod

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager


class BaseScraper(ABC):
    """Base class for all store scrapers. Handles browser setup, logging, and page fetching."""

    def __init__(self, store_name: str, delay: float = 3.0):
        self.store_name = store_name
        self.delay = delay
        self.setup_logging()
        self.driver = self._init_driver()
        self.logger.info(f"{self.store_name} scraper initialized (delay={self.delay}s)")

    def _init_driver(self) -> webdriver.Chrome:
        """Set up headless Chrome with a realistic user-agent."""
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options,
        )

    def setup_logging(self):
        """Configure file and console logging."""
        log_dir = os.path.join(os.path.dirname(__file__), "logs")
        os.makedirs(log_dir, exist_ok=True)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger(self.store_name)
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            fh = logging.FileHandler(
                os.path.join(log_dir, f"{self.store_name}.log"), encoding="utf-8"
            )
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
            self.logger.addHandler(sh)

    def fetch_page_selenium(
        self, url: str, wait_selector: str = "body", max_retries: int = 3
    ) -> str | None:
        """
        Load a page with Selenium and return its rendered HTML.

        Applies rate limiting before each attempt and exponential backoff on failure.
        Returns None if all retries are exhausted.
        """
        for attempt in range(max_retries):
            # Rate limiting — randomised to avoid detection
            time.sleep(self.delay + random.uniform(0.5, 2.0))

            try:
                self.logger.info(f"Attempt {attempt + 1}/{max_retries}: {url}")
                self.driver.get(url)

                # Wait for the target element before reading the DOM
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
                )

                # Scroll to trigger lazy-loaded content
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                self.logger.info(f"OK — {len(self.driver.page_source):,} bytes")
                return self.driver.page_source

            except TimeoutException:
                self.logger.warning(f"Timeout on attempt {attempt + 1}: {url}")
            except WebDriverException as e:
                self.logger.warning(f"WebDriver error on attempt {attempt + 1}: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")

            # Exponential backoff before next retry
            if attempt < max_retries - 1:
                backoff = 2 ** attempt
                self.logger.info(f"Retrying in {backoff}s...")
                time.sleep(backoff)

        self.logger.error(f"All {max_retries} attempts failed: {url}")
        return None

    @abstractmethod
    def scrape(self):
        """Main scraping entry point. Must be implemented by each store subclass."""
        raise NotImplementedError

    def close(self):
        """Quit the browser and release resources."""
        if self.driver:
            self.driver.quit()
            self.logger.info("Browser closed.")
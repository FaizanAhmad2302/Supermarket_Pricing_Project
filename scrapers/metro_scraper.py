import os
import re
import csv
import sys
import time
import random
import requests

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base_scraper import BaseScraper


class MetroScraper(BaseScraper):
    """
    Scraper for Metro Online Pakistan (https://www.metro-online.pk).

    Uses Selenium to render the JS-heavy frontend and set city cookies,
    then fetches full product data via requests from the same public API
    that Metro's own frontend calls. BeautifulSoup is used for HTML fallback.
    """

    BASE_URL = "https://www.metro-online.pk"
    API_URL  = "https://admin.metro-online.pk/api/read/Products"

    STORE_MAP = {
        "Lahore":    10,
        "Islamabad": 11,
        "Karachi":   12,
    }

    # We must iterate by category because querying all products
    # caps out at 120 items total. The API requires specific tier1Id integers.
    # We will fetch these dynamically per store.

    LIMIT = 60  # products per API page

    CSV_HEADERS = [
        "Store", "City", "Category", "Sub-category", "Brand",
        "Product Name", "Original Price", "Discounted Price",
        "Unit", "Quantity", "Product URL", "Timestamp",
    ]

    def __init__(self):
        super().__init__(store_name="Metro", delay=2.0)
        self.raw_path = os.path.normpath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "data", "raw", "metro_raw.csv")
        )
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": self.BASE_URL + "/",
            "Origin": self.BASE_URL,
        })

    def _set_city_cookie(self, store_id: int):
        """Set storeId cookie in requests session."""
        self.session.cookies.set("storeId", str(store_id), domain="www.metro-online.pk")
        self.logger.info(f"City cookie set in session: storeId={store_id}")

    def _fetch_api_page(self, store_id: int, category_id: int, offset: int) -> dict | None:
        """
        Fetch one page of products from Metro's public API.
        Uses the same filter/filterValue param format as Metro's frontend.
        """
        params = [
            ("type", "Products_nd_associated_Brands"),
            ("order", "product_scoring__DESC"),
            ("offset", str(offset)),
            ("limit", str(self.LIMIT)),
            ("filter", "active"),
            ("filterValue", "true"),
            ("filter", "storeId"),
            ("filterValue", str(store_id)),
            ("filter", "||tier1Id"),
            ("filterValue", f"||{category_id}"),
            ("filter", "!url"),
            ("filterValue", "!null"),
            ("filter", "Op.available_stock"),
            ("filterValue", "Op.gt__0"),
        ]

        for attempt in range(3):
            time.sleep(self.delay + random.uniform(0.5, 1.5))
            try:
                self.logger.info(
                    f"API: store={store_id} cat={category_id} offset={offset} (attempt {attempt+1}/3)"
                )
                resp = self.session.get(self.API_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                count = len(data.get("data", []))
                total = data.get("total_count", "?")
                self.logger.info(f"API OK — {count} products (total: {total})")
                return data
            except Exception as e:
                self.logger.warning(f"API attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)

        self.logger.error(f"All API attempts failed: cat={category_id} offset={offset}")
        return None

    def _fetch_categories(self, store_id: int) -> dict:
        """Dynamically fetch all top-level category IDs for a given store."""
        url = "https://admin.metro-online.pk/api/read/Categories"
        params = {"storeId": store_id}
        
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                self.logger.info(f"Fetching categories for storeId={store_id}...")
                resp = self.session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json().get("data", [])
                
                cats = {}
                for c in data:
                    # Top-level categories have no parentId and a slug/name
                    if not c.get("parentId"):
                        cid = c.get("id")
                        name = c.get("name_lang", {}).get("en") or c.get("slug") or str(cid)
                        # Clean up names like "electronics_4"
                        name = name.replace("_", " ").title()
                        if cid:
                            cats[cid] = name
                
                self.logger.info(f"Found {len(cats)} categories for storeId={store_id}")
                return cats
            except Exception as e:
                self.logger.warning(f"Category fetch attempt {attempt+1} failed: {e}")
                time.sleep(2 ** attempt)
                
        self.logger.error(f"Failed to fetch categories for storeId={store_id}")
        return {}

    @staticmethod
    def _extract_unit_quantity(name: str):
        """Extract unit and quantity from product name via regex."""
        match = re.search(
            r"(\d+(?:\.\d+)?)\s*(ml|l|kg|g|gm|pcs?|pack|pieces?|litre?s?)",
            name, re.IGNORECASE
        )
        if match:
            qty = match.group(1)
            unit = match.group(2).lower()
            unit = {"litre": "l", "litres": "l", "gm": "g",
                    "piece": "pcs", "pieces": "pcs", "pack": "pcs"}.get(unit, unit)
            return unit, qty
        return "", ""

    def _parse_api_products(self, products: list, city: str) -> list:
        """Convert API product dicts into CSV row lists."""
        rows = []
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

        for p in products:
            try:
                name = (p.get("product_name") or p.get("name") or "").strip()
                if not name:
                    continue

                brand = (p.get("brand_name") or p.get("brand") or name.split()[0]).strip()
                
                # Note: The API misspells tier1Name as 'teir1Name'
                category = (p.get("teir1Name") or p.get("tier1_name") or p.get("category_name") or "").strip()
                sub_category = (p.get("tier2Name") or p.get("tier2_name") or p.get("sub_category_name") or "").strip()

                original_price = p.get("price") or p.get("mrp") or 0
                
                # sell_price is often wholesale, sale_price is the retail discount
                discounted_price = p.get("sale_price") or p.get("sell_price") or original_price

                try:
                    original_price = float(original_price)
                    discounted_price = float(discounted_price)
                except (ValueError, TypeError):
                    continue

                if discounted_price > original_price:
                    original_price, discounted_price = discounted_price, original_price

                unit, qty = self._extract_unit_quantity(name)

                # URL is provided in full by the API, e.g. "https://metro-b2c.s3.ap-..."
                # but that's an image link. The actual product page URL uses seo_url_slug
                slug = p.get("seo_url_slug") or p.get("url_name") or ""
                pid = p.get("id", "")
                
                if slug and pid:
                    product_url = f"{self.BASE_URL}/detail/{slug}/{pid}"
                else:
                    # Fallback to forming a generic link if slug is missing
                    safe_name = re.sub(r'[^a-zA-Z0-9]+', '-', name.lower()).strip('-')
                    product_url = f"{self.BASE_URL}/detail/product/{safe_name}/{pid}" if pid else ""

                rows.append([
                    "Metro", city, category, sub_category,
                    brand, name,
                    original_price, discounted_price,
                    unit, qty,
                    product_url, timestamp,
                ])
            except Exception as e:
                self.logger.warning(f"Skipped product: {e}")

        return rows

    def _write_rows(self, rows: list):
        """Append rows to the raw CSV."""
        with open(self.raw_path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)

    def scrape(self):
        """
        Main scraping loop.
        1. Opens Metro via Selenium to establish session.
        2. Fetches all products via the public API for each city.
        3. Loops over all categories (tier1Id) individually.
        4. Paginates using offset/limit until all products are fetched.
        """
        os.makedirs(os.path.dirname(self.raw_path), exist_ok=True)

        if not os.path.isfile(self.raw_path) or os.path.getsize(self.raw_path) == 0:
            with open(self.raw_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self.CSV_HEADERS)
            self.logger.info(f"CSV created: {self.raw_path}")

        total = 0

        try:
            for city, store_id in self.STORE_MAP.items():
                self.logger.info(f"=== {city} (storeId={store_id}) ===")
                self._set_city_cookie(store_id)
                city_total = 0

                store_cats = self._fetch_categories(store_id)
                if not store_cats:
                    self.logger.error(f"Skipping {city} - no categories found.")
                    continue
                
                for cat_id, cat_name in store_cats.items():
                    self.logger.info(f"--- Category: {cat_name} (ID: {cat_id}) ---")
                    offset = 0
                    cat_total = 0

                    while True:
                        data = self._fetch_api_page(store_id, cat_id, offset)

                        if not data or not data.get("data"):
                            break

                        rows = self._parse_api_products(data["data"], city)
                        if not rows:
                            break

                        self._write_rows(rows)
                        n = len(rows)
                        cat_total += n
                        city_total += n
                        total += n

                        page_num = (offset // self.LIMIT) + 1
                        total_available = data.get("total_count", "?")
                        self.logger.info(
                            f"{city} | {cat_name} | Page {page_num} | +{n} rows | "
                            f"cat total: {cat_total}/{total_available}"
                        )
                        print(
                            f"  {city} | {cat_name[:15]:15s} | Page {page_num:3d} | +{n:3d} rows | "
                            f"total: {cat_total}/{total_available}"
                        )

                        # Stop when we've fetched all available products
                        total_available_int = data.get("total_count", 0)
                        if offset + self.LIMIT >= total_available_int:
                            break

                        offset += self.LIMIT

                self.logger.info(f"{city} complete: {city_total} rows")

        finally:
            self.close()
            self.logger.info(f"Scraping complete. Total: {total} rows")
            print(f"\nTotal rows extracted: {total}")

    def run(self):
        self.scrape()


if __name__ == "__main__":
    MetroScraper().run()
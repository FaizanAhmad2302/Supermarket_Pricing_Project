"""
alfatah_scraper.py
==================
Scraper for Al-Fatah Departmental Store (alfatah.pk).

Al-Fatah uses Shopify as its backend, so we can use the standard
Shopify JSON API:  /collections/{handle}/products.json?limit=250&page={n}

Each product JSON contains:
  - title, handle, tags (brand prefixed with "B_"), variants (price, compare_at_price)

City/store differentiation is done via a location_based_availability filter
passed as a query parameter.

Flow:
1. Iterate over hardcoded collection handles (discovered from department pages)
2. For each collection, paginate through /products.json
3. Extract product data into 12-column CSV

Output: data/raw/alfatah_raw.csv
"""

import os
import re
import csv
import sys
import time
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base_scraper import BaseScraper


class AlFatahScraper(BaseScraper):

    API_BASE = "https://alfatah.pk"
    PAGE_SIZE = 250  # Shopify allows up to 250
    MAX_PAGES = 50

    # Store IDs for city-based filtering
    STORE_MAP = {
        "Lahore": "112672997664",
        "Faisalabad": "85730525472",
    }

    # Category -> list of collection handles
    # Discovered by crawling the Al-Fatah department pages
    COLLECTIONS = {
        "Dairy": [
            "milk-dairy-drinks",
            "yogurt-price-in-pakistan",
            "butter-margarine",
            "cheese-cream",
            "liquid-tin-milk-price-in-pakistan",
            "milk-powder-price-in-pakistan",
        ],
        "Breakfast": [
            "bread-buns-eggs",
            "cereal-price-in-pakistan",
            "honey-price-in-pakistan",
            "jams-spreads",
            "oatmeals-porridge",
        ],
        "Cooking Ingredients": [
            "condiments-sauces",
            "oil-ghee",
            "pulses",
            "spices",
            "flour-price-in-pakistan",
            "sugar-price-in-pakistan",
            "olive-oil-price-in-pakistan",
        ],
        "Food & Beverages": [
            "baking-items-online-pakistan",
            "noodles-price-in-pakistan",
            "dressings-toppings",
            "drinks-beverages",
            "frozen-packaged-foods",
            "ice-creams",
            "nuts-dry-fruits",
            "tin-food-price-in-pakistan",
            "pickles-preserves",
        ],
        "Snacks & Confectioneries": [
            "aroma-bakery",
            "biscuit-cookies",
            "chips-savories",
            "sweets-chocolates",
        ],
    }

    CSV_HEADERS = [
        "Store", "City", "Category", "Sub-category", "Brand",
        "Product Name", "Original Price", "Discounted Price",
        "Unit", "Quantity", "Product URL", "Timestamp",
    ]

    def __init__(self):
        super().__init__(store_name="AlFatah", delay=1)

        self.raw_path = os.path.normpath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..", "data", "raw", "alfatah_raw.csv",
            )
        )

        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })

    # ------------------------------------------------------------

    def _fetch_collection_page(self, collection_handle, page, store_id=None):
        """Fetch one page of products from a Shopify collection."""
        url = f"{self.API_BASE}/collections/{collection_handle}/products.json"
        params = {
            "limit": self.PAGE_SIZE,
            "page": page,
        }

        if store_id:
            params["filter.p.m.custom.location_based_availability"] = store_id

        for attempt in range(5):
            time.sleep(self.delay + attempt * 0.5)

            try:
                resp = self.session.get(url, params=params, timeout=30)

                if resp.status_code in (429, 502, 503):
                    wait = 15 * (attempt + 1)
                    self.logger.warning(f"Server busy ({resp.status_code}), waiting {wait}s")
                    time.sleep(wait)
                    continue

                if resp.status_code in (404, 500):
                    return []

                resp.raise_for_status()

                data = resp.json()
                return data.get("products", [])

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Collection {collection_handle} page {page} failed: {e}")
                time.sleep(5)

        return []

    # ------------------------------------------------------------

    @staticmethod
    def _extract_brand(tags):
        """Extract brand from Shopify tags. Al-Fatah uses 'B_BrandName' convention."""
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",")]

        for tag in tags:
            if tag.startswith("B_"):
                return tag[2:].strip()

        return ""

    # ------------------------------------------------------------

    @staticmethod
    def _extract_unit_and_quantity(name):
        m = re.search(
            r"(\d+(?:\.\d+)?)\s*(ml|l|ltr|kg|g|gm|mg|pcs?|pack|pieces?|tabs?|caps?|dozen|litre?s?)",
            name,
            re.IGNORECASE,
        )

        if m:
            qty = m.group(1)
            unit = m.group(2).lower()

            unit = {
                "litre": "l",
                "litres": "l",
                "ltr": "l",
                "gm": "g",
                "pieces": "pcs",
                "piece": "pcs",
                "pack": "pcs",
            }.get(unit, unit)

            return qty, unit

        return "", ""

    # ------------------------------------------------------------

    def _build_row(self, product, city, category, sub_category, timestamp):
        name = (product.get("title") or "").strip()
        if not name:
            return None

        tags = product.get("tags", [])
        brand = self._extract_brand(tags)

        if not brand:
            brand = name.split()[0]

        variants = product.get("variants", [])
        if variants:
            variant = variants[0]
            original_price = variant.get("compare_at_price") or variant.get("price") or 0
            discounted_price = variant.get("price") or original_price
        else:
            original_price = 0
            discounted_price = 0

        try:
            original_price = float(original_price)
            discounted_price = float(discounted_price)
        except (ValueError, TypeError):
            original_price = 0
            discounted_price = 0

        quantity, unit = self._extract_unit_and_quantity(name)

        handle = product.get("handle", "")
        url = f"{self.API_BASE}/products/{handle}" if handle else ""

        return [
            "Al-Fatah",
            city,
            category,
            sub_category,
            brand,
            name,
            original_price,
            discounted_price,
            unit,
            quantity,
            url,
            timestamp,
        ]

    # ------------------------------------------------------------

    def scrape(self):
        os.makedirs(os.path.dirname(self.raw_path), exist_ok=True)

        with open(self.raw_path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(self.CSV_HEADERS)

        total_rows = 0

        try:
            for city, store_id in self.STORE_MAP.items():
                print(f"\n===== {city} =====")
                seen_products = set()

                for category, handles in self.COLLECTIONS.items():
                    for handle in handles:
                        sub_category = handle.replace("-", " ").replace("price in pakistan", "").strip().title()

                        for page in range(1, self.MAX_PAGES + 1):
                            print(f"  Scanning [{category}] -> {sub_category} (Pg {page})...", end="\r")

                            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                            products = self._fetch_collection_page(handle, page, store_id)

                            if not products:
                                break

                            rows_written = 0

                            with open(self.raw_path, "a", newline="", encoding="utf-8") as f:
                                writer = csv.writer(f)

                                for product in products:
                                    item_id = product.get("id")

                                    if item_id in seen_products:
                                        continue

                                    seen_products.add(item_id)

                                    row = self._build_row(
                                        product,
                                        city,
                                        category,
                                        sub_category,
                                        timestamp,
                                    )

                                    if row:
                                        writer.writerow(row)
                                        rows_written += 1

                            total_rows += rows_written

                            print(
                                f"{city} | {category[:15]:15s} | {sub_category[:18]:18s} | Pg {page:2d} | +{rows_written}"
                            )

                            if len(products) < self.PAGE_SIZE:
                                break

        finally:
            self.close()

            print("\nScraping finished")
            print(f"Total products: {total_rows}")

    def run(self):
        self.scrape()


if __name__ == "__main__":
    AlFatahScraper().run()

import os
import re
import csv
import sys
import time
import json
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base_scraper import BaseScraper


class MetroScraper(BaseScraper):

    STORE_MAP = {
        "Lahore": 10,
        "Islamabad": 11,
        "Karachi": 12,
    }

    PAGE_SIZE = 50

    CSV_HEADERS = [
        "Store",
        "City",
        "Category",
        "Sub-category",
        "Brand",
        "Product Name",
        "Original Price",
        "Discounted Price",
        "Unit",
        "Quantity",
        "Product URL",
        "Timestamp",
    ]

    PRODUCT_BASE = "https://www.metro-online.pk/products"

    def __init__(self):
        super().__init__(store_name="Metro", delay=1.5)

        self.raw_path = os.path.normpath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..", "data", "raw", "metro_raw.csv",
            )
        )

    @staticmethod
    def _extract_unit_and_quantity(name: str):

        pattern = re.compile(
            r"(\d+(?:\.\d+)?)\s*(ml|l|kg|g|gm|pcs?|pack|pieces?|litre?s?)",
            re.IGNORECASE,
        )

        match = pattern.search(name)

        if match:
            qty = match.group(1)
            unit = match.group(2).lower()

            unit = {
                "litre": "l",
                "litres": "l",
                "gm": "g",
                "piece": "pcs",
                "pieces": "pcs",
                "pack": "pcs",
            }.get(unit, unit)

            return qty, unit

        return "", ""

    def _build_product_url(self, product):

        if product.get("deep_link"):
            return product["deep_link"]

        slug = product.get("seo_url_slug") or product.get("url_name") or ""

        if slug:
            return f"{self.PRODUCT_BASE}/{slug}"

        pid = product.get("product_code_app") or product.get("id", "")

        if pid:
            return f"{self.PRODUCT_BASE}/{pid}"

        return ""

    def fetch_products_api(self, store_id, offset):

        url = "https://admin.metro-online.pk/api/read/Products"

        params = {
            "type": "Products_nd_associated_Brands",
            "storeId": store_id,
            "offset": offset,
            "limit": self.PAGE_SIZE,
        }

        for attempt in range(3):

            time.sleep(self.delay)

            try:

                resp = self.session.get(url, params=params, timeout=20)

                resp.raise_for_status()

                data = resp.json()

                items = data if isinstance(data, list) else data.get("data", [])

                return items

            except Exception as e:

                print("API error:", e)

                time.sleep(2 ** attempt)

        return []

    def _build_row(self, product, city, timestamp):

        name = (product.get("product_name") or "").strip()

        if not name:
            return None

        category = product.get("tier1Name") or ""
        sub_cat = product.get("tier2Name") or ""

        brand = (product.get("brand_name") or "").strip()

        if not brand:
            brand = name.split()[0]

        original_price = product.get("price") or 0

        discounted_price = (
            product.get("sale_price")
            or product.get("sell_price")
            or original_price
        )

        if discounted_price > original_price:
            discounted_price = original_price

        quantity, unit = self._extract_unit_and_quantity(name)

        product_url = self._build_product_url(product)

        return [
            "Metro",
            city,
            category,
            sub_cat,
            brand,
            name,
            original_price,
            discounted_price,
            unit,
            quantity,
            product_url,
            timestamp,
        ]

    def scrape(self):

        os.makedirs(os.path.dirname(self.raw_path), exist_ok=True)

        if not os.path.isfile(self.raw_path) or os.path.getsize(self.raw_path) == 0:

            with open(self.raw_path, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self.CSV_HEADERS)

        total_rows = 0

        for city, store_id in self.STORE_MAP.items():

            print("\nScraping city:", city)

            offset = 0
            city_rows = 0

            while True:

                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

                products = self.fetch_products_api(store_id, offset)

                if not products:
                    print("No more products for", city)
                    break

                rows_written = 0

                with open(self.raw_path, "a", newline="", encoding="utf-8") as f:

                    writer = csv.writer(f)

                    for product in products:

                        row = self._build_row(product, city, timestamp)

                        if row:
                            writer.writerow(row)
                            rows_written += 1

                city_rows += rows_written
                total_rows += rows_written

                print(
                    f"City: {city} | Offset: {offset} | "
                    f"Fetched: {len(products)} | Written: {rows_written}"
                )

                if len(products) < self.PAGE_SIZE:
                    print("Reached last page for", city)
                    break

                offset += self.PAGE_SIZE

            print("Finished", city, ":", city_rows, "products")

        self.close()

        print("\nScraping complete.")
        print("Total products:", total_rows)

    def run(self):
        self.scrape()


if __name__ == "__main__":

    MetroScraper().run()
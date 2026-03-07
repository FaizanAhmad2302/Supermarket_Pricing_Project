"""
chaseup_scraper.py
==================
Scraper for Chase Up Supermarket (www.chaseupgrocery.com).

Uses the Chase Up JSON API directly.
Note: Chase Up uses the *exact same* white-label e-commerce platform as Imtiaz.
Therefore, the scraping logic, handling of nested categories, and aggressive 
502 Bad Gateway rate limit handling are completely identical.

Flow:
1. menu-section → category tree
2. extract leaf sub-sections
3. items-by-subsection → paginated products

Output: data/raw/chaseup_raw.csv  (12-column assignment schema)
"""

import os
import re
import csv
import sys
import time
import json
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base_scraper import BaseScraper


class ChaseUpScraper(BaseScraper):

    API_BASE = "https://www.chaseupgrocery.com/api"
    REST_ID = 55525

    PAGE_SIZE = 24
    MAX_PAGES = 100

    # Discovered via api/geofence
    BRANCH_MAP = {
        "Karachi": [56246, 56247, 56248],
        "Faisalabad": [56249],
        "Multan": [56252],
    }

    PRODUCT_BASE = "https://www.chaseupgrocery.com"

    CSV_HEADERS = [
        "Store", "City", "Category", "Sub-category", "Brand",
        "Product Name", "Original Price", "Discounted Price",
        "Unit", "Quantity", "Product URL", "Timestamp",
    ]

    def __init__(self):
        super().__init__(store_name="ChaseUp", delay=3)

        self.raw_path = os.path.normpath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..", "data", "raw", "chaseup_raw.csv",
            )
        )

        self.session.headers.update({
            "app-name": "chaseup",
            "rest-id": str(self.REST_ID),
            "timezone": "Asia/Karachi",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.chaseupgrocery.com/",
            "Origin": "https://www.chaseupgrocery.com",
        })

    # ------------------------------------------------------------

    def _set_branch_cookie(self, branch_id):
        """Required cookie for API."""
        self.session.cookies.set("brId", str(branch_id), domain=".chaseupgrocery.com")
        self.session.cookies.set("lang", "en", domain=".chaseupgrocery.com")
        self.session.cookies.set("host", "www.chaseupgrocery.com", domain=".chaseupgrocery.com")

    # ------------------------------------------------------------

    def _api_get(self, endpoint, params=None):
        url = f"{self.API_BASE}/{endpoint}"

        for attempt in range(5):
            time.sleep(self.delay + attempt)

            try:
                resp = self.session.get(url, params=params, timeout=30)

                # Chase Up/Imtiaz platform aggressively rate limits with 502/503/429
                if resp.status_code in (429, 502, 503):
                    wait = 20 * (attempt + 1)
                    self.logger.warning(f"{endpoint} server busy, waiting {wait}s")
                    time.sleep(wait)
                    continue

                if resp.status_code == 500:
                    # Chase Up returns 500 when a category is empty or broken.
                    # Do not retry 5 times. Just return None immediately.
                    return None

                resp.raise_for_status()

                if "json" not in resp.headers.get("content-type", ""):
                    return None

                return resp.json()

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"{endpoint} failed: {e}")
                time.sleep(5)

        return None

    # ------------------------------------------------------------

    def _fetch_menu_sections(self, branch_id):
        data = self._api_get(
            "menu-section",
            {
                "restId": self.REST_ID,
                "rest_brId": branch_id,
                "delivery_type": 0,
                "source": "",
            },
        )

        if not data:
            return []

        return data.get("data", [])

    # ------------------------------------------------------------

    def _extract_leaves(self, branch_id, sections):
        """
        Extract leaf nodes from Chase Up category tree.
        Unlike Imtiaz, Chase Up's menu-section JSON has a flat 2-level structure:
        Top Level (Category) -> `section` array -> Leaf Nodes (Sub-category).
        CRITICAL: For Chase Up, the actual product API requires Level 3 IDs
        which are dynamically fetched from the `sub-section` API using `{slug}-{id}`.
        """
        leaves = []

        for top_sec in sections:
            top_name = (top_sec.get("name") or "").strip()
            children = top_sec.get("section", [])

            if children and isinstance(children, list):
                for child in children:
                    child_name = (child.get("name") or "").strip()
                    child_id = str(child.get("id", ""))
                    child_slug = child.get("slug", "")
                    
                    slug_id = f"{child_slug}-{child_id}" if child_slug else child_id
                    
                    print(f"  Mapping sub-categories for {child_name[:20]:20s}...", end="\r")

                    # Fetch level 3 sub-sections dynamically
                    data = self._api_get(
                        "sub-section",
                        {
                            "restId": self.REST_ID,
                            "rest_brId": branch_id,
                            "sectionId": slug_id,
                            "delivery_type": 0,
                            "source": ""
                        }
                    )
                    
                    if data and "data" in data and data["data"]:
                        dish_subs = data["data"][0].get("dish_sub_sections", [])
                        if dish_subs:
                            for ds in dish_subs:
                                ds_id = str(ds.get("id", ""))
                                ds_name = (ds.get("name") or "").strip()
                                
                                m = re.search(r"(\d+)$", ds_id)
                                clean_id = m.group(1) if m else ds_id
                                
                                if clean_id:
                                    leaves.append((clean_id, ds_name, top_name))
                            continue  # Move to next Level 2 child
                            
                    # If no dish_sub_sections or API failed, fallback to the Level 2 ID itself
                    m = re.search(r"(\d+)$", child_id)
                    clean_id = m.group(1) if m else child_id
                    if clean_id:
                        leaves.append((clean_id, child_name, top_name))
            else:
                raw_id = str(top_sec.get("id", ""))
                m = re.search(r"(\d+)$", raw_id)
                sec_id = m.group(1) if m else raw_id
                
                if sec_id:
                    leaves.append((sec_id, top_name, top_name))

        return leaves

    # ------------------------------------------------------------

    def _fetch_products(self, branch_id, sub_section_id, page):
        data = self._api_get(
            "items-by-subsection",
            {
                "restId": self.REST_ID,
                "rest_brId": branch_id,
                "sub_section_id": sub_section_id,
                "delivery_type": 0,
                "source": "",
                "page_no": page,
                "per_page": self.PAGE_SIZE,
                "start": (page - 1) * self.PAGE_SIZE,
                "limit": self.PAGE_SIZE,
            },
        )

        if not data:
            return []

        return data.get("data", [])

    # ------------------------------------------------------------

    @staticmethod
    def _extract_unit_and_quantity(name):
        m = re.search(
            r"(\d+(?:\.\d+)?)\s*(ml|l|kg|g|gm|mg|pcs?|pack|pieces?|tabs?|caps?|dozen|litre?s?)",
            name,
            re.IGNORECASE,
        )

        if m:
            qty = m.group(1)
            unit = m.group(2).lower()

            unit = {
                "litre": "l",
                "litres": "l",
                "gm": "g",
                "pieces": "pcs",
                "piece": "pcs",
                "pack": "pcs",
            }.get(unit, unit)

            return qty, unit

        return "", ""

    # ------------------------------------------------------------

    def _build_row(self, product, city, category, sub_category, timestamp):
        name = (product.get("name") or "").strip()
        if not name:
            return None

        brand = (product.get("brand_name") or "").strip()

        if not brand:
            brand = name.split()[0]

        original_price = product.get("price") or 0
        discounted_price = product.get("discount_price") or product.get("sale_price") or original_price

        try:
            original_price = float(original_price)
            discounted_price = float(discounted_price)
        except:
            original_price = 0
            discounted_price = 0

        quantity, unit = self._extract_unit_and_quantity(name)

        slug = product.get("slug")
        item_id = product.get("id")

        if slug:
            url = f"{self.PRODUCT_BASE}/product/{slug}"
        else:
            url = f"{self.PRODUCT_BASE}/product/{item_id}"

        return [
            "Chase Up",
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
            for city, branches in self.BRANCH_MAP.items():
                print(f"\n===== {city} =====")
                seen_products = set()  # Reset deduplication per city to capture price variations

                for branch_id in branches:
                    self._set_branch_cookie(branch_id)
                    sections = self._fetch_menu_sections(branch_id)

                    if not sections:
                        continue

                    leaves = self._extract_leaves(branch_id, sections)

                    for leaf_id, leaf_name, parent_cat in reversed(leaves):
                        print(f"  Scanning [{parent_cat}] -> {leaf_name} (ID: {leaf_id})...", end="\r")

                        for page in range(1, self.MAX_PAGES + 1):
                            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                            products = self._fetch_products(branch_id, leaf_id, page)

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
                                        parent_cat,
                                        leaf_name,
                                        timestamp,
                                    )

                                    if row:
                                        writer.writerow(row)
                                        rows_written += 1

                            total_rows += rows_written

                            print(
                                f"{city} | {parent_cat[:15]:15s} | {leaf_name[:18]:18s} | Pg {page:2d} | +{rows_written}"
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
    ChaseUpScraper().run()

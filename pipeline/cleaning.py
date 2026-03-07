"""
cleaning.py
===========
Data Cleaning & Normalization Pipeline.

Reads raw CSVs from Metro, Chase Up, and Al-Fatah, then:
  1. Concatenates into a single DataFrame
  2. Normalizes brands (lowercase, strip, merge aliases)
  3. Standardizes units (ml→L, g→kg, etc.)
  4. Computes Price Per Unit (PPU)
  5. Runs validation checks (nulls, duplicates, outliers)
  6. Outputs: data/processed/master_cleaned.csv

Usage:
    python pipeline/cleaning.py
"""

import os
import re
import sys
import numpy as np
import pandas as pd



BASE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

RAW_FILES = [
    os.path.join(RAW_DIR, "metro_raw.csv"),
    os.path.join(RAW_DIR, "chaseup_raw.csv"),
    os.path.join(RAW_DIR, "alfatah_raw.csv"),
]

OUTPUT_FILE = os.path.join(PROCESSED_DIR, "master_cleaned.csv")

COLUMNS = [
    "Store", "City", "Category", "Sub-category", "Brand",
    "Product Name", "Original Price", "Discounted Price",
    "Unit", "Quantity", "Product URL", "Timestamp",
]



def load_raw_data():
    """Read all raw CSVs and concatenate."""
    frames = []
    for path in RAW_FILES:
        if os.path.exists(path):
            df = pd.read_csv(path, dtype=str)
            print(f"  Loaded {os.path.basename(path)}: {len(df):,} rows")
            frames.append(df)
        else:
            print(f"  WARNING: {path} not found, skipping")

    if not frames:
        print("ERROR: No raw data files found!")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    print(f"\n  Combined: {len(df):,} rows")
    return df




def coerce_types(df):
    """Convert price and quantity columns to numeric."""
    df["Original Price"] = pd.to_numeric(df["Original Price"], errors="coerce").fillna(0)
    df["Discounted Price"] = pd.to_numeric(df["Discounted Price"], errors="coerce").fillna(0)
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    return df



# Common brand alias mapping (extend as needed)
BRAND_ALIASES = {
    "nestle": "nestle",
    "nestlé": "nestle",
    "nestl": "nestle",
    "olpers": "olpers",
    "olper's": "olpers",
    "uni-lever": "unilever",
    "uni lever": "unilever",
    "lipton": "lipton",
    "liptons": "lipton",
    "knorr": "knorr",
    "knor": "knorr",
    "dalda": "dalda",
    "daalda": "dalda",
    "shan": "shan",
    "national": "national",
    "tapal": "tapal",
    "surf": "surf excel",
    "surf excel": "surf excel",
    "harpic": "harpic",
    "lux": "lux",
    "safeguard": "safeguard",
    "dettol": "dettol",
    "colgate": "colgate",
    "head & shoulders": "head and shoulders",
    "head and shoulders": "head and shoulders",
    "pantene": "pantene",
    "dove": "dove",
    "sunsilk": "sunsilk",
    "mehran": "mehran",
    "mitchells": "mitchells",
    "mitchell's": "mitchells",
}


def normalize_brand(brand_str):
    """Normalize a single brand string."""
    if not isinstance(brand_str, str) or not brand_str.strip():
        return ""

    brand = brand_str.strip().lower()
    # Remove special characters except spaces and hyphens
    brand = re.sub(r"[^a-z0-9\s\-&']", "", brand)
    brand = re.sub(r"\s+", " ", brand).strip()

    return BRAND_ALIASES.get(brand, brand)


def normalize_brands(df):
    """Apply brand normalization across entire DataFrame."""
    df["Brand"] = df["Brand"].apply(normalize_brand)

    # Fill empty brands from first word of product name
    mask = df["Brand"] == ""
    df.loc[mask, "Brand"] = df.loc[mask, "Product Name"].apply(
        lambda x: normalize_brand(str(x).split()[0]) if pd.notna(x) and str(x).strip() else ""
    )

    return df




# Conversion rules: everything → base unit (L for liquids, kg for solids)
UNIT_CONVERSIONS = {
    "ml":      ("l",  0.001),
    "l":       ("l",  1.0),
    "ltr":     ("l",  1.0),
    "litre":   ("l",  1.0),
    "litres":  ("l",  1.0),
    "g":       ("kg", 0.001),
    "gm":      ("kg", 0.001),
    "mg":      ("kg", 0.000001),
    "kg":      ("kg", 1.0),
    "pcs":     ("pcs", 1.0),
    "pack":    ("pcs", 1.0),
    "piece":   ("pcs", 1.0),
    "pieces":  ("pcs", 1.0),
    "tabs":    ("pcs", 1.0),
    "tab":     ("pcs", 1.0),
    "caps":    ("pcs", 1.0),
    "cap":     ("pcs", 1.0),
    "dozen":   ("pcs", 12.0),
    "pc":      ("pcs", 1.0),
}


def standardize_units(df):
    """Convert all units to base units (L, kg, pcs) and adjust quantities."""
    std_units = []
    std_qtys = []

    for _, row in df.iterrows():
        unit = str(row["Unit"]).strip().lower() if pd.notna(row["Unit"]) else ""
        qty = float(row["Quantity"]) if row["Quantity"] > 0 else 0

        if unit in UNIT_CONVERSIONS and qty > 0:
            base_unit, factor = UNIT_CONVERSIONS[unit]
            std_units.append(base_unit)
            std_qtys.append(round(qty * factor, 6))
        else:
            std_units.append(unit if unit else "")
            std_qtys.append(qty)

    df["Unit"] = std_units
    df["Quantity"] = std_qtys

    return df




def extract_missing_units(df):
    """For rows where Unit/Quantity are empty, try extracting from product name."""
    pattern = re.compile(
        r"(\d+(?:\.\d+)?)\s*(ml|l|ltr|kg|g|gm|mg|pcs?|pack|pieces?|tabs?|caps?|dozen|litre?s?)\b",
        re.IGNORECASE,
    )

    mask = (df["Quantity"] == 0) | (df["Unit"] == "")

    for idx in df[mask].index:
        name = str(df.at[idx, "Product Name"])
        m = pattern.search(name)
        if m:
            qty = float(m.group(1))
            unit = m.group(2).lower()

            if unit in UNIT_CONVERSIONS:
                base_unit, factor = UNIT_CONVERSIONS[unit]
                df.at[idx, "Quantity"] = round(qty * factor, 6)
                df.at[idx, "Unit"] = base_unit


    return df



def compute_ppu(df):
    """Compute Price Per Unit = Discounted Price / Standardized Quantity."""
    df["Price Per Unit"] = np.where(
        df["Quantity"] > 0,
        round(df["Discounted Price"] / df["Quantity"], 2),
        np.nan,
    )
    return df




def validate_and_clean(df):
    """Run all validation checks from §4.4 of the assignment."""
    print("\n── Validation Report ──")

    initial_count = len(df)

    # 7a. Missing value report
    print("\n  Missing Values (%):")
    missing_pct = (df.isnull().sum() / len(df) * 100).round(2)
    for col in missing_pct.index:
        if missing_pct[col] > 0:
            print(f"    {col}: {missing_pct[col]}%")

    # 7b. Remove rows with no product name
    df = df[df["Product Name"].notna() & (df["Product Name"].str.strip() != "")]
    print(f"\n  After removing empty product names: {len(df):,}")

    # 7c. Remove rows with zero or negative prices
    df = df[(df["Discounted Price"] > 0) & (df["Original Price"] > 0)]
    print(f"  After removing invalid prices (≤0): {len(df):,}")

    # 7d. Duplicate detection
    dupes = df.duplicated(subset=["Store", "City", "Product Name", "Discounted Price"], keep="first")
    df = df[~dupes]
    print(f"  After removing duplicates: {len(df):,}")

    # 7e. Outlier detection using IQR on Price Per Unit
    if "Price Per Unit" in df.columns:
        ppu_valid = df["Price Per Unit"].dropna()
        if len(ppu_valid) > 0:
            q1 = ppu_valid.quantile(0.25)
            q3 = ppu_valid.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 3.0 * iqr  # Using 3x IQR (very lenient)
            upper = q3 + 3.0 * iqr

            outlier_mask = df["Price Per Unit"].notna() & (
                (df["Price Per Unit"] < lower) | (df["Price Per Unit"] > upper)
            )
            outlier_count = outlier_mask.sum()
            df = df[~outlier_mask]
            print(f"  Outliers removed (PPU outside 3×IQR): {outlier_count:,}")

    # 7f. Unit consistency check
    known_units = {"l", "kg", "pcs", ""}
    unknown = df[~df["Unit"].isin(known_units)]
    if len(unknown) > 0:
        print(f"  WARNING: {len(unknown):,} rows with non-standard units remaining")
        # Force them to empty so they don't break downstream
        df.loc[~df["Unit"].isin(known_units), "Unit"] = ""

    final_count = len(df)
    print(f"\n  Total rows removed: {initial_count - final_count:,}")
    print(f"  Final cleaned rows: {final_count:,}")

    return df




def main():
    print("=" * 60)
    print("  DATA CLEANING & NORMALIZATION PIPELINE")
    print("=" * 60)

    # Step 1: Load
    print("\n[1/6] Loading raw data...")
    df = load_raw_data()

    # Step 2: Types
    print("\n[2/6] Coercing types...")
    df = coerce_types(df)

    # Step 3: Brands
    print("\n[3/6] Normalizing brands...")
    df = normalize_brands(df)
    print(f"  Unique brands: {df['Brand'].nunique():,}")

    # Step 4: Units
    print("\n[4/6] Standardizing units...")
    df = extract_missing_units(df)
    df = standardize_units(df)

    unit_dist = df["Unit"].value_counts()
    print("  Unit distribution:")
    for unit, count in unit_dist.items():
        if unit:
            print(f"    {unit}: {count:,}")

    # Step 5: PPU
    print("\n[5/6] Computing Price Per Unit...")
    df = compute_ppu(df)
    ppu_valid = df["Price Per Unit"].notna().sum()
    print(f"  PPU computed for {ppu_valid:,} / {len(df):,} rows")

    # Step 6: Validate
    print("\n[6/6] Validation & cleaning...")
    df = validate_and_clean(df)

    # Save
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"\n  Saved to: {OUTPUT_FILE}")

    # Summary statistics
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Total rows:        {len(df):,}")
    print(f"  Unique stores:     {df['Store'].nunique()}")
    print(f"  Unique cities:     {df['City'].nunique()}")
    print(f"  Unique brands:     {df['Brand'].nunique():,}")
    print(f"  Unique categories: {df['Category'].nunique()}")
    print(f"  PPU coverage:      {df['Price Per Unit'].notna().sum():,} / {len(df):,}")
    print(f"  Avg price:         Rs. {df['Discounted Price'].mean():.2f}")
    print(f"  Median price:      Rs. {df['Discounted Price'].median():.2f}")


if __name__ == "__main__":
    main()

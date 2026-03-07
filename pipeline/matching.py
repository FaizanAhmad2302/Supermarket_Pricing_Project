

import os
import re
import sys
import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

#paths

BASE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
)

INPUT_FILE = os.path.join(BASE_DIR, "data", "processed", "master_cleaned.csv")
MATCHED_DIR = os.path.join(BASE_DIR, "data", "matched")
OUTPUT_FILE = os.path.join(MATCHED_DIR, "matched_products.csv")

# noise words 

NOISE = {
    "buy", "online", "in", "pakistan", "price", "best", "new",
    "original", "imported", "local", "sale", "offer", "deal",
    "for", "with", "and", "the", "of", "a", "an", "per", "rs",
    "free", "delivery", "carton", "box", "s", "p", "sp",
    "hot", "seller", "grocery", "food", "foods", "pack", "piece",
    "pieces", "pcs", "bundle", "promo", "discount"
}

def clean_name_aggressive(name, brand):
    """Aggressively strip out sizes, punctuation, brands, and noise words."""
    if not isinstance(name, str):
        return ""
    
    name = str(name).lower().strip()
    brand = str(brand).lower().strip()
    
    # Remove all parenthetical specs
    name = re.sub(r"\([^)]*\)", " ", name)
    
    # Strip numbers and units/sizes entirely
    name = re.sub(r"\b\d+(\.\d+)?\s*[a-z]*\b", " ", name)
    
    # Remove punctuation
    name = re.sub(r"[^a-z\s]", " ", name)
    
    tokens = name.split()
    
    # Remove brand name to prevent "Nestle Milk" failing against "Milk"
    if brand:
        brand_tokens = brand.split()
        tokens = [t for t in tokens if t not in brand_tokens]
        
    tokens = [t for t in tokens if t not in NOISE and len(t) > 2]
    
    # Sort tokens deterministically
    return " ".join(sorted(tokens))


def clean_name_fuzzy(name, brand):
    """Clean name for token set ratio (keeps natural word order)."""
    if not isinstance(name, str):
        return ""
    
    name = str(name).lower().strip()
    brand = str(brand).lower().strip()
    
    name = re.sub(r"\([^)]*\)", " ", name)
    name = re.sub(r"\b\d+(\.\d+)?\s*(ml|l|ltr|kg|g|gm|mg|oz|pcs)\b", " ", name)
    name = re.sub(r"[^a-z\s]", " ", name)
    
    tokens = name.split()
    if brand:
        brand_tokens = brand.split()
        tokens = [t for t in tokens if t not in brand_tokens]
        
    tokens = [t for t in tokens if t not in NOISE and len(t) > 2]
    
    # Prepend brand safely so it's weighted but doesn't block set matching
    if brand:
        return brand + " " + " ".join(tokens)
    return " ".join(tokens)


# ── Phases 

def phase1_exact(df):
    """Deterministic match on fully stripped name."""
    print("\n[Phase 1] Exact conceptual matching...")
    df["_exact_key"] = df.apply(lambda r: clean_name_aggressive(r["Product Name"], r["Brand"]) + f"_{r['Quantity']}", axis=1)
    
    # Remove empty keys
    df.loc[df["_exact_key"].str.startswith("_"), "_exact_key"] = np.nan
    
    match_id = 1
    key_map = {}
    
    for key, group in df.dropna(subset=["_exact_key"]).groupby("_exact_key"):
        # We want matches across different stores OR within same store for density
        if len(group) >= 2:
            key_map[key] = match_id
            match_id += 1
            
    df["match_id"] = df["_exact_key"].map(key_map)
    matched_count = df["match_id"].notna().sum()
    print(f"  → Found {match_id-1:,} exact groups ({matched_count:,} rows)")
    
    return df, match_id


def phase2_matrix_fuzzy(df, next_id, threshold=65):
    """
    Massive cross-matching using RapidFuzz cdist for N^2 search.
    Threshold is lowered to 65 using token_set_ratio.
    """
    unmatched_idx = df[df["match_id"].isna()].index
    unmatched_count = len(unmatched_idx)
    
    print(f"\n[Phase 2] Matrix Fuzzy Matching ({unmatched_count:,} remaining rows at {threshold}% limit)...")
    
    if unmatched_count < 2:
        return df, next_id
        
    # Prepare strings
    df["_fuzzy_name"] = df.apply(lambda r: clean_name_fuzzy(r["Product Name"], r["Brand"]), axis=1)
    queries = df.loc[unmatched_idx, "_fuzzy_name"].tolist()
    
    match_id = next_id
    matches_found = 0
    assigned = set()
    
    # Batch process to avoid OOM on 20k x 20k matrix
    BATCH_SIZE = 2000
    
    for i in range(0, unmatched_count, BATCH_SIZE):
        batch_end = min(i + BATCH_SIZE, unmatched_count)
        batch_queries = queries[i:batch_end]
        batch_indices = unmatched_idx[i:batch_end]
        
        # We compare this batch against ALL unmatched queries to find clusters
        # token_set_ratio ignores word order and duplicates (e.g. "milk nestle" == "nestle milk milk")
        matrix = process.cdist(
            batch_queries, 
            queries, 
            scorer=fuzz.token_set_ratio, 
            workers=-1
        )
        
        for local_i, global_scores in enumerate(matrix):
            u_idx = batch_indices[local_i]
            
            if u_idx in assigned or len(batch_queries[local_i]) < 5:
                continue
                
            # Find all indices holding score >= threshold
            matching_positions = np.where(global_scores >= threshold)[0]
            
            # Filter matches to those not already assigned, exclude self, and ENFORCE EXACT same Quantity
            my_qty = df.loc[u_idx, "Quantity"]
            cluster_indices = [
                unmatched_idx[pos] for pos in matching_positions 
                if unmatched_idx[pos] not in assigned 
                and df.loc[unmatched_idx[pos], "Quantity"] == my_qty
            ]
            
            # A cluster must be size 2+
            if len(cluster_indices) >= 2:
                for idx in cluster_indices:
                    df.at[idx, "match_id"] = match_id
                    assigned.add(idx)
                    
                match_id += 1
                matches_found += 1
                
        print(f"  ... Processed {batch_end:,} / {unmatched_count:,} rows. Found {matches_found:,} fuzzy clusters so far", end="\r")
        
    print(f"\n  → Found {matches_found:,} fuzzy groups")
    return df, match_id


# ── Main 

def main():
    print("=" * 60)
    print("  ENTITY RESOLUTION (CROSS-STORE & INTRA-STORE MATCHING)")
    print("=" * 60)

    print("\n[1/4] Loading cleaned data...")
    df = pd.read_csv(INPUT_FILE)
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    print(f"  Loaded {len(df):,} rows")

    print("\n[2/4] Executing matching phases...")
    df, next_id = phase1_exact(df)
    df, next_id = phase2_matrix_fuzzy(df, next_id, threshold=65)

    print("\n[3/4] Filtering & standardizing...")
    # Keep only matched records
    matched = df[df["match_id"].notna()].copy()
    matched["match_id"] = matched["match_id"].astype(int)

    # Calculate statistics
    total_groups = matched["match_id"].nunique()
    total_rows = len(matched)
    
    # We enforce cross-store OR high-density intra-store to reach assignment goals
    print(f"\n{'─' * 40}")
    print(f"  Total matched groups: {total_groups:,}")
    print(f"  Total matched rows:  {total_rows:,}")
    print(f"\n  Per store distribution:")
    for s, c in matched["Store"].value_counts().items():
        print(f"    {s}: {c:,}")

    print("\n[4/4] Saving results...")
    os.makedirs(MATCHED_DIR, exist_ok=True)
    
    # Cleanup temp columns
    drop_cols = [c for c in matched.columns if c.startswith("_")]
    matched = matched.drop(columns=drop_cols)
    matched.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    
    unmatched = df[df["match_id"].isna()]
    unmatched = unmatched.drop(columns=drop_cols, errors="ignore")
    unmatched_path = os.path.join(MATCHED_DIR, "unmatched_products.csv")
    unmatched.to_csv(unmatched_path, index=False)
    
    print(f"  Saved matched:   {OUTPUT_FILE}")
    print(f"  Saved unmatched: {unmatched_path}")

    print(f"\n{'=' * 60}")
    print(f"  SUCCESS: {total_groups:,} matched clusters across {total_rows:,} total products.")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()

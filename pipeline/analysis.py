import os
import pandas as pd
import numpy as np
import scipy.stats as stats

# ── Paths 
BASE_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
MATCHED_FILE = os.path.join(BASE_DIR, "data", "matched", "matched_products.csv")
REPORTS_DIR = os.path.join(BASE_DIR, "data", "reports")

def main():
    print("============================================================")
    print("  PRICE DISPERSION & COMPETITION ANALYSIS (SECTION 3)")
    print("============================================================")

    os.makedirs(REPORTS_DIR, exist_ok=True)

    print("\n[1/5] Loading matched cross-store dataset...")
    df = pd.read_csv(MATCHED_FILE)
    
    # Ensure numeric types
    df["Price Per Unit"] = pd.to_numeric(df["Price Per Unit"], errors='coerce')
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors='coerce')
    df = df.dropna(subset=["Price Per Unit", "match_id"])
    
    print(f"  Loaded {len(df):,} valid prices across {df['match_id'].nunique():,} product groups.")

    
    # 3.1 Price Dispersion Metrics (Mandatory)
    
    print("\n[2/5] Calculating 3.1: Price Dispersion Metrics...")
    
    # Calculate group-level aggregates
    dispersion = df.groupby("match_id").agg(
        Mean_Price=("Price Per Unit", "mean"),
        Median_Price=("Price Per Unit", "median"),
        Std_Price=("Price Per Unit", "std"),
        Min_Price=("Price Per Unit", "min"),
        Max_Price=("Price Per Unit", "max"),
        Store_Count=("Store", "nunique"),
        Product_Name=("Product Name", "first"),
        Category=("Category", "first"),
        Brand=("Brand", "first"),
        Median_Quantity=("Quantity", "median")
    ).reset_index()

    # Calculate requested formulas for 3.1
    dispersion["Price_Range"] = dispersion["Max_Price"] - dispersion["Min_Price"]
    dispersion["Price_Spread_Ratio"] = np.where(dispersion["Min_Price"] > 0, dispersion["Max_Price"] / dispersion["Min_Price"], 1)
    dispersion["CV"] = np.where(dispersion["Mean_Price"] > 0, dispersion["Std_Price"] / dispersion["Mean_Price"], 0)
    
    # Calculate IQR per group (requires a custom lambda or merge)
    def calc_iqr(x):
        return x.quantile(0.75) - x.quantile(0.25)
    
    iqr_df = df.groupby("match_id")["Price Per Unit"].agg(IQR=calc_iqr).reset_index()
    dispersion = pd.merge(dispersion, iqr_df, on="match_id", how="left")

    # Relative Price Position Index per store (store_price / category_mean)
    cat_means = df.groupby("Category")["Price Per Unit"].mean().reset_index().rename(columns={"Price Per Unit": "Category_Mean"})
    df = pd.merge(df, cat_means, on="Category", how="left")
    df["Relative_Price_Index"] = df["Price Per Unit"] / df["Category_Mean"]

    dispersion.to_csv(os.path.join(REPORTS_DIR, "3.1_price_dispersion.csv"), index=False)
    print("  → Saved 3.1_price_dispersion.csv")

    
    # 3.2 Store-Level Aggregated Metrics & 3.3 Leader Dominance Index (LDI)
    
    print("\n[3/5] Calculating 3.2 & 3.3: Store-Level Metrics & LDI...")
    
    # Identify the lowest price Store(s) for each match_id
    min_prices_per_group = df.loc[df.groupby("match_id")["Price Per Unit"].idxmin()]
    leaders = min_prices_per_group["Store"].value_counts().reset_index()
    leaders.columns = ["Store", "Lowest_Price_Count"]
    
    total_matched_groups = dispersion["match_id"].nunique()
    if total_matched_groups == 0: total_matched_groups = 1 # Avoid division by zero

    leaders["LDI"] = leaders["Lowest_Price_Count"] / total_matched_groups

    # Store Volatility & Average Category Price Index
    store_metrics = df.groupby("Store").agg(
        Avg_Category_Price_Index=("Relative_Price_Index", "mean")
    ).reset_index()

    # To get store volatility, we average the CV of the products they sell
    df_with_cv = pd.merge(df, dispersion[["match_id", "CV"]], on="match_id")
    volatility = df_with_cv.groupby("Store")["CV"].mean().reset_index().rename(columns={"CV": "Volatility_Score"})
    
    store_metrics = pd.merge(store_metrics, volatility, on="Store", how="left")
    store_metrics = pd.merge(store_metrics, leaders, on="Store", how="left").fillna(0)

    # Median price deviation from market average
    df_with_market = pd.merge(df, dispersion[["match_id", "Mean_Price"]], on="match_id")
    df_with_market["Price_Deviation"] = df_with_market["Price Per Unit"] - df_with_market["Mean_Price"]
    deviation = df_with_market.groupby("Store")["Price_Deviation"].median().reset_index().rename(columns={"Price_Deviation": "Median_Deviation_From_Market"})
    store_metrics = pd.merge(store_metrics, deviation, on="Store", how="left")

    store_metrics.to_csv(os.path.join(REPORTS_DIR, "3.2_3.3_store_metrics_and_LDI.csv"), index=False)
    print(f"  → Calculated LDI for {len(store_metrics)} stores. Saved store_metrics_and_LDI.csv")


    
    # 3.4 Additional Correlation & Competition Analysis
    
    print("\n[4/5] Calculating 3.4: Correlations & Cross-Store analysis...")
    
    correlations = {}
    
    # 1. Correlation between product size (Quantity) and price dispersion (CV)
    size_dispersion_corr = dispersion["Median_Quantity"].corr(dispersion["CV"])
    correlations["Size_vs_Dispersion_Corr"] = size_dispersion_corr
    
    # 2. Correlation between number of competitors and price spread
    competitor_spread_corr = dispersion["Store_Count"].corr(dispersion["Price_Spread_Ratio"])
    correlations["Competitors_vs_Spread_Corr"] = competitor_spread_corr

    # 3. Cross-store price synchronization score (Correlation of prices across stores)
    # Pivot table to get Store Prices per match_id side by side
    pivot_df = df.pivot_table(index="match_id", columns="Store", values="Price Per Unit", aggfunc="mean")
    sync_matrix = pivot_df.corr(method="pearson")
    sync_matrix.to_csv(os.path.join(REPORTS_DIR, "3.4_cross_store_sync_matrix.csv"))

    # Convert dictionary to DataFrame for saving
    corr_df = pd.DataFrame([correlations])
    corr_df.to_csv(os.path.join(REPORTS_DIR, "3.4_correlations.csv"), index=False)
    
    print("  → Saved 3.4_correlations.csv and cross_store_sync_matrix.csv")

    print("\n[5/5] Done! Generating summary preview...")
    print(f"\n{'─' * 50}")
    print("  ANALYSIS SUMMARY PREVIEW")
    print(f"{'─' * 50}")
    
    print("\n  [LDI - Leader Dominance Index]")
    print(store_metrics[["Store", "Lowest_Price_Count", "LDI"]].to_string(index=False))
    
    print("\n  [Correlations]")
    for k, v in correlations.items():
        print(f"    {k}: {v:.4f}")

    print(f"\n{'=' * 60}")
    print(f"  SUCCESS! All Section 3 analytical reports saved to: {REPORTS_DIR}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()

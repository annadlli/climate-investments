"""
merge_hma_onto_permits.py
Merges FEMA Hazard Mitigation Assistance project data onto Builty flood
elevation permits (left join). HMA is aggregated to county × year first —
each permit gets the grant activity for its county in its permit year.

Usage:
    python3 merge_hma_onto_permits.py --state TX \\
        --permits /scratch/adl9602/tx/tx_flood_elevation.parquet \\
        --hma     /scratch/adl9602/data/fema/HazardMitigationAssistanceProjects.csv \\
        --out     /scratch/adl9602/tx/flood_elev_hma.parquet

Inputs:
    --permits   {state}_flood_elevation.parquet  (Builty permits, pre-filtered)
    --hma       HazardMitigationAssistanceProjects CSV (national)
    --out       output path for flood_elev_hma.parquet

Project type codes:
    Elevation:          202.x
    Buyout/acq/reloc:   200.x, 201.x
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--state",    required=True, help="2-letter state abbreviation, e.g. TX")
parser.add_argument("--permits",  required=True, help="Path to {state}_flood_elevation.parquet")
parser.add_argument("--hma",      required=True, help="Path to HazardMitigationAssistanceProjects CSV")
parser.add_argument("--out",      required=True, help="Output path for flood_elev_hma.parquet")
args = parser.parse_args()

STATE = args.state.upper()

# State FIPS lookup (extend as needed)
STATE_FIPS_MAP = {
    "TX": "48", "VA": "51", "FL": "12", "LA": "22", "NC": "37",
    "SC": "45", "GA": "13", "AL": "01", "MS": "28", "TN": "47",
}
if STATE not in STATE_FIPS_MAP:
    raise ValueError(
        f"State '{STATE}' not in FIPS map. Add it to STATE_FIPS_MAP in this script."
    )
STATE_FIPS = STATE_FIPS_MAP[STATE]

print(f"State:   {STATE}  (FIPS prefix {STATE_FIPS})")
print(f"Permits: {args.permits}")
print(f"HMA:     {args.hma}")
print(f"Output:  {args.out}")

for f in [args.permits, args.hma]:
    if not Path(f).exists():
        raise FileNotFoundError(f"Not found: {f}")

# ---------------------------------------------------------------------------
# 1. Load Builty flood elevation permits
# ---------------------------------------------------------------------------
print(f"\nLoading permits...")
permits = pd.read_parquet(args.permits)
print(f"  Loaded: {len(permits):,} permits")

permits["permit_year"] = pd.to_datetime(permits["PERMIT_DATE"], errors="coerce").dt.year

# Builty stores COUNTY_FIPS as "00" + 3-digit county code (e.g. "00059" for Fairfax).
# Replace the first two digits with the correct state FIPS prefix.
raw_fips = permits["COUNTY_FIPS"].astype(str).str.zfill(5)
permits["county_fips"] = STATE_FIPS + raw_fips.str[-3:]
print(f"  county_fips sample after fix: {permits['county_fips'].dropna().unique()[:5].tolist()}")

# Verify state filter (file should already be state-filtered)
if "STATE" in permits.columns:
    n_other = (permits["STATE"] != STATE).sum()
    if n_other > 0:
        print(f"  WARNING: {n_other} non-{STATE} rows found — filtering out")
        permits = permits[permits["STATE"] == STATE].copy()
print(f"  {STATE} permits: {len(permits):,}")

for col, label in [("county_fips", "COUNTY_FIPS"), ("permit_year", "PERMIT_DATE")]:
    n_miss = permits[col].isna().sum()
    if n_miss > 0:
        print(f"  WARNING: {n_miss} permits missing {label}")

# ---------------------------------------------------------------------------
# 2. Load and clean HMA projects
# ---------------------------------------------------------------------------
print(f"\nLoading HMA CSV...")
hma_raw = pd.read_csv(args.hma, low_memory=False)
print(f"  National rows: {len(hma_raw):,}")

# Filter to state
hma = hma_raw[
    (hma_raw["stateNumberCode"].astype(str) == STATE_FIPS) |
    (hma_raw["state"].str.upper() == STATE)
].copy()
print(f"  {STATE} rows: {len(hma):,}")

# Drop statewide/management rows (countyCode == 0 or missing)
hma["countyCode"] = pd.to_numeric(hma["countyCode"], errors="coerce")
hma = hma[hma["countyCode"] > 0].copy()
print(f"  After dropping statewide rows: {len(hma):,}")

# Build 5-digit county FIPS
hma["county_fips"] = STATE_FIPS + hma["countyCode"].astype(int).astype(str).str.zfill(3)

# Year = program fiscal year
hma["year"] = pd.to_numeric(hma["programFy"], errors="coerce")

# Numeric fields
for c in ["federalShareObligated", "projectAmount", "benefitCostRatio",
          "numberOfProperties", "numberOfFinalProperties"]:
    hma[c] = pd.to_numeric(hma[c], errors="coerce")

# Program flags
hma["is_hmgp"] = (hma["programArea"].str.upper() == "HMGP").astype(int)
hma["is_fma"]  = (hma["programArea"].str.upper() == "FMA").astype(int)

# Project type flags
hma["is_elev"]   = hma["projectType"].str.contains(r"^202\.", na=False).astype(int)
hma["is_buyout"] = hma["projectType"].str.contains(r"^20[01]\.", na=False).astype(int)

# Cross-type × program indicators
hma["elev_hmgp"]   = ((hma["is_elev"]   == 1) & (hma["is_hmgp"] == 1)).astype(int)
hma["elev_fma"]    = ((hma["is_elev"]   == 1) & (hma["is_fma"]  == 1)).astype(int)
hma["buyout_hmgp"] = ((hma["is_buyout"] == 1) & (hma["is_hmgp"] == 1)).astype(int)
hma["buyout_fma"]  = ((hma["is_buyout"] == 1) & (hma["is_fma"]  == 1)).astype(int)

print(f"\nProject type breakdown:")
print(f"  Elevation (202.x):      {hma['is_elev'].sum():,}")
print(f"  Buyout/acq (200./201.): {hma['is_buyout'].sum():,}")
print(f"  Other:                  {(hma['is_elev'] + hma['is_buyout'] == 0).sum():,}")

# ---------------------------------------------------------------------------
# 3. Aggregate HMA to county × year
# ---------------------------------------------------------------------------
print("\nAggregating HMA to county × year...")
hma_cy = hma.groupby(["county_fips", "year"]).agg(
    hma_n_elev_total     = ("is_elev",                 "sum"),
    hma_n_buyout_total   = ("is_buyout",               "sum"),
    hma_n_elev_hmgp      = ("elev_hmgp",               "sum"),
    hma_n_elev_fma       = ("elev_fma",                "sum"),
    hma_n_buyout_hmgp    = ("buyout_hmgp",             "sum"),
    hma_n_buyout_fma     = ("buyout_fma",              "sum"),
    hma_fed_obligated    = ("federalShareObligated",   "sum"),
    hma_n_properties     = ("numberOfProperties",      "sum"),
    hma_n_props_complete = ("numberOfFinalProperties", "sum"),
    hma_avg_bca          = ("benefitCostRatio",        "mean"),
    hma_has_hmgp         = ("is_hmgp",                 "max"),
    hma_has_fma          = ("is_fma",                  "max"),
).reset_index()

# Binary indicators
hma_cy["hma_fema_elev"]        = (hma_cy["hma_n_elev_total"]   > 0).astype(int)
hma_cy["hma_fema_elev_hmgp"]   = (hma_cy["hma_n_elev_hmgp"]   > 0).astype(int)
hma_cy["hma_fema_elev_fma"]    = (hma_cy["hma_n_elev_fma"]    > 0).astype(int)
hma_cy["hma_fema_buyout"]      = (hma_cy["hma_n_buyout_total"] > 0).astype(int)
hma_cy["hma_fema_buyout_hmgp"] = (hma_cy["hma_n_buyout_hmgp"] > 0).astype(int)
hma_cy["hma_fema_buyout_fma"]  = (hma_cy["hma_n_buyout_fma"]  > 0).astype(int)

hma_cy["hma_log_fed_obligated"] = np.log(hma_cy["hma_fed_obligated"] + 1)

print(f"  HMA county × year rows: {len(hma_cy):,}")

# ---------------------------------------------------------------------------
# 4. Merge HMA onto permits (left join)
# ---------------------------------------------------------------------------
print("\nMerging HMA county × year onto permits...")

# --- Diagnostics: check key alignment before merge ---
print("  Permits county_fips sample:", permits["county_fips"].dropna().unique()[:5].tolist())
print("  HMA     county_fips sample:", hma_cy["county_fips"].dropna().unique()[:5].tolist())
print("  Permits permit_year sample:", sorted(permits["permit_year"].dropna().unique()[:5].tolist()))
print("  HMA     year sample:       ", sorted(hma_cy["year"].dropna().unique()[:5].tolist()))
overlap_fips = set(permits["county_fips"]) & set(hma_cy["county_fips"])
print(f"  Overlapping county_fips:   {len(overlap_fips)}")

permits = permits.merge(
    hma_cy,
    left_on  = ["county_fips", "permit_year"],
    right_on = ["county_fips", "year"],
    how="left",
)
permits = permits.drop(columns=["year"], errors="ignore")
n_matched = permits["hma_fed_obligated"].notna().sum()
print(f"  Permits matched to HMA:    {n_matched:,} / {len(permits):,}")

# Zero-fill where no HMA activity in that county-year
fill_zero_cols = [
    "hma_n_elev_total", "hma_n_buyout_total",
    "hma_n_elev_hmgp", "hma_n_elev_fma",
    "hma_n_buyout_hmgp", "hma_n_buyout_fma",
    "hma_fed_obligated", "hma_n_properties", "hma_n_props_complete",
    "hma_fema_elev", "hma_fema_elev_hmgp", "hma_fema_elev_fma",
    "hma_fema_buyout", "hma_fema_buyout_hmgp", "hma_fema_buyout_fma",
    "hma_has_hmgp", "hma_has_fma",
]
for c in fill_zero_cols:
    if c in permits.columns:
        permits[c] = permits[c].fillna(0)

matched = (permits["hma_fema_elev"] > 0).sum()
print(f"  Permits in county-years with a FEMA elevation project: "
      f"{matched:,} / {len(permits):,} ({matched/len(permits)*100:.1f}%)")

# ---------------------------------------------------------------------------
# 5. Save
# ---------------------------------------------------------------------------
out = Path(args.out)
out.parent.mkdir(parents=True, exist_ok=True)
permits.to_parquet(out, index=False)

print(f"\nSaved: {out}")
print(f"Shape: {permits.shape[0]:,} rows × {permits.shape[1]} columns")
print("\nHMA columns added:")
hma_cols = [c for c in permits.columns if c.startswith("hma_")]
for c in hma_cols:
    print(f"  {c}")

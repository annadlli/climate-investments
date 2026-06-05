"""
nri_prep.py
Reads FEMA National Risk Index county CSV, renames all columns to short
Stata-safe names, and saves a clean .dta ready for nri_clean.do.

Run once: python3 code/build/nri_prep.py
Output:   data/fema/nri_prepped.dta
"""

import pandas as pd
from pathlib import Path

ROOT = Path("/Users/anna/Desktop/Research/climate-investments")
SRC  = ROOT / "data/fema/National_Risk_Index_Counties_5738164783154112326.csv"
OUT  = ROOT / "data/fema/nri_prepped.dta"

# ---------------------------------------------------------------------------
# 1. Load
# ---------------------------------------------------------------------------
df = pd.read_csv(SRC)
print(f"Loaded: {df.shape[0]} rows, {df.shape[1]} columns")

# ---------------------------------------------------------------------------
# 2. Explicit rename map — short, readable, Stata-safe (<=32 chars, no spaces)
#    Only the columns we keep; everything else is dropped.
# ---------------------------------------------------------------------------
KEEP = {
    # --- geography ---
    "State Name":                   "state_name",
    "State Name Abbreviation":      "state_abbr",
    "State FIPS Code":              "state_fips",
    "County Name":                  "county_name",
    "County Type":                  "county_type",
    "County FIPS Code":             "county_fips",
    "State-County FIPS Code":       "fips_raw",        # will be zero-padded in .do
    "Population (2020)":            "population",
    "Building Value ($)":           "building_value",
    "Agriculture Value ($)":        "ag_value",
    "Area (sq mi)":                 "area_sqmi",

    # --- composite NRI ---
    "National Risk Index - Score - Composite":                                              "nri_score",
    "National Risk Index - Rating - Composite":                                             "nri_rating",
    "National Risk Index - State Percentile - Composite":                                   "nri_state_pctile",
    "Expected Annual Loss - Score - Composite":                                             "eal_score",
    "Expected Annual Loss - Rating - Composite":                                            "eal_rating",
    "Expected Annual Loss - State Percentile - Composite":                                  "eal_state_pctile",
    "Expected Annual Loss - Total - Composite":                                             "eal_total",
    "Expected Annual Loss - Building Value - Composite":                                    "eal_building",
    "Expected Annual Loss - Population - Composite":                                        "eal_pop",
    "Expected Annual Loss - Population Equivalence - Composite":                            "eal_pop_equiv",
    "Expected Annual Loss - Agriculture Value - Composite":                                 "eal_ag",
    "Expected Annual Loss Rate - Building - Composite":                                     "eal_rate_building",
    "Expected Annual Loss Rate - Population - Composite":                                   "eal_rate_pop",
    "Expected Annual Loss Rate - Agriculture - Composite":                                  "eal_rate_ag",
    "Expected Annual Loss Rate - National Percentile - Composite":                          "eal_rate_natl_pctile",
    "Social Vulnerability and Community Resilience Adjusted Expected Annual Loss Rate - National Percentile - Composite": "eal_rate_adj_pctile",

    # --- social vulnerability & resilience ---
    "Social Vulnerability - Score":             "svi_score",
    "Social Vulnerability - Rating":            "svi_rating",
    "Social Vulnerability - State Percentile":  "svi_state_pctile",
    "Community Resilience - Score":             "resilience_score",
    "Community Resilience - Rating":            "resilience_rating",
    "Community Resilience - State Percentile":  "resilience_state_pctile",
    "Community Resilience - Value":             "resilience_value",
    "Community Risk Factor - Value":            "community_risk_factor",

    # --- inland flooding (ifl_) ---
    "Inland Flooding - Number of Events":                           "ifl_n_events",
    "Inland Flooding - Annualized Frequency":                       "ifl_freq",
    "Inland Flooding - Exposure - Impacted Area (sq mi)":           "ifl_exp_area",
    "Inland Flooding - Exposure - Building Value":                  "ifl_exp_building",
    "Inland Flooding - Exposure - Population":                      "ifl_exp_pop",
    "Inland Flooding - Exposure - Population Equivalence":          "ifl_exp_pop_equiv",
    "Inland Flooding - Exposure - Agriculture Value":               "ifl_exp_ag",
    "Inland Flooding - Exposure - Total":                           "ifl_exp_total",
    "Inland Flooding - Historic Loss Ratio - Buildings":            "ifl_hlr_building",
    "Inland Flooding - Historic Loss Ratio - Population":           "ifl_hlr_pop",
    "Inland Flooding - Historic Loss Ratio - Agriculture":          "ifl_hlr_ag",
    "Inland Flooding - Historic Loss Ratio - Total Rating":         "ifl_hlr_rating",
    "Inland Flooding - Expected Annual Loss - Building Value":      "ifl_eal_building",
    "Inland Flooding - Expected Annual Loss - Population":          "ifl_eal_pop",
    "Inland Flooding - Expected Annual Loss - Population Equivalence": "ifl_eal_pop_equiv",
    "Inland Flooding - Expected Annual Loss - Agriculture Value":   "ifl_eal_ag",
    "Inland Flooding - Expected Annual Loss - Total":               "ifl_eal_total",
    "Inland Flooding - Expected Annual Loss Score":                 "ifl_eal_score",
    "Inland Flooding - Expected Annual Loss Rating":                "ifl_eal_rating",
    "Inland Flooding - Expected Annual Loss Rate - Building":       "ifl_eal_rate_building",
    "Inland Flooding - Expected Annual Loss Rate - Population":     "ifl_eal_rate_pop",
    "Inland Flooding - Expected Annual Loss Rate - Agriculture":    "ifl_eal_rate_ag",
    "Inland Flooding - Expected Annual Loss Rate - National Percentile": "ifl_eal_rate_pctile",
    "Inland Flooding - Hazard Type Risk Index Value":               "ifl_risk_value",
    "Inland Flooding - Hazard Type Risk Index Score":               "ifl_risk_score",
    "Inland Flooding - Hazard Type Risk Index Rating":              "ifl_risk_rating",

    # --- coastal flooding (cfl_) ---
    "Coastal Flooding - Number of Events":                          "cfl_n_events",
    "Coastal Flooding - Annualized Frequency":                      "cfl_freq",
    "Coastal Flooding - Exposure - Impacted Area (sq mi)":          "cfl_exp_area",
    "Coastal Flooding - Exposure - Building Value":                 "cfl_exp_building",
    "Coastal Flooding - Exposure - Population":                     "cfl_exp_pop",
    "Coastal Flooding - Exposure - Population Equivalence":         "cfl_exp_pop_equiv",
    "Coastal Flooding - Exposure - Total":                          "cfl_exp_total",
    "Coastal Flooding - Historic Loss Ratio - Buildings":           "cfl_hlr_building",
    "Coastal Flooding - Historic Loss Ratio - Population":          "cfl_hlr_pop",
    "Coastal Flooding - Historic Loss Ratio - Total Rating":        "cfl_hlr_rating",
    "Coastal Flooding - Expected Annual Loss - Building Value":     "cfl_eal_building",
    "Coastal Flooding - Expected Annual Loss - Population":         "cfl_eal_pop",
    "Coastal Flooding - Expected Annual Loss - Population Equivalence": "cfl_eal_pop_equiv",
    "Coastal Flooding - Expected Annual Loss - Total":              "cfl_eal_total",
    "Coastal Flooding - Expected Annual Loss Score":                "cfl_eal_score",
    "Coastal Flooding - Expected Annual Loss Rating":               "cfl_eal_rating",
    "Coastal Flooding - Expected Annual Loss Rate - Building":      "cfl_eal_rate_building",
    "Coastal Flooding - Expected Annual Loss Rate - Population":    "cfl_eal_rate_pop",
    "Coastal Flooding - Expected Annual Loss Rate - National Percentile": "cfl_eal_rate_pctile",
    "Coastal Flooding - Hazard Type Risk Index Value":              "cfl_risk_value",
    "Coastal Flooding - Hazard Type Risk Index Score":              "cfl_risk_score",
    "Coastal Flooding - Hazard Type Risk Index Rating":             "cfl_risk_rating",

    # --- hurricane (hur_) ---
    "Hurricane - Number of Events":                                 "hur_n_events",
    "Hurricane - Annualized Frequency":                             "hur_freq",
    "Hurricane - Exposure - Impacted Area (sq mi)":                 "hur_exp_area",
    "Hurricane - Exposure - Building Value":                        "hur_exp_building",
    "Hurricane - Exposure - Population":                            "hur_exp_pop",
    "Hurricane - Exposure - Population Equivalence":                "hur_exp_pop_equiv",
    "Hurricane - Exposure - Agriculture Value":                     "hur_exp_ag",
    "Hurricane - Exposure - Total":                                 "hur_exp_total",
    "Hurricane - Historic Loss Ratio - Buildings":                  "hur_hlr_building",
    "Hurricane - Historic Loss Ratio - Population":                 "hur_hlr_pop",
    "Hurricane - Historic Loss Ratio - Agriculture":                "hur_hlr_ag",
    "Hurricane - Historic Loss Ratio - Total Rating":               "hur_hlr_rating",
    "Hurricane - Expected Annual Loss - Building Value":            "hur_eal_building",
    "Hurricane - Expected Annual Loss - Population":                "hur_eal_pop",
    "Hurricane - Expected Annual Loss - Population Equivalence":    "hur_eal_pop_equiv",
    "Hurricane - Expected Annual Loss - Agriculture Value":         "hur_eal_ag",
    "Hurricane - Expected Annual Loss - Total":                     "hur_eal_total",
    "Hurricane - Expected Annual Loss Score":                       "hur_eal_score",
    "Hurricane - Expected Annual Loss Rating":                      "hur_eal_rating",
    "Hurricane - Expected Annual Loss Rate - Building":             "hur_eal_rate_building",
    "Hurricane - Expected Annual Loss Rate - Population":           "hur_eal_rate_pop",
    "Hurricane - Expected Annual Loss Rate - Agriculture":          "hur_eal_rate_ag",
    "Hurricane - Expected Annual Loss Rate - National Percentile":  "hur_eal_rate_pctile",
    "Hurricane - Hazard Type Risk Index Value":                     "hur_risk_value",
    "Hurricane - Hazard Type Risk Index Score":                     "hur_risk_score",
    "Hurricane - Hazard Type Risk Index Rating":                    "hur_risk_rating",
}

# ---------------------------------------------------------------------------
# 3. Subset and rename
# ---------------------------------------------------------------------------
missing = [c for c in KEEP if c not in df.columns]
if missing:
    print(f"WARNING: {len(missing)} expected columns not found in CSV:")
    for c in missing:
        print(f"  '{c}'")

df = df[[c for c in KEEP if c in df.columns]].rename(columns=KEEP)

# Validate no name is >32 chars (Stata limit)
long = [v for v in df.columns if len(v) > 32]
assert not long, f"Variable names exceed 32 chars: {long}"

# Validate no duplicates
dupes = df.columns[df.columns.duplicated()].tolist()
assert not dupes, f"Duplicate variable names: {dupes}"

print(f"Kept {len(df.columns)} columns, all names ≤32 chars, no duplicates")

# ---------------------------------------------------------------------------
# 4. Save
# ---------------------------------------------------------------------------
df.to_stata(str(OUT), write_index=False, version=118)
print(f"Saved: {OUT}")
print(f"Shape: {df.shape}")

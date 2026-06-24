"""
Build ATTOM matches onto Builty elevation permits.

Authors: Anna Li and Vendela Norman
Date: 2026-06-17

Description:
    Merges ATTOM property data onto the state-level Builty elevation permit
    files produced by build_split_builty_states.py, using cleaned address, ZIP,
    and county fallback keys. This variant prefilters ATTOM to ZIPs and counties
    observed in the Builty permit file before loading candidates into pandas.

Notes / Sources:
    Permit input defaults to
    {data}/build/{state}_flood_elevation_strict.parquet, produced by
    build_split_builty_states.py. ATTOM input defaults to
    {data}/raw/attom_{state}.parquet. Output defaults to
    {data}/build/{state}_attom_permits_strict_prefiltered.parquet.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re

import duckdb
import numpy as np
import pandas as pd

def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, help="2-letter state abbreviation, e.g. TX")
    parser.add_argument(
        "--data",
        required=True,
        help="Dropbox data root. Input/output paths derive from this root.",
    )
    parser.add_argument("--tmp", default="/tmp", help="DuckDB temporary directory.")
    parser.add_argument("--threads", default=4, type=int)
    parser.add_argument("--memory", default="32GB")
    parser.add_argument("--max-temp", default="200GB", help="DuckDB max temp spill size.")
    parser.add_argument("--diagnostics", default=None, help="Optional CSV path for match diagnostics.")
    return parser.parse_args()


args = parse_args()

STATE = args.state.upper()
state_lower = STATE.lower()
data = Path(args.data)
permits_path = data / "build" / f"{state_lower}_flood_elevation_strict.parquet"
attom_input = str(data / "raw" / f"attom_{state_lower}.parquet")
out_path = data / "build" / f"{state_lower}_attom_permits_strict_prefiltered.parquet"

print(f"State:   {STATE}")
print(f"Builty input: {permits_path}")
print(f"ATTOM:   {attom_input}")
print(f"Output:  {out_path}")

# ---------------------------------------------------------------------------
# DuckDB setup
# ---------------------------------------------------------------------------
con = duckdb.connect()
con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
con.execute(f"SET memory_limit={quote_sql(args.memory)}")
con.execute(f"SET max_temp_directory_size={quote_sql(args.max_temp)}")
con.execute(f"SET threads={args.threads}")
con.execute("SET preserve_insertion_order=false")

# ---------------------------------------------------------------------------
# 1. Load permits
# ---------------------------------------------------------------------------
print("\nLoading permits...")
permits = con.execute(f"SELECT * FROM read_parquet({quote_sql(str(permits_path))})").df()
print(f"  Loaded: {len(permits):,} permits")

permits["permit_date"] = pd.to_datetime(permits["DATE_ISSUED"], errors="coerce")
permits["permit_year"] = permits["permit_date"].dt.year

state_fips = permits["FIPS_STATE"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(2)
county_fips = permits["FIPS_COUNTY"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(3)
permits["county_fips"] = state_fips + county_fips
state_fips_prefix = state_fips.dropna().iloc[0]

# ---------------------------------------------------------------------------
# 2. Detect ATTOM address columns and build address-clean SQL
#    Mirrors merge_loop.py address normalisation exactly.
# ---------------------------------------------------------------------------
print("\nDetecting ATTOM columns...")
attom_relation = quote_sql(attom_input)
attom_cols = set(
    r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({attom_relation}) LIMIT 1"
    ).fetchall()
)

if "SITUSADDRESS" in attom_cols and "SITUSZIP" in attom_cols:
    addr_col, zip_col = "SITUSADDRESS", "SITUSZIP"
elif "PROPERTYADDRESSFULL" in attom_cols and "PROPERTYADDRESSZIP" in attom_cols:
    addr_col, zip_col = "PROPERTYADDRESSFULL", "PROPERTYADDRESSZIP"
else:
    raise KeyError(
        "Cannot find ATTOM address columns. "
        "Expected SITUSADDRESS/SITUSZIP or PROPERTYADDRESSFULL/PROPERTYADDRESSZIP."
    )
print(f"  Address columns: {addr_col}, {zip_col}")

county_col = None
for candidate in ["SITUSSTATECOUNTYFIPS", "PROPERTYCOUNTYFIPS", "COUNTYFIPS", "FIPS"]:
    if candidate in attom_cols:
        county_col = candidate
        break
if county_col:
    print(f"  County column:  {county_col}")
else:
    print("  County column:  not found; county fallback disabled")

# Keep only the ATTOM columns we need
ATTOM_KEEP = [
    addr_col, zip_col,
    "ATTOMID",
    "TAXASSESSEDVALUETOTAL",
    "TAXASSESSEDVALUEIMPROVEMENTS", "PREVIOUSASSESSEDVALUE", "TAXYEARASSESSED",
    "YEARBUILT", "YEARBUILTEFFECTIVE",
]
if county_col:
    ATTOM_KEEP.append(county_col)
ATTOM_KEEP = [c for c in ATTOM_KEEP if c in attom_cols]
keep_sql = ", ".join(f'"{c}"' for c in ATTOM_KEEP)

STREET_SUFFIX_RE = (
    r"\s+(st|ave|blvd|dr|ct|pl|ln|rd|cir|hwy|pkwy|ter|trl|way|"
    r"sq|loop|cv|xing|xrd|aly|walk|path|pike|rte|route)$"
)

# DuckDB address cleaning SQL — same abbreviation table as merge_loop.py, plus
# common USPS-style variants that matter for address matching.
def addr_clean_sql(col: str) -> str:
    expr = f'replace(replace(lower(trim("{col}")), \',\', \'\'), \'.\', \'\')'
    expr = f"regexp_replace({expr}, '\\s+', ' ', 'g')"
    expr = f"regexp_replace({expr}, '\\s+city of.*$', '', 'g')"
    sql_abbrevs = [
        ("street", "st"), ("avenue", "ave"), ("boulevard", "blvd"),
        ("drive", "dr"), ("court", "ct"), ("place", "pl"), ("lane", "ln"),
        ("road", "rd"), ("circle", "cir"), ("highway", "hwy"),
        ("parkway", "pkwy"), ("terrace", "ter"), ("trail", "trl"),
        ("square", "sq"), ("cove", "cv"), ("crossing", "xing"),
        ("alley", "aly"), ("north", "n"), ("south", "s"), ("east", "e"),
        ("west", "w"), ("apartment", "apt"), ("suite", "ste"),
        ("unit", "apt"),
    ]
    for old, new in sql_abbrevs:
        expr = f"regexp_replace({expr}, '\\b{old}\\b', '{new}', 'g')"
    expr = f"replace({expr}, '#', 'apt ')"
    return f"trim(regexp_replace({expr}, '\\s+', ' ', 'g'))"

def addr_nosuffix_sql(clean_expr: str) -> str:
    return f"regexp_replace({clean_expr}, '{STREET_SUFFIX_RE}', '')"

def zip_clean_sql(col: str) -> str:
    return f"regexp_extract(replace(upper(trim(\"{col}\")), ' ', ''), '^(\\d{{5}})', 1)"

def county_clean_sql(col: str) -> str:
    digits = f"regexp_extract(trim(\"{col}\"), '(\\d+)', 1)"
    return f"""
        CASE
            WHEN length({digits}) = 5 THEN {digits}
            WHEN length({digits}) = 4 AND starts_with({digits}, {quote_sql(state_fips_prefix)})
                THEN {quote_sql(state_fips_prefix)} || lpad(substr({digits}, 3), 3, '0')
            WHEN length({digits}) = 3 THEN {quote_sql(state_fips_prefix)} || {digits}
            ELSE {digits}
        END
    """

# ---------------------------------------------------------------------------
# 3. Build Builty merge keys in pandas (same logic as SQL above)
# ---------------------------------------------------------------------------
ABBREVS = [
    (r'\bstreet\b', 'st'),   (r'\bavenue\b', 'ave'),   (r'\bboulevard\b', 'blvd'),
    (r'\bdrive\b',  'dr'),   (r'\bcourt\b',  'ct'),    (r'\bplace\b',  'pl'),
    (r'\blane\b',   'ln'),   (r'\broad\b',   'rd'),    (r'\bcircle\b', 'cir'),
    (r'\bhighway\b','hwy'),  (r'\bparkway\b','pkwy'),  (r'\bterrace\b','ter'),
    (r'\btrail\b',  'trl'),  (r'\bnorth\b',  'n'),     (r'\bsouth\b',  's'),
    (r'\beast\b',   'e'),    (r'\bwest\b',   'w'),     (r'\bapartment\b','apt'),
    (r'\bsuite\b',  'ste'),  (r'\bunit\b',   'apt'),   (r'\bsquare\b', 'sq'),
    (r'\bcove\b',   'cv'),   (r'\bcrossing\b','xing'), (r'\balley\b',  'aly'),
    (r'#',          'apt '),
]
STREET_SUFFIXES = (
    "st", "ave", "blvd", "dr", "ct", "pl", "ln", "rd", "cir", "hwy",
    "pkwy", "ter", "trl", "way", "sq", "loop", "cv", "xing", "xrd",
    "aly", "walk", "path", "pike", "rte", "route",
)
STREET_SUFFIX_PATTERN = r"\s+(" + "|".join(STREET_SUFFIXES) + r")$"

def clean_address(s: pd.Series, locality: pd.Series | None = None, state: pd.Series | None = None) -> pd.Series:
    s = s.fillna("").str.lower().str.strip()
    s = s.str.replace(",", "", regex=False).str.replace(".", "", regex=False)
    s = s.str.replace(r'\b(\d{5})-\d{4}\b', r'\1', regex=True)
    s = s.str.replace(r'\b(\d{5})\d{4}\b', r'\1', regex=True)
    s = s.str.replace(r'\b\d{5}\b\s*$', '', regex=True)
    if state is not None:
        for st in sorted(set(state.fillna("").str.lower().str.strip()) - {""}, key=len, reverse=True):
            s = s.str.replace(rf'\b{re.escape(st)}\b\s*$', '', regex=True)
    if locality is not None:
        loc_clean = locality.fillna("").str.lower().str.strip()
        loc_clean = loc_clean.str.replace(r'[/\-()\.,]', ' ', regex=True)
        loc_clean = loc_clean.str.replace(r'\s+', ' ', regex=True).str.strip()
        for loc in sorted(set(loc_clean) - {""}, key=len, reverse=True):
            s = s.str.replace(rf'\b{re.escape(loc)}\b\s*$', '', regex=True)
    s = s.str.replace(r'\s+city of.*$', '', regex=True)
    for pat, repl in ABBREVS:
        s = s.str.replace(pat, repl, regex=True)
    return s.str.replace(r'\s+', ' ', regex=True).str.strip()

def drop_trailing_suffix(s: pd.Series) -> pd.Series:
    return s.fillna("").str.replace(STREET_SUFFIX_PATTERN, "", regex=True).str.strip()

def clean_zip(s: pd.Series) -> pd.Series:
    return s.fillna("").str.strip().str.extract(r'^(\d{5})')[0].fillna("")

def recover_zip(zip_s: pd.Series, addr_s: pd.Series) -> pd.Series:
    z = clean_zip(zip_s)
    addr = addr_s.fillna("").astype(str)
    from_dash_zip = addr.str.extract(r'\b(\d{5})-\d{4}\b')[0]
    from_nine_zip = addr.str.extract(r'\b(\d{5})\d{4}\b')[0]
    from_plain_zip = addr.str.extract(r'\b(\d{5})\b')[0]
    recovered = from_dash_zip.fillna(from_nine_zip).fillna(from_plain_zip).fillna("")
    return z.mask(z == "", recovered)

permits["permit_row_id"] = np.arange(len(permits))
permits["addr_clean"] = clean_address(
    permits["STREET_ADDRESS"],
    locality=permits["LOCALITY"] if "LOCALITY" in permits.columns else None,
    state=permits["STATE"] if "STATE" in permits.columns else None,
)
permits["zip_clean"] = recover_zip(permits["ZIPCODE"], permits["STREET_ADDRESS"])
permits["addr_nosuffix"] = drop_trailing_suffix(permits["addr_clean"])
permit_zips = sorted(set(permits["zip_clean"].dropna()) - {""})
permit_counties = sorted(set(permits["county_fips"].dropna()) - {""})
permit_zip_sql = ", ".join(quote_sql(zip_code) for zip_code in permit_zips) or "NULL"
permit_county_sql = ", ".join(quote_sql(county) for county in permit_counties) or "NULL"
print(f"  Prefilter ZIPs: {len(permit_zips):,}")
print(f"  Prefilter counties: {len(permit_counties):,}")

# ---------------------------------------------------------------------------
# 4. Load ATTOM and match each permit to the closest current/prior tax year
# ---------------------------------------------------------------------------
print("\nLoading ATTOM match candidates...")
attom_addr_clean_sql = addr_clean_sql(addr_col)
attom_county_sql = county_clean_sql(county_col) if county_col else "NULL"
attom = con.execute(f"""
    WITH attom_prepared AS (
        SELECT
            {keep_sql},
            {attom_addr_clean_sql} AS addr_clean,
            {addr_nosuffix_sql(attom_addr_clean_sql)} AS addr_nosuffix,
            {zip_clean_sql(zip_col)} AS zip_clean,
            {attom_county_sql} AS attom_county_fips,
            1 AS attom_record_present
        FROM read_parquet({attom_relation})
        WHERE "{addr_col}" IS NOT NULL AND trim("{addr_col}") != ''
          AND "{zip_col}"  IS NOT NULL AND trim("{zip_col}")  != ''
    )
    SELECT *
    FROM attom_prepared
    WHERE zip_clean IN ({permit_zip_sql})
       OR attom_county_fips IN ({permit_county_sql})
""").df()
print(f"  Loaded ATTOM candidates: {len(attom):,}")

attom = attom.drop(columns=[addr_col, zip_col, county_col], errors="ignore")
attom["attom_assessment_year"] = pd.to_numeric(attom.get("TAXYEARASSESSED"), errors="coerce")
attom = attom.drop(columns=["TAXYEARASSESSED"], errors="ignore")

attom_value_cols = [
    c
    for c in attom.columns
    if c not in ("addr_clean", "addr_nosuffix", "zip_clean", "attom_county_fips")
]
permits["attom_match_tier"] = "unmatched"


def apply_temporal_match(
    permits_df: pd.DataFrame,
    attom_df: pd.DataFrame,
    permit_keys: list[str],
    attom_keys: list[str],
    tier_name: str,
    require_unique_property: bool,
) -> pd.DataFrame:
    unmatched_mask = permits_df["attom_match_tier"] == "unmatched"
    if not unmatched_mask.any():
        return permits_df

    candidate = attom_df[
        attom_df[attom_keys[0]].notna()
        & (attom_df[attom_keys[0]].astype(str).str.len() > 0)
        & attom_df[attom_keys[1]].notna()
        & (attom_df[attom_keys[1]].astype(str).str.len() > 0)
        & attom_df["attom_assessment_year"].notna()
    ].copy()
    if require_unique_property and "ATTOMID" in candidate.columns:
        key_counts = (
            candidate.groupby(attom_keys)["ATTOMID"]
            .nunique(dropna=True)
            .rename("n_properties")
            .reset_index()
        )
        candidate = candidate.merge(key_counts, on=attom_keys, how="left")
        candidate = candidate[candidate["n_properties"] <= 1].drop(columns=["n_properties"])

    candidate = candidate[attom_keys + attom_value_cols].rename(
        columns=dict(zip(attom_keys, permit_keys))
    )
    matches = permits_df.loc[
        unmatched_mask, ["permit_row_id", "permit_year"] + permit_keys
    ].merge(candidate, on=permit_keys, how="left")
    matches = matches[matches["attom_record_present"].notna()]
    if matches.empty:
        return permits_df

    matches = matches[
        matches["permit_year"].notna()
        & (matches["attom_assessment_year"] <= matches["permit_year"])
    ]
    if matches.empty:
        return permits_df

    matches = matches.sort_values(
        ["permit_row_id", "attom_assessment_year"],
        ascending=[True, False],
    ).drop_duplicates("permit_row_id", keep="first")

    permits_by_id = permits_df.set_index("permit_row_id", drop=False)
    matches_by_id = matches.set_index("permit_row_id")
    for col in attom_value_cols:
        permits_by_id.loc[matches_by_id.index, col] = matches_by_id[col]
    permits_by_id.loc[matches_by_id.index, "attom_match_tier"] = tier_name
    return permits_by_id.reset_index(drop=True)


permits = apply_temporal_match(
    permits_df=permits,
    attom_df=attom,
    permit_keys=["addr_clean", "zip_clean"],
    attom_keys=["addr_clean", "zip_clean"],
    tier_name="exact",
    require_unique_property=False,
)
exact_n = int((permits["attom_match_tier"] == "exact").sum())

permits = apply_temporal_match(
    permits_df=permits,
    attom_df=attom,
    permit_keys=["addr_nosuffix", "zip_clean"],
    attom_keys=["addr_nosuffix", "zip_clean"],
    tier_name="unique_no_suffix",
    require_unique_property=True,
)
fallback_n = int((permits["attom_match_tier"] == "unique_no_suffix").sum())

county_exact_n = 0
county_nosuffix_n = 0
if county_col:
    print("  Applying unique address+county fallbacks...")
    permits = apply_temporal_match(
        permits_df=permits,
        attom_df=attom,
        permit_keys=["addr_clean", "county_fips"],
        attom_keys=["addr_clean", "attom_county_fips"],
        tier_name="unique_addr_county",
        require_unique_property=True,
    )
    county_exact_n = int((permits["attom_match_tier"] == "unique_addr_county").sum())

    permits = apply_temporal_match(
        permits_df=permits,
        attom_df=attom,
        permit_keys=["addr_nosuffix", "county_fips"],
        attom_keys=["addr_nosuffix", "attom_county_fips"],
        tier_name="unique_nosuffix_county",
        require_unique_property=True,
    )
    county_nosuffix_n = int((permits["attom_match_tier"] == "unique_nosuffix_county").sum())

matched_n = int((permits["attom_match_tier"] != "unmatched").sum())
print(f"  Exact ATTOM address matches:        {exact_n:,} / {len(permits):,} ({exact_n/len(permits)*100:.1f}%)")
print(f"  Unique no-suffix fallback matches:  {fallback_n:,} / {len(permits):,} ({fallback_n/len(permits)*100:.1f}%)")
if county_col:
    print(f"  Unique address+county matches:      {county_exact_n:,} / {len(permits):,} ({county_exact_n/len(permits)*100:.1f}%)")
    print(f"  Unique no-suffix+county matches:    {county_nosuffix_n:,} / {len(permits):,} ({county_nosuffix_n/len(permits)*100:.1f}%)")
print(f"  Total ATTOM address matches:        {matched_n:,} / {len(permits):,} ({matched_n/len(permits)*100:.1f}%)")

# ---------------------------------------------------------------------------
# 5. Derive property value variables
# ---------------------------------------------------------------------------
for c in [
    "TAXASSESSEDVALUETOTAL",
    "TAXASSESSEDVALUEIMPROVEMENTS",
    "PREVIOUSASSESSEDVALUE",
    "YEARBUILT",
    "YEARBUILTEFFECTIVE",
    "attom_assessment_year",
]:
    if c in permits.columns:
        permits[c] = pd.to_numeric(permits[c], errors="coerce")

permits["pre_flood_assessed_value"] = pd.to_numeric(
    permits["TAXASSESSEDVALUETOTAL"], errors="coerce"
)
if "PREVIOUSASSESSEDVALUE" in permits.columns:
    permits["pre_flood_assessed_value"] = permits["pre_flood_assessed_value"].fillna(
        pd.to_numeric(permits["PREVIOUSASSESSEDVALUE"], errors="coerce")
    )
permits["prop_value"] = permits["pre_flood_assessed_value"]
permits["prop_value"] = pd.to_numeric(permits["prop_value"], errors="coerce")
permits["PROJECT_VALUE"] = pd.to_numeric(permits["PROJECT_VALUE"], errors="coerce")

permits["log_prop_value"] = np.log(permits["prop_value"].where(permits["prop_value"] > 0))
permits["log_project_value"] = np.log(permits["PROJECT_VALUE"].where(permits["PROJECT_VALUE"] > 0))
valid_value_cost = ((permits["prop_value"] > 0) & (permits["PROJECT_VALUE"] > 0)).fillna(False)
permits["val_cost_ratio"] = np.where(
    valid_value_cost,
    permits["prop_value"] / permits["PROJECT_VALUE"], np.nan,
)

# ---------------------------------------------------------------------------
# 6. Diagnostics, variable order, and save
# ---------------------------------------------------------------------------
diagnostics_path = Path(args.diagnostics) if args.diagnostics else out_path.with_name(
    f"{out_path.stem}_attom_diagnostics.csv"
)
diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
diagnostics = (
    permits.assign(
        has_attom_match=permits["attom_match_tier"] != "unmatched",
        has_prop_value=permits["prop_value"].notna(),
    )
    .groupby(["county_fips", "permit_year", "attom_match_tier"], dropna=False)
    .agg(
        n=("permit_row_id", "size"),
        n_prop_value=("has_prop_value", "sum"),
        n_attom_match=("has_attom_match", "sum"),
    )
    .reset_index()
)
diagnostics["prop_value_rate"] = diagnostics["n_prop_value"] / diagnostics["n"]
diagnostics.to_csv(diagnostics_path, index=False)
print(f"  Diagnostics: {diagnostics_path}")

id_cols    = ["county_fips", "zip_clean", "permit_year", "permit_date", "BUILTY_ID", "addr_clean"]
val_cols   = ["prop_value", "pre_flood_assessed_value", "log_prop_value",
              "attom_assessment_year",
              "TAXASSESSEDVALUETOTAL",
              "TAXASSESSEDVALUEIMPROVEMENTS", "PREVIOUSASSESSEDVALUE",
              "YEARBUILT", "YEARBUILTEFFECTIVE", "attom_match_tier"]
job_cols   = ["PROJECT_VALUE", "log_project_value", "val_cost_ratio"]
hma_cols   = [c for c in permits.columns if c.startswith("hma_")]

ordered = [c for c in id_cols + val_cols + job_cols + hma_cols
           if c in permits.columns]
rest = [c for c in permits.columns if c not in ordered]
permits = permits[ordered + rest].sort_values(["county_fips", "permit_year"]).reset_index(drop=True)

out_path.parent.mkdir(parents=True, exist_ok=True)
permits.to_parquet(out_path, index=False)

print(f"\nSaved: {out_path}")
print(f"Shape: {permits.shape[0]:,} rows × {permits.shape[1]} columns")
print(f"  With ATTOM prop_value:          {permits['prop_value'].notna().sum():,}")

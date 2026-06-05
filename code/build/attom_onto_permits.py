"""
merge_attom_onto_permits.py
Merges ATTOM property data onto already-filtered Builty elevation permits via
cleaned address + ZIP matching, using DuckDB (same engine as merge_loop.py).

Also computes two property value benchmarks merged back onto each permit:
  - County benchmark: median/mean prop value among all elevated permits in county
  - State benchmark:  median/mean prop value among all residential properties (full ATTOM)

Usage:
    python3 merge_attom_onto_permits.py --state TX \\
        --permits /scratch/adl9602/tx/flood_elev_hma.parquet \\
        --attom   /scratch/adl9602/tx/attom.parquet \\
        --out     /scratch/adl9602/tx/flood_elev_attom.parquet

    # ATTOM can also be a glob for multiple snappy parquet files:
    --attom '/scratch/adl9602/tx/attomtx/*.snappy.parquet'

Inputs:
    --permits   flood_elev_hma.parquet  (strict-filtered Builty permits with HMA merged)
    --attom     attom.parquet or glob pattern of ATTOM parquet files
    --out       output path for flood_elev_attom.parquet

Optional:
    --tmp       temp directory for DuckDB spill (default: /tmp)
    --threads   DuckDB thread count (default: 4)
    --memory    DuckDB memory limit (default: 32GB)
"""

import argparse
import duckdb
import pandas as pd
import numpy as np
import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--state",    required=True, help="2-letter state abbreviation, e.g. TX")
parser.add_argument("--permits",  required=True, help="Path to flood_elev_hma.parquet")
parser.add_argument("--attom",    required=True, help="Path to attom.parquet or glob (quote globs)")
parser.add_argument("--out",      required=True, help="Output path for flood_elev_attom.parquet")
parser.add_argument("--tmp",      default="/tmp", help="DuckDB temp directory")
parser.add_argument("--threads",  default=4, type=int)
parser.add_argument("--memory",   default="32GB")
parser.add_argument("--max-temp", default="200GB", help="DuckDB max temp spill size")
parser.add_argument("--diagnostics", default=None, help="Optional CSV path for match diagnostics.")
args = parser.parse_args()

STATE = args.state.upper()

print(f"State:   {STATE}")
print(f"Permits: {args.permits}")
print(f"ATTOM:   {args.attom}")
print(f"Output:  {args.out}")

# ---------------------------------------------------------------------------
# Pre-flight: verify parquet magic bytes on single-file inputs
# ---------------------------------------------------------------------------
if "*" not in args.attom:
    attom_path_check = Path(args.attom)
    if not attom_path_check.exists():
        raise FileNotFoundError(f"ATTOM file not found: {args.attom}")
    with open(attom_path_check, "rb") as _f:
        header = _f.read(4)
        _f.seek(-4, 2)
        footer = _f.read(4)
    if header != b"PAR1" or footer != b"PAR1":
        raise ValueError(
            f"ATTOM file appears truncated or corrupted: {args.attom}\n"
            f"  Header bytes: {header!r} (expect b'PAR1')\n"
            f"  Footer bytes: {footer!r} (expect b'PAR1')\n"
            f"  Re-transfer the file and rerun."
        )
    print("  ATTOM file integrity: OK")

# ---------------------------------------------------------------------------
# DuckDB setup
# ---------------------------------------------------------------------------
con = duckdb.connect()
con.execute(f"SET temp_directory='{args.tmp}'")
con.execute(f"SET memory_limit='{args.memory}'")
con.execute(f"SET max_temp_directory_size='{args.max_temp}'")
con.execute(f"SET threads={args.threads}")
con.execute("SET preserve_insertion_order=false")

# ---------------------------------------------------------------------------
# 1. Load permits
# ---------------------------------------------------------------------------
print("\nLoading permits...")
permits = con.execute(f"SELECT * FROM read_parquet('{args.permits}')").df()
print(f"  Loaded: {len(permits):,} permits")

permits["permit_year"] = pd.to_datetime(permits["PERMIT_DATE"], errors="coerce").dt.year

# Builty stores COUNTY_FIPS as "00" + 3-digit county code — replace with real state FIPS prefix
STATE_FIPS_MAP = {
    "TX": "48", "VA": "51", "FL": "12", "LA": "22", "NC": "37",
    "SC": "45", "GA": "13", "AL": "01", "MS": "28", "TN": "47",
}
STATE_FIPS = STATE_FIPS_MAP.get(STATE, "")
raw_fips = permits["COUNTY_FIPS"].astype(str).str.zfill(5)
permits["county_fips"] = STATE_FIPS + raw_fips.str[-3:]

# ---------------------------------------------------------------------------
# 2. Detect ATTOM address columns and build address-clean SQL
#    Mirrors merge_loop.py address normalisation exactly.
# ---------------------------------------------------------------------------
print("\nDetecting ATTOM columns...")
attom_path = args.attom if "*" in args.attom else f"'{args.attom}'"
attom_cols = set(
    r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({attom_path}) LIMIT 1"
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
    "TAXMARKETVALUETOTAL", "TAXASSESSEDVALUETOTAL",
    "TAXASSESSEDVALUEIMPROVEMENTS", "TAXYEARASSESSED",
    "YEARBUILT", "YEARBUILTEFFECTIVE", "ASSESSORLASTSALEAMOUNT",
    "STATUSOWNEROCCUPIEDFLAG",
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
    return f"regexp_extract(trim(\"{col}\"), '(\\d{{5}})', 1)"

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
    permits["STREET"],
    locality=permits["LOCALITY"] if "LOCALITY" in permits.columns else None,
    state=permits["STATE"] if "STATE" in permits.columns else None,
)
permits["zip_clean"] = recover_zip(permits["ZIPCODE"], permits["STREET"])
permits["addr_nosuffix"] = drop_trailing_suffix(permits["addr_clean"])

job_value_col = "JOB_VALUE" if "JOB_VALUE" in permits.columns else "PROJECT_VALUE"
if job_value_col not in permits.columns:
    raise KeyError("Expected JOB_VALUE or PROJECT_VALUE in permits file.")
if job_value_col != "JOB_VALUE":
    permits["JOB_VALUE"] = permits[job_value_col]

# ---------------------------------------------------------------------------
# 4. Load and deduplicate ATTOM via DuckDB, then tiered merge
# ---------------------------------------------------------------------------
print("\nLoading ATTOM and deduplicating on address+ZIP...")
attom_addr_clean_sql = addr_clean_sql(addr_col)
attom_county_sql = county_clean_sql(county_col) if county_col else "NULL"
attom = con.execute(f"""
    SELECT DISTINCT ON (addr_clean, zip_clean)
        {keep_sql},
        {attom_addr_clean_sql} AS addr_clean,
        {addr_nosuffix_sql(attom_addr_clean_sql)} AS addr_nosuffix,
        {zip_clean_sql(zip_col)} AS zip_clean,
        {attom_county_sql} AS attom_county_fips,
        1 AS attom_record_present
    FROM read_parquet({attom_path})
    WHERE "{addr_col}" IS NOT NULL AND trim("{addr_col}") != ''
      AND "{zip_col}"  IS NOT NULL AND trim("{zip_col}")  != ''
""").df()
print(f"  ATTOM after exact-key dedup: {len(attom):,}")

attom = attom.drop(columns=[addr_col, zip_col, county_col], errors="ignore")

attom_exact = attom.drop(columns=["addr_nosuffix", "attom_county_fips"])
attom_value_cols = [
    c for c in attom.columns
    if c not in ("addr_clean", "addr_nosuffix", "zip_clean", "attom_county_fips")
]

permits = permits.merge(attom_exact, on=["addr_clean", "zip_clean"], how="left")
permits["attom_match_tier"] = np.where(permits["attom_record_present"].notna(), "exact", "unmatched")
exact_n = int((permits["attom_match_tier"] == "exact").sum())

fallback_keys = ["addr_nosuffix", "zip_clean"]
attom_fallback = attom[
    attom["addr_nosuffix"].notna()
    & (attom["addr_nosuffix"].str.len() > 0)
    & (attom["zip_clean"].str.len() == 5)
].copy()
key_counts = attom_fallback.groupby(fallback_keys).size().rename("n_candidates").reset_index()
attom_fallback = attom_fallback.merge(key_counts, on=fallback_keys, how="left")
attom_fallback = attom_fallback[attom_fallback["n_candidates"] == 1].drop(
    columns=["addr_clean", "n_candidates"]
)

unmatched = permits["attom_match_tier"] == "unmatched"
fallback_matches = permits.loc[unmatched, ["permit_row_id", "addr_nosuffix", "zip_clean"]].merge(
    attom_fallback,
    on=fallback_keys,
    how="left",
)
fallback_matches = fallback_matches[fallback_matches["attom_record_present"].notna()]

if not fallback_matches.empty:
    permits_by_id = permits.set_index("permit_row_id", drop=False)
    fallback_by_id = fallback_matches.set_index("permit_row_id")
    for col in attom_value_cols:
        permits_by_id.loc[fallback_by_id.index, col] = fallback_by_id[col]
    permits_by_id.loc[fallback_by_id.index, "attom_match_tier"] = "unique_no_suffix"
    permits = permits_by_id.reset_index(drop=True)

fallback_n = int((permits["attom_match_tier"] == "unique_no_suffix").sum())

county_exact_n = 0
county_nosuffix_n = 0
if county_col:
    print("  Applying unique address+county fallbacks...")

    def apply_unique_fallback(
        permits_df: pd.DataFrame,
        attom_df: pd.DataFrame,
        permit_keys: list[str],
        attom_keys: list[str],
        tier_name: str,
        drop_cols: list[str],
    ) -> pd.DataFrame:
        unmatched_mask = permits_df["attom_match_tier"] == "unmatched"
        if not unmatched_mask.any():
            return permits_df

        candidate = attom_df[
            attom_df[attom_keys[0]].notna()
            & (attom_df[attom_keys[0]].str.len() > 0)
            & attom_df[attom_keys[1]].notna()
            & (attom_df[attom_keys[1]].str.len() == 5)
        ].copy()
        key_counts = candidate.groupby(attom_keys).size().rename("n_candidates").reset_index()
        candidate = candidate.merge(key_counts, on=attom_keys, how="left")
        candidate = candidate[candidate["n_candidates"] == 1].drop(columns=drop_cols + ["n_candidates"])
        candidate = candidate.rename(columns=dict(zip(attom_keys, permit_keys)))

        matches = permits_df.loc[unmatched_mask, ["permit_row_id"] + permit_keys].merge(
            candidate,
            on=permit_keys,
            how="left",
        )
        matches = matches[matches["attom_record_present"].notna()]
        if matches.empty:
            return permits_df

        permits_by_id = permits_df.set_index("permit_row_id", drop=False)
        matches_by_id = matches.set_index("permit_row_id")
        for col in attom_value_cols:
            permits_by_id.loc[matches_by_id.index, col] = matches_by_id[col]
        permits_by_id.loc[matches_by_id.index, "attom_match_tier"] = tier_name
        return permits_by_id.reset_index(drop=True)

    permits = apply_unique_fallback(
        permits_df=permits,
        attom_df=attom,
        permit_keys=["addr_clean", "county_fips"],
        attom_keys=["addr_clean", "attom_county_fips"],
        tier_name="unique_addr_county",
        drop_cols=["addr_nosuffix", "zip_clean"],
    )
    county_exact_n = int((permits["attom_match_tier"] == "unique_addr_county").sum())

    permits = apply_unique_fallback(
        permits_df=permits,
        attom_df=attom,
        permit_keys=["addr_nosuffix", "county_fips"],
        attom_keys=["addr_nosuffix", "attom_county_fips"],
        tier_name="unique_nosuffix_county",
        drop_cols=["addr_clean", "zip_clean"],
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
for c in ["TAXMARKETVALUETOTAL", "TAXASSESSEDVALUETOTAL", "TAXASSESSEDVALUEIMPROVEMENTS",
          "ASSESSORLASTSALEAMOUNT", "YEARBUILT", "YEARBUILTEFFECTIVE", "TAXYEARASSESSED"]:
    if c in permits.columns:
        permits[c] = pd.to_numeric(permits[c], errors="coerce")

permits["prop_value"] = permits["TAXMARKETVALUETOTAL"]
if "TAXASSESSEDVALUETOTAL" in permits.columns:
    permits["prop_value"] = permits["prop_value"].fillna(permits["TAXASSESSEDVALUETOTAL"])
permits["prop_value"] = pd.to_numeric(permits["prop_value"], errors="coerce")
permits["JOB_VALUE"] = pd.to_numeric(permits["JOB_VALUE"], errors="coerce")

permits["log_prop_value"] = np.log(permits["prop_value"].where(permits["prop_value"] > 0))
permits["log_job_value"]  = np.log(permits["JOB_VALUE"].where(permits["JOB_VALUE"] > 0))
valid_value_cost = ((permits["prop_value"] > 0) & (permits["JOB_VALUE"] > 0)).fillna(False)
permits["val_cost_ratio"] = np.where(
    valid_value_cost,
    permits["prop_value"] / permits["JOB_VALUE"], np.nan,
)

# ---------------------------------------------------------------------------
# 6. County benchmark: prop value across all elevated properties in county
# ---------------------------------------------------------------------------
print("\nComputing county × year property value benchmarks...")
county_year_bench = (
    permits[permits["prop_value"].notna()]
    .groupby(["county_fips", "permit_year"])["prop_value"]
    .agg(
        cnty_yr_propval_mean   ="mean",
        cnty_yr_propval_median ="median",
        cnty_yr_propval_p25    =lambda x: x.quantile(0.25),
        cnty_yr_propval_p75    =lambda x: x.quantile(0.75),
        cnty_yr_n              ="count",
    )
    .reset_index()
)
county_year_bench["cnty_yr_log_propval_mean"]   = np.log(county_year_bench["cnty_yr_propval_mean"].where(county_year_bench["cnty_yr_propval_mean"] > 0))
county_year_bench["cnty_yr_log_propval_median"] = np.log(county_year_bench["cnty_yr_propval_median"].where(county_year_bench["cnty_yr_propval_median"] > 0))
print(f"  County × year benchmarks: {len(county_year_bench):,} cells")

# ---------------------------------------------------------------------------
# 7. Merge benchmarks and compute relative measure
# ---------------------------------------------------------------------------
permits = permits.merge(county_year_bench, on=["county_fips", "permit_year"], how="left")

valid_rel = ((permits["prop_value"] > 0) & (permits["cnty_yr_propval_median"] > 0)).fillna(False)
permits["propval_rel_cnty_yr"] = np.where(
    valid_rel,
    permits["prop_value"] / permits["cnty_yr_propval_median"], np.nan,
)

# ---------------------------------------------------------------------------
# 8. Diagnostics, variable order, and save
# ---------------------------------------------------------------------------
diagnostics_path = Path(args.diagnostics) if args.diagnostics else Path(args.out).with_name(
    f"{Path(args.out).stem}_attom_diagnostics.csv"
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

id_cols    = ["county_fips", "zip_clean", "permit_year", "PERMIT_NUMBER", "PERMIT_DATE", "addr_clean"]
val_cols   = ["prop_value", "log_prop_value", "TAXMARKETVALUETOTAL", "TAXASSESSEDVALUETOTAL",
              "TAXASSESSEDVALUEIMPROVEMENTS", "TAXYEARASSESSED",
              "YEARBUILT", "YEARBUILTEFFECTIVE", "ASSESSORLASTSALEAMOUNT",
              "STATUSOWNEROCCUPIEDFLAG", "attom_match_tier"]
bench_cols = [c for c in permits.columns if c.startswith("cnty_yr_")]
rel_cols   = ["propval_rel_cnty_yr"]
job_cols   = ["JOB_VALUE", "log_job_value", "val_cost_ratio"]
hma_cols   = [c for c in permits.columns if c.startswith("hma_")]

ordered = [c for c in id_cols + val_cols + bench_cols + rel_cols + job_cols + hma_cols
           if c in permits.columns]
rest = [c for c in permits.columns if c not in ordered]
permits = permits[ordered + rest].sort_values(["county_fips", "permit_year"]).reset_index(drop=True)

out = Path(args.out)
out.parent.mkdir(parents=True, exist_ok=True)
permits.to_parquet(out, index=False)

print(f"\nSaved: {out}")
print(f"Shape: {permits.shape[0]:,} rows × {permits.shape[1]} columns")
print(f"  With ATTOM prop_value:          {permits['prop_value'].notna().sum():,}")
print(f"  With county×year benchmark:     {permits['cnty_yr_propval_median'].notna().sum():,}")
print(f"  With propval_rel_cnty_yr:       {permits['propval_rel_cnty_yr'].notna().sum():,}")

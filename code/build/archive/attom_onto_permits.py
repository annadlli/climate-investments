"""
merge_attom_onto_permits.py
Merges ATTOM property data onto Builty flood elevation permits via
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
    --permits   flood_elev_hma.parquet  (Builty permits with HMA merged)
    --attom     attom.parquet or glob pattern of ATTOM parquet files
    --out       output path for flood_elev_attom.parquet

Optional:
    --tmp       temp directory for DuckDB spill (default: /tmp)
    --threads   DuckDB thread count (default: 4)
    --memory    DuckDB memory limit (default: 32GB)
"""

import argparse
import re
import duckdb
import pandas as pd
import numpy as np
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
# 2. Flood elevation text filter — flood_elev_strict & ~flood_elev_falsepos
#    Mirrors texas.do exactly.
# ---------------------------------------------------------------------------
print("Applying flood elevation filter...")

desc = (permits["DESCRIPTION"]
        .fillna("")
        .str.lower()
        .str.replace(r"[/\-()\.,]", " ", regex=True))

strict = (
    desc.str.contains(r"house elevation") |
    desc.str.contains(r"home elevation") |
    desc.str.contains(r"residential house elevation") |
    desc.str.contains(r"flood damage house elevation") |
    desc.str.contains(r"flood damaged house elevation") |
    desc.str.contains(r"elevat(?:e|ing) (?:existing )?(?:house|home)") |
    desc.str.contains(r"rais(?:e|ed|ing) (?:the )?(?:house|home)") |
    desc.str.contains(r"(?:house|home).{0,40}rais(?:e|ed|ing)") |
    desc.str.contains(r"(?:house|home).{0,40}elevat") |
    desc.str.contains(r"lowest floor of the house.{0,50}elevat") |
    desc.str.contains(r"raise house to fema") |
    desc.str.contains(r"raised to meet elevation requirements") |
    desc.str.contains(r"elevation of existing home") |
    desc.str.contains(r"floodplain.{0,40}elevat") |
    desc.str.contains(r"elevat.{0,40}floodplain") |
    desc.str.contains(r"flood zone.{0,40}(?:elevat|rais)") |
    desc.str.contains(r"(?:elevat|rais).{0,40}flood zone") |
    desc.str.contains(r"(?:rais|elevat).{0,40}base flood elevation") |
    desc.str.contains(r"base flood elevation.{0,40}(?:rais|increas|meet|comply|above|compli)") |
    desc.str.contains(r"freeboard") |
    desc.str.contains(r"nfip.{0,40}(?:rais|elevat)") |
    desc.str.contains(r"(?:rais|elevat).{0,40}nfip") |
    desc.str.contains(r"hazard mitigation.{0,40}(?:rais|elevat|home|house|structur|residen)") |
    desc.str.contains(r"(?:rais|elevat).{0,40}hazard mitigation") |
    desc.str.contains(r"substantially (?:damaged|improved).{0,40}(?:rais|elevat)") |
    desc.str.contains(r"(?:rais|elevat).{0,40}substantially (?:damaged|improved)") |
    desc.str.contains(r"sfr.{0,30}(?:elevat|rais)") |
    desc.str.contains(r"(?:elevat|rais).{0,30}sfr") |
    desc.str.contains(r"residential.{0,30}(?:elevat|rais).{0,30}(?:flood|fema|bfe|mitigation)") |
    desc.str.contains(r"lift(?:ed|ing).{0,40}(?:house|home|structur|residen|dwelling|out of)") |
    desc.str.contains(r"(?:house|home|structur|residen|dwelling).{0,40}lift(?:ed|ing)") |
    desc.str.contains(r"lift(?:ed|ing).{0,40}(?:floodplain|flood|fema|icc)") |
    desc.str.contains(r"out of (?:the )?floodplain") |
    desc.str.contains(r"jack(?:ed|ing) up.{0,40}(?:house|home|structur|residen)")
)

falsepos = (
    desc.str.contains(r"(?:front|rear|side|north|south|east|west) elevation") |
    desc.str.contains(r"(?:left|right) swing elevation") |
    desc.str.contains(r"plan[: ]+[0-9a-z ]+.*elevation[: ]+[a-z]") |
    desc.str.contains(r"elevation (?:drawing|plan|view|sheet|detail)") |
    desc.str.contains(r"elevation [a-z](?:\s|$)") |
    desc.str.contains(r"(?:grade|grading|pad|site|curb|street|road|drain) elevation") |
    desc.str.contains(r"generator") |
    desc.str.contains(r"water heater") |
    desc.str.contains(r"signage|wall sign|channel letters") |
    desc.str.contains(r"illuminated.{0,30}(?:sign|letter|cabinet)") |
    desc.str.contains(r"finished floor") |
    desc.str.contains(r"minimum ffe|minimun ffe|min ffe") |
    desc.str.contains(r"elevation certificate") |
    desc.str.contains(r"flood plain determination") |
    desc.str.contains(r"raise (?:the )?roof") |
    desc.str.contains(r"raise (?:the )?bar") |
    desc.str.contains(r"patio addition rear elevation") |
    desc.str.contains(r"front elevation refacing") |
    desc.str.contains(r"new (?:home|sfr|single family|residence).{0,80}(?:plan |elevation [a-z](?:\s|$))") |
    desc.str.contains(r"(?:plan |master plan ).{0,30}elevation [a-z](?:\s|$)") |
    desc.str.contains(r"new (?:sfr|single family).{0,30}existing elevation")
)

permits = permits[strict & ~falsepos].copy()
print(f"  After flood_elev_final: {len(permits):,}")

permits = permits[(permits["permit_year"] >= 2000) & permits["permit_year"].notna()]
print(f"  After year >= 2000:     {len(permits):,}")

if "RESIDENTIAL" in permits.columns:
    permits = permits[permits["RESIDENTIAL"] == 1]
    print(f"  After residential:      {len(permits):,}")

# ---------------------------------------------------------------------------
# 3. Detect ATTOM address columns and build address-clean SQL
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

# Keep only the ATTOM columns we need
ATTOM_KEEP = [
    addr_col, zip_col,
    "TAXMARKETVALUETOTAL", "TAXASSESSEDVALUETOTAL",
    "TAXASSESSEDVALUEIMPROVEMENTS", "TAXYEARASSESSED",
    "YEARBUILT", "YEARBUILTEFFECTIVE", "ASSESSORLASTSALEAMOUNT",
]
ATTOM_KEEP = [c for c in ATTOM_KEEP if c in attom_cols]
keep_sql = ", ".join(f'"{c}"' for c in ATTOM_KEEP)

# DuckDB address cleaning SQL — same abbreviation table as merge_loop.py
def addr_clean_sql(col: str) -> str:
    return f"""
        regexp_replace(
        replace(replace(replace(replace(replace(replace(replace(replace(
        replace(replace(replace(replace(replace(replace(replace(replace(
        replace(replace(replace(replace(replace(
        regexp_replace(
        regexp_replace(
            replace(replace(lower(trim("{col}")), ',', ''), '.', ''),
        '\\s+', ' ', 'g'),
        '\\s+city of.*$', '', 'g'),
        ' street', ' st'),
        ' avenue', ' ave'),
        ' boulevard', ' blvd'),
        ' drive', ' dr'),
        ' court', ' ct'),
        ' place', ' pl'),
        ' lane', ' ln'),
        ' road', ' rd'),
        ' circle', ' cir'),
        ' highway', ' hwy'),
        ' parkway', ' pkwy'),
        ' terrace', ' ter'),
        ' trail', ' trl'),
        ' north', ' n'),
        ' south', ' s'),
        ' east', ' e'),
        ' west', ' w'),
        ' apartment', ' apt'),
        ' suite', ' ste'),
        ' unit', ' apt'),
        '#', 'apt '),
        '\\s+', ' ', 'g')
    """

def zip_clean_sql(col: str) -> str:
    return f"regexp_extract(replace(upper(trim(\"{col}\")), ' ', ''), '^(\\d{{5}})', 1)"

# ---------------------------------------------------------------------------
# 4. Build Builty merge keys in pandas (same logic as SQL above)
# ---------------------------------------------------------------------------
ABBREVS = [
    (r'\bstreet\b', 'st'),   (r'\bavenue\b', 'ave'),   (r'\bboulevard\b', 'blvd'),
    (r'\bdrive\b',  'dr'),   (r'\bcourt\b',  'ct'),    (r'\bplace\b',  'pl'),
    (r'\blane\b',   'ln'),   (r'\broad\b',   'rd'),    (r'\bcircle\b', 'cir'),
    (r'\bhighway\b','hwy'),  (r'\bparkway\b','pkwy'),  (r'\bterrace\b','ter'),
    (r'\btrail\b',  'trl'),  (r'\bnorth\b',  'n'),     (r'\bsouth\b',  's'),
    (r'\beast\b',   'e'),    (r'\bwest\b',   'w'),     (r'\bapartment\b','apt'),
    (r'\bsuite\b',  'ste'),  (r'\bunit\b',   'apt'),   (r'#',          'apt '),
]

def clean_address(s: pd.Series) -> pd.Series:
    s = s.fillna("").str.lower().str.strip()
    s = s.str.replace(",", "", regex=False).str.replace(".", "", regex=False)
    s = s.str.replace(r'\s+city of.*$', '', regex=True)
    for pat, repl in ABBREVS:
        s = s.str.replace(pat, repl, regex=True)
    return s.str.replace(r'\s+', ' ', regex=True).str.strip()

def clean_zip(s: pd.Series) -> pd.Series:
    return s.fillna("").str.strip().str.extract(r'^(\d{5})')[0].fillna("")

permits["addr_clean"] = clean_address(permits["STREET"])
permits["zip_clean"]  = clean_zip(permits["ZIPCODE"])

# ---------------------------------------------------------------------------
# 5. Load and deduplicate ATTOM via DuckDB, then merge
# ---------------------------------------------------------------------------
print("\nLoading ATTOM and deduplicating on address+ZIP...")
attom = con.execute(f"""
    SELECT DISTINCT ON (addr_clean, zip_clean)
        {keep_sql},
        {addr_clean_sql(addr_col)} AS addr_clean,
        {zip_clean_sql(zip_col)}   AS zip_clean
    FROM read_parquet({attom_path})
    WHERE "{addr_col}" IS NOT NULL AND trim("{addr_col}") != ''
      AND "{zip_col}"  IS NOT NULL AND trim("{zip_col}")  != ''
""").df()
print(f"  ATTOM after dedup: {len(attom):,}")

attom = attom.drop(columns=[addr_col, zip_col], errors="ignore")

permits = permits.merge(attom, on=["addr_clean", "zip_clean"], how="left")
n_matched = permits["TAXMARKETVALUETOTAL"].notna().sum()
print(f"  Matched to ATTOM: {n_matched:,} / {len(permits):,} ({n_matched/len(permits)*100:.1f}%)")

# ---------------------------------------------------------------------------
# 6. Derive property value variables
# ---------------------------------------------------------------------------
for c in ["TAXMARKETVALUETOTAL", "TAXASSESSEDVALUETOTAL", "TAXASSESSEDVALUEIMPROVEMENTS",
          "ASSESSORLASTSALEAMOUNT", "YEARBUILT", "YEARBUILTEFFECTIVE", "TAXYEARASSESSED"]:
    if c in permits.columns:
        permits[c] = pd.to_numeric(permits[c], errors="coerce")

permits["prop_value"] = permits["TAXMARKETVALUETOTAL"]
if "TAXASSESSEDVALUETOTAL" in permits.columns:
    permits["prop_value"] = permits["prop_value"].fillna(permits["TAXASSESSEDVALUETOTAL"])

permits["log_prop_value"] = np.log(permits["prop_value"].where(permits["prop_value"] > 0))
permits["log_job_value"]  = np.log(permits["JOB_VALUE"].where(permits["JOB_VALUE"] > 0))
permits["val_cost_ratio"] = np.where(
    (permits["prop_value"] > 0) & (permits["JOB_VALUE"] > 0),
    permits["prop_value"] / permits["JOB_VALUE"], np.nan,
)

# ---------------------------------------------------------------------------
# 7. County benchmark: prop value across all elevated properties in county
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
# 8. Merge benchmarks and compute relative measure
# ---------------------------------------------------------------------------
permits = permits.merge(county_year_bench, on=["county_fips", "permit_year"], how="left")

permits["propval_rel_cnty_yr"] = np.where(
    (permits["prop_value"] > 0) & (permits["cnty_yr_propval_median"] > 0),
    permits["prop_value"] / permits["cnty_yr_propval_median"], np.nan,
)

# ---------------------------------------------------------------------------
# 9. Variable order and save
# ---------------------------------------------------------------------------
id_cols    = ["county_fips", "zip_clean", "permit_year", "PERMIT_NUMBER", "PERMIT_DATE", "addr_clean"]
val_cols   = ["prop_value", "log_prop_value", "TAXMARKETVALUETOTAL", "TAXASSESSEDVALUETOTAL",
              "TAXASSESSEDVALUEIMPROVEMENTS", "TAXYEARASSESSED",
              "YEARBUILT", "YEARBUILTEFFECTIVE", "ASSESSORLASTSALEAMOUNT"]
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

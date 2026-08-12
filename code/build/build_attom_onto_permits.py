"""
Authors: Anna Li
Date: 2026-08-12

Merging Builty permits with Attom values.
For each state, read the Builty, clean the addresses, match to ATTOM.
For each permit, keep the closest prior ATTOM value for that specific address.
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


# args and paths
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--permits", default=None)
    parser.add_argument("--attom", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--tmp", default="/tmp")
    parser.add_argument("--threads", default=4, type=int)
    parser.add_argument("--memory", default="32GB")
    parser.add_argument("--max-temp", default="200GB")
    parser.add_argument("--diagnostics", default=None)
    return parser.parse_args()


# Builty input is the statewide file of elevations
args = parse_args()
STATE = args.state.upper()
state_lower = STATE.lower()
data = Path(args.data)
permits_path = Path(args.permits) if args.permits else data / "builty_elevations.dta"
attom_input = args.attom if args.attom else str(data / "raw" / "attom" / f"attom_{state_lower}.parquet")
out_path = Path(args.out) if args.out else data / "build" / f"{state_lower}_attom_permits.parquet"

# This is the geocoded attom file -w ith censusblockgroupfips in it.
ADDR_COL, ZIP_COL, COUNTY_COL, BG_COL = (
    "property_address_full", "zip", "countycode", "censusblockgroupfips"
)
if Path(attom_input).suffix.lower() == ".dta":
    attom_frame = pd.read_stata(attom_input, convert_categoricals=False)
    attom_frame.columns = [column.lower() for column in attom_frame.columns]
    ZIP_COL = "zip_key" if "zip_key" in attom_frame.columns else ZIP_COL
    COUNTY_COL = "county_key" if "county_key" in attom_frame.columns else COUNTY_COL
    BG_COL = "bg_key" if "bg_key" in attom_frame.columns else BG_COL
else:
    attom_frame = None

# duckdb setup
con = duckdb.connect()
con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
con.execute(f"SET memory_limit={quote_sql(args.memory)}")
con.execute(f"SET max_temp_directory_size={quote_sql(args.max_temp)}")
con.execute(f"SET threads={args.threads}")
con.execute("SET preserve_insertion_order=false")

# load builty elevation permits and make merge keys
# read the permit file
if permits_path.suffix.lower() == ".dta":
    permits = pd.read_stata(permits_path, convert_categoricals=False)
elif permits_path.suffix.lower() in {".parquet", ".pq"}:
    permits = con.execute(
        f"SELECT * FROM read_parquet({quote_sql(str(permits_path))})"
    ).df()
else:
    raise ValueError(
        f"Unsupported Builty input {permits_path}; expected .dta or .parquet"
    )
permits.columns = [c.upper() for c in permits.columns]
if "STATE" not in permits.columns:
    raise KeyError(f"Builty input {permits_path} has no STATE column")
permits = permits[permits["STATE"].fillna("").str.upper().eq(STATE)].copy()

# If a state happens to have no permits, write an empty file and exit cleanly.
# ex: MD and MS
if len(permits) == 0:
    permits.to_parquet(out_path, index=False)
    print(f"{STATE}: 0 permits, wrote empty output")
    raise SystemExit(0)

# Use the canonical property's earliest permit year. Older permit-level inputs
# instead carry three raw date fields, which remain supported for reproducibility.
if "YEAR" in permits.columns:
    permits["permit_year"] = pd.to_numeric(permits["YEAR"], errors="coerce")
    permits["permit_date"] = pd.to_datetime(
        permits["permit_year"].astype("Int64").astype(str) + "-01-01", errors="coerce"
    )
else:
    permits["permit_date"] = pd.to_datetime(permits["DATE_ISSUED"], errors="coerce")
    for fallback in ["DATE_SUBMITTED", "DATE_FINALED"]:
        permits["permit_date"] = permits["permit_date"].fillna(
            pd.to_datetime(permits[fallback], errors="coerce")
        )
    permits["permit_year"] = permits["permit_date"].dt.year

# county FIPS (5 digits)
state_fips = permits["FIPS_STATE"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(2)
county_fips = permits["FIPS_COUNTY"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(3)
permits["county_fips"] = state_fips + county_fips
state_fips_prefix = state_fips.iloc[0]

# address cleaning for ATTOM side
# ATTOM columns used in the match
raw_attom_schema = False
if attom_frame is None:
    parquet_columns = {
        row[0] for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet({quote_sql(attom_input)})"
        ).fetchall()
    }
    if "PROPERTYADDRESSFULL" in parquet_columns:
        raw_attom_schema = True
        ADDR_COL = "PROPERTYADDRESSFULL"
        ZIP_COL = "PROPERTYADDRESSZIP"
        COUNTY_COL = "SITUSSTATECOUNTYFIPS"
        BG_COL = None

ATTOM_KEEP = [
    (ADDR_COL, ADDR_COL),
    (ZIP_COL, "attom_zipcode"),
    (COUNTY_COL, COUNTY_COL),
    ("ATTOMID" if raw_attom_schema else "attomid", "ATTOMID"),
    ("TAXASSESSEDVALUETOTAL" if raw_attom_schema else "assessed_value_total", "TAXASSESSEDVALUETOTAL"),
    ("TAXASSESSEDVALUEIMPROVEMENTS" if raw_attom_schema else "assessed_value_improvements", "TAXASSESSEDVALUEIMPROVEMENTS"),
    ("PREVIOUSASSESSEDVALUE" if raw_attom_schema else "previous_assessed_value", "PREVIOUSASSESSEDVALUE"),
    ("TAXYEARASSESSED" if raw_attom_schema else "year", "TAXYEARASSESSED"),
    ("YEARBUILT" if raw_attom_schema else "construction_year", "YEARBUILT"),
    ("YEARBUILTEFFECTIVE" if raw_attom_schema else "construction_year", "YEARBUILTEFFECTIVE"),
    (BG_COL, "censusblockgroupfips"),
    (None if raw_attom_schema else "geocode_match", "geocode_match"),
    ("LONGITUDE" if raw_attom_schema else "longitude", "longitude"),
    ("LATITUDE" if raw_attom_schema else "latitude", "latitude"),
]
keep_sql = ", ".join(
    f'"{source}" AS "{target}"' if source is not None else f"'' AS \"{target}\""
    for source, target in ATTOM_KEEP
)

STREET_SUFFIX_RE = (r"\s+(st|ave|blvd|dr|ct|pl|ln|rd|cir|hwy|pkwy|ter|trl|way|"
                    r"sq|loop|cv|xing|xrd|aly|walk|path|pike|rte|route)$")


# lowercase, strip punctuation, and normalize USPS abbreviations
def addr_clean_sql(col: str) -> str:
    expr = f'replace(replace(lower(trim("{col}")), \',\', \'\'), \'.\', \'\')'
    expr = f"regexp_replace({expr}, '\\s+', ' ', 'g')"
    expr = f"regexp_replace({expr}, '\\s+city of.*$', '', 'g')"
    for old, new in [("street", "st"), ("avenue", "ave"), ("boulevard", "blvd"), ("drive", "dr"),
                     ("court", "ct"), ("place", "pl"), ("lane", "ln"), ("road", "rd"), ("circle", "cir"),
                     ("highway", "hwy"), ("parkway", "pkwy"), ("terrace", "ter"), ("trail", "trl"),
                     ("square", "sq"), ("cove", "cv"), ("crossing", "xing"), ("alley", "aly"),
                     ("point", "pt"),
                     ("north", "n"), ("south", "s"), ("east", "e"), ("west", "w"),
                     ("apartment", "apt"), ("suite", "ste"), ("unit", "apt")]:
        expr = f"regexp_replace({expr}, '\\b{old}\\b', '{new}', 'g')"
    expr = f"replace({expr}, '#', 'apt ')"
    for joined, spaced in [("groveway", "grove way"), ("soundview", "sound view"),
                           ("harborview", "harbor view"), ("farmcreek", "farm creek"),
                           ("newmarker", "new marker"), ("highridge", "high ridge")]:
        expr = f"regexp_replace({expr}, '\\b{joined}\\b', '{spaced}', 'g')"
    return f"trim(regexp_replace({expr}, '\\s+', ' ', 'g'))"


# drop the street-type suffix for the no-suffix fallback tier
def addr_nosuffix_sql(clean_expr: str) -> str:
    no_unit = f"regexp_replace({clean_expr}, '\\s+(apt|unit|ste)\\s+.*$', '')"
    return f"regexp_replace({no_unit}, '{STREET_SUFFIX_RE}', '')"


def addr_compact_sql(clean_expr: str) -> str:
    return f"replace({addr_nosuffix_sql(clean_expr)}, ' ', '')"


# keep just the leading 5-digit ZIP
def zip_clean_sql(col: str) -> str:
    return f"regexp_extract(replace(upper(trim(\"{col}\")), ' ', ''), '^(\\d{{5}})', 1)"


# normalize ATTOM county code to 5-digit state + county FIPS
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


# address cleaning for builty permit side
ABBREVS = [(r'\bstreet\b', 'st'), (r'\bavenue\b', 'ave'), (r'\bboulevard\b', 'blvd'),
           (r'\bdrive\b', 'dr'), (r'\bcourt\b', 'ct'), (r'\bplace\b', 'pl'), (r'\blane\b', 'ln'),
           (r'\broad\b', 'rd'), (r'\bcircle\b', 'cir'), (r'\bhighway\b', 'hwy'), (r'\bparkway\b', 'pkwy'),
           (r'\bterrace\b', 'ter'), (r'\btrail\b', 'trl'), (r'\bnorth\b', 'n'), (r'\bsouth\b', 's'),
           (r'\beast\b', 'e'), (r'\bwest\b', 'w'), (r'\bapartment\b', 'apt'), (r'\bsuite\b', 'ste'),
           (r'\bunit\b', 'apt'), (r'\bsquare\b', 'sq'), (r'\bcove\b', 'cv'), (r'\bcrossing\b', 'xing'),
           (r'\balley\b', 'aly'), (r'#', 'apt ')]
ABBREVS.append((r'\bpoint\b', 'pt'))
STREET_SUFFIX_PATTERN = r"\s+(" + "|".join((
    "st", "ave", "blvd", "dr", "ct", "pl", "ln", "rd", "cir", "hwy", "pkwy", "ter", "trl", "way",
    "sq", "loop", "cv", "xing", "xrd", "aly", "walk", "path", "pike", "rte", "route")) + r")$"


# clean permit address to match ATTOM format
def clean_address(s: pd.Series, locality: pd.Series, state: pd.Series) -> pd.Series:
    s = s.fillna("").str.lower().str.strip()
    # true address is before comma (after comma is city, state, etc)
    s = s.str.split(",").str[0]
    s = s.str.replace(",", "", regex=False).str.replace(".", "", regex=False)
    s = s.str.replace(r'\b(\d{5})-\d{4}\b', r'\1', regex=True)
    s = s.str.replace(r'\b(\d{5})\d{4}\b', r'\1', regex=True)
    s = s.str.replace(r'\b\d{4,5}\b\s*$', '', regex=True)
    #remove city names and state names from address
    #don't remove 2-letter state abbreviation
    state_names = {"ct": "connecticut", "de": "delaware", "ri": "rhode island"}
    states_short = set(state.fillna("").str.lower().str.strip()) - {""}
    states_full = {state_names.get(st, st) for st in states_short}
    for st in sorted(states_full, key=len, reverse=True):
        s = s.str.replace(rf'\b{re.escape(st)}\b\s*$', '', regex=True)
    for st in states_short - {"ct"}:
        s = s.str.replace(rf'\b{re.escape(st)}\b\s*$', '', regex=True)
    loc_clean = locality.fillna("").str.lower().str.strip().str.replace(r'[/\-()\.,]', ' ', regex=True)
    loc_clean = loc_clean.str.replace(r'\s+', ' ', regex=True).str.strip()
    for loc in sorted(set(loc_clean) - {""}, key=len, reverse=True):
        state_tail = "|".join(re.escape(x) for x in states_short | states_full)
        s = s.str.replace(
            rf'\b{re.escape(loc)}(?:\s+beach)?(?:\s+(?:{state_tail}))?\b\s*$',
            '', regex=True,
        )
    # weird exceptions: Norwalk exports many addresses as "STREET (house number)"
    s = s.str.replace(r'^(.+?)\s*\((\d{1,5})\)(?:\s+.*)?$', r'\2 \1', regex=True)
    s = s.str.replace(r'^0+(\d+)\s+', r'\1 ', regex=True)
    #weird exceptions: Old Saybrook appends a parcel/unit marker to the street address.
    s = s.str.replace(r'-\d+\s*$', '', regex=True)
    s = s.str.replace(r'\s+city of.*$', '', regex=True)
    #fix format and abbreviations
    for pat, repl in ABBREVS:
        s = s.str.replace(pat, repl, regex=True)
    s = s.str.replace(r'\bcr\b', 'cir', regex=True)
    s = s.str.replace(r'\bpk\b', 'park', regex=True)
    s = s.str.replace(r'\bla\b$', 'ln', regex=True)
    s = s.str.replace(r'\b(trl|rd|st|ave|dr|ln|ct)\s+\1\b$', r'\1', regex=True)
    s = s.str.replace(r'\btrl\s+trl\b', 'trl', regex=True)
    s = s.str.replace(r'^152 overshores dr e$', '152 overshores e', regex=True)
    s = s.str.replace(r'\s+milton$', '', regex=True)
    return s.str.replace(r'\s+', ' ', regex=True).str.strip()


# ZIP field with fallback to ZIP embedded in the address text
def recover_zip(zip_s: pd.Series, addr_s: pd.Series) -> pd.Series:
    z = zip_s.fillna("").str.strip().str.extract(r'^(\d{5})')[0].fillna("")
    addr = addr_s.fillna("").astype(str)
    recovered = (addr.str.extract(r'\b(\d{5})-\d{4}\b')[0]
                 .fillna(addr.str.extract(r'\b(\d{5})\d{4}\b')[0])
                 .fillna(addr.str.extract(r'\b(\d{5})\b')[0]).fillna(""))
    return z.mask(z == "", recovered)


# permit-side key fields
permits["permit_row_id"] = np.arange(len(permits))
permits["addr_clean"] = clean_address(permits["STREET_ADDRESS"], permits["LOCALITY"], permits["STATE"])
permits["zip_clean"] = recover_zip(permits["ZIPCODE"], permits["STREET_ADDRESS"])
permits["addr_nosuffix"] = permits["addr_clean"].str.replace(STREET_SUFFIX_PATTERN, "", regex=True).str.strip()
permits["addr_nosuffix"] = permits["addr_nosuffix"].str.replace(
    r'\s+(apt|unit|ste)\s+.*$', '', regex=True
).str.strip()
permits["addr_compact"] = permits["addr_nosuffix"].str.replace(" ", "", regex=False)

# load ATTOM and clean
if attom_frame is not None: #already loaded as df
    con.register("attom_input", attom_frame)
    attom_source = "attom_input"
    attom_candidate_filter = ""
else: #parquet
    attom_source = f"read_parquet({quote_sql(attom_input)})"
    con.register("permit_keys", permits[["addr_clean", "addr_nosuffix", "addr_compact"]].drop_duplicates())
    attom_candidate_filter = f"""
      AND (
          {addr_clean_sql(ADDR_COL)} IN (SELECT addr_clean FROM permit_keys)
          OR {addr_nosuffix_sql(addr_clean_sql(ADDR_COL))}
             IN (SELECT addr_nosuffix FROM permit_keys)
          OR {addr_compact_sql(addr_clean_sql(ADDR_COL))}
             IN (SELECT addr_compact FROM permit_keys)
      )
    """
    #build df
attom = con.execute(f"""
    SELECT {keep_sql},
        {addr_clean_sql(ADDR_COL)} AS addr_clean,
        {addr_nosuffix_sql(addr_clean_sql(ADDR_COL))} AS addr_nosuffix,
        {addr_compact_sql(addr_clean_sql(ADDR_COL))} AS addr_compact,
        {zip_clean_sql("attom_zipcode")} AS zip_clean,
        {county_clean_sql(COUNTY_COL)} AS attom_county_fips,
        1 AS attom_record_present
    FROM {attom_source}
    WHERE "{ADDR_COL}" IS NOT NULL AND trim("{ADDR_COL}") != ''
      AND "{ZIP_COL}"  IS NOT NULL AND trim("{ZIP_COL}")  != ''
      {attom_candidate_filter}
""").df()
attom = attom.drop(columns=[ADDR_COL, COUNTY_COL])
attom["attom_assessment_year"] = pd.to_numeric(attom.pop("TAXYEARASSESSED"), errors="coerce")

# match start empty and fill in
attom_value_cols = [c for c in attom.columns if c not in ("addr_clean", "addr_nosuffix", "addr_compact", "zip_clean", "attom_county_fips")]
for c in attom_value_cols:
    permits[c] = pd.Series(pd.NA, index=permits.index, dtype="object")
permits["attom_match_tier"] = "unmatched"
# prior = at or before permit; post = nearest later year if needed
permits["attom_value_asof"] = pd.Series(pd.NA, index=permits.index, dtype="object")


# match permits on a given address key and pick the closest prior year, requiring attom is nonmissing
def apply_temporal_match(permits_df, attom_df, permit_keys, attom_keys, tier_name, require_unique_property):
    unmatched_mask = permits_df["attom_match_tier"] == "unmatched"
    candidate = attom_df[attom_df[attom_keys[0]].astype(str).str.len().gt(0)
                         & attom_df[attom_keys[1]].astype(str).str.len().gt(0)
                         & attom_df["attom_assessment_year"].notna()].copy()
    if require_unique_property:
        counts = candidate.groupby(attom_keys)["ATTOMID"].nunique().rename("n").reset_index()
        candidate = candidate.merge(counts, on=attom_keys, how="left")
        candidate = candidate[candidate["n"] <= 1].drop(columns="n")

    candidate = candidate[attom_keys + attom_value_cols].rename(columns=dict(zip(attom_keys, permit_keys)))
    matches = permits_df.loc[unmatched_mask, ["permit_row_id", "permit_year"] + permit_keys].merge(candidate, on=permit_keys, how="left")
    matches = matches[matches["attom_record_present"].notna()]
    #pick best attom value, aka using closest assessment year
    missing_permit_year = matches["permit_year"].isna()
    is_prior = matches["attom_assessment_year"] <= matches["permit_year"]
    matches = matches.assign(
        _is_prior=is_prior,
        _rank=np.where(
            missing_permit_year,
            -matches["attom_assessment_year"],
            np.where(is_prior, -matches["attom_assessment_year"], matches["attom_assessment_year"]),
        ),
    )
    matches = matches.sort_values(["permit_row_id", "_is_prior", "_rank"], ascending=[True, False, True]).drop_duplicates("permit_row_id")
    matches["attom_value_asof"] = np.where(
        missing_permit_year.loc[matches.index], "undated",
        np.where(matches["_is_prior"], "prior", "post"),
    )

    permits_by_id = permits_df.set_index("permit_row_id", drop=False)
    matches_by_id = matches.set_index("permit_row_id")
    for col in attom_value_cols + ["attom_value_asof"]:
        permits_by_id.loc[matches_by_id.index, col] = matches_by_id[col]
    permits_by_id.loc[matches_by_id.index, "attom_match_tier"] = tier_name
    return permits_by_id.reset_index(drop=True)


# run the match tiers from precise to loose. tiers as before.
permits = apply_temporal_match(permits, attom, ["addr_clean", "zip_clean"], ["addr_clean", "zip_clean"], "exact", False)
permits = apply_temporal_match(permits, attom, ["addr_nosuffix", "zip_clean"], ["addr_nosuffix", "zip_clean"], "unique_no_suffix", True)
permits = apply_temporal_match(permits, attom, ["addr_compact", "zip_clean"], ["addr_compact", "zip_clean"], "unique_compact", True)
permits = apply_temporal_match(permits, attom, ["addr_clean", "county_fips"], ["addr_clean", "attom_county_fips"], "unique_addr_county", True)
permits = apply_temporal_match(permits, attom, ["addr_nosuffix", "county_fips"], ["addr_nosuffix", "attom_county_fips"], "unique_nosuffix_county", True)
permits = apply_temporal_match(permits, attom, ["addr_compact", "county_fips"], ["addr_compact", "attom_county_fips"], "unique_compact_county", True)

# if the permit ZIP is missing, use the ATTOM ZIP when it exists.
attom_zip = permits["attom_zipcode"].fillna("").astype(str).str.extract(r"^(\d{5})", expand=False).fillna("")
fill_attom_zip = permits["zip_clean"].eq("") & attom_zip.ne("") & permits["attom_match_tier"].ne("unmatched")
permits.loc[fill_attom_zip, "zip_clean"] = attom_zip[fill_attom_zip]
permits.loc[fill_attom_zip, "ZIPCODE"] = attom_zip[fill_attom_zip]
if "ZIPCODE_SOURCE" not in permits.columns:
    permits["ZIPCODE_SOURCE"] = ""
permits.loc[fill_attom_zip, "ZIPCODE_SOURCE"] = "attom"

# final clean up: convert var to numeric.
for c in ["TAXASSESSEDVALUETOTAL", "TAXASSESSEDVALUEIMPROVEMENTS", "PREVIOUSASSESSEDVALUE",
          "YEARBUILT", "YEARBUILTEFFECTIVE", "attom_assessment_year", "PROJECT_VALUE"]:
    if c not in permits.columns:
        permits[c] = np.nan
    permits[c] = pd.to_numeric(permits[c], errors="coerce")
#cleaned property value measure: assessed value if possible, then fall back to prior
permits["pre_flood_assessed_value"] = permits["TAXASSESSEDVALUETOTAL"].fillna(permits["PREVIOUSASSESSEDVALUE"])
permits["prop_value"] = permits["pre_flood_assessed_value"]

#check if have censusblockgroupfips that is valid
valid = ((permits["prop_value"] > 0) & (permits["PROJECT_VALUE"] > 0)).fillna(False)
permits["val_cost_ratio"] = np.where(valid, permits["prop_value"] / permits["PROJECT_VALUE"], np.nan)
permits["attom_has_blockgroup"] = (
    permits["censusblockgroupfips"].fillna("").astype(str).str.len() == 12
).astype(int)

#  diagnostics and final save

#coverage table to check
diagnostics_path = Path(args.diagnostics) if args.diagnostics else out_path.with_name(f"{out_path.stem}_attom_diagnostics.csv")
diagnostics = (permits.assign(has_prop_value=permits["prop_value"].notna(),
                              has_blockgroup=permits["attom_has_blockgroup"].eq(1))
               .groupby(["county_fips", "permit_year", "attom_match_tier"], dropna=False)
               .agg(n=("permit_row_id", "size"),
                    n_prop_value=("has_prop_value", "sum"),
                    n_blockgroup=("has_blockgroup", "sum")).reset_index())
diagnostics["prop_value_rate"] = diagnostics["n_prop_value"] / diagnostics["n"]
diagnostics["blockgroup_rate"] = diagnostics["n_blockgroup"] / diagnostics["n"]
diagnostics.to_csv(diagnostics_path, index=False)

#reorder
id_cols = ["county_fips", "zip_clean", "permit_year", "permit_date", "BUILTY_ID", "addr_clean"]
val_cols = ["prop_value", "pre_flood_assessed_value", "log_prop_value", "attom_assessment_year",
            "attom_value_asof",
            "attom_zipcode",
            "TAXASSESSEDVALUETOTAL", "TAXASSESSEDVALUEIMPROVEMENTS", "PREVIOUSASSESSEDVALUE",
            "YEARBUILT", "YEARBUILTEFFECTIVE", "attom_match_tier",
            "censusblockgroupfips", "geocode_match",
            "longitude", "latitude", "attom_has_blockgroup"]
job_cols = ["PROJECT_VALUE", "log_project_value", "val_cost_ratio"]
ordered = [c for c in id_cols + val_cols + job_cols if c in permits.columns]
rest = [c for c in permits.columns if c not in ordered]
permits = permits[ordered + rest].sort_values(["county_fips", "permit_year"]).reset_index(drop=True)
permits.to_parquet(out_path, index=False)

# Final summary printout
n_matched = int((permits["attom_match_tier"] != "unmatched").sum())
print(f"{STATE}: {len(permits):,} permits, {n_matched:,} matched ({n_matched / len(permits):.0%}), saved {out_path.name}")

"""
Authors: Anna Li
Original Date: 2026-08-12
Revised Date: 2026-08-16

Puts an ATTOM property value onto every Builty elevation permit.

Builty is the only source with a real street address, so this is the one place
in the pipeline where we get a genuine 1:1 property match rather than a fuzzy
cell match. Addresses on both sides get scrubbed into the same shape, then we
try a ladder of match keys from tightest to loosest. For each permit we take
the ATTOM assessment closest to the permit year, preferring one from before
the elevation happened.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# the street-type endings removed  for the looser match tiers
STREET_SUFFIXES = ("st|ave|blvd|dr|ct|pl|ln|rd|cir|hwy|pkwy|ter|trl|way|"
                   "sq|loop|cv|xing|xrd|aly|walk|path|pike|rte|route")

# long forms and their USPS abbreviations, applied on both sides so the two match
ABBREVIATIONS = [
    ("street", "st"), ("avenue", "ave"), ("boulevard", "blvd"), ("drive", "dr"),
    ("court", "ct"), ("place", "pl"), ("lane", "ln"), ("road", "rd"), ("circle", "cir"),
    ("highway", "hwy"), ("parkway", "pkwy"), ("terrace", "ter"), ("trail", "trl"),
    ("square", "sq"), ("cove", "cv"), ("crossing", "xing"), ("alley", "aly"),
    ("point", "pt"), ("north", "n"), ("south", "s"), ("east", "e"), ("west", "w"),
    ("apartment", "apt"), ("suite", "ste"), ("unit", "apt"),
]

# ATTOM runs some street names together that Builty spells out (found with Claude)
RUN_TOGETHER = [("groveway", "grove way"), ("soundview", "sound view"),
                ("harborview", "harbor view"), ("farmcreek", "farm creek"),
                ("newmarker", "new marker"), ("highridge", "high ridge")]

# full state names to strip off the end of an address. Bare two-letter codes are
# deliberately left alone, because "la" is a real street suffix in some sources
STATE_NAMES = {
    "al": "alabama", "ct": "connecticut", "de": "delaware", "fl": "florida",
    "ga": "georgia", "la": "louisiana", "ma": "massachusetts", "md": "maryland",
    "me": "maine", "ms": "mississippi", "nc": "north carolina", "nh": "new hampshire",
    "nj": "new jersey", "ny": "new york", "pa": "pennsylvania", "ri": "rhode island",
    "sc": "south carolina", "tx": "texas", "va": "virginia", "vt": "vermont",
}

# the match ladder: (permit-side keys, ATTOM-side keys, name, with the address must be
# unique in ATTOM). The first tier allows duplicates because address plus zip is
# already tight; every looser tier insists the address points at one property only
MATCH_TIERS = [
    (["addr_clean", "zip_clean"],      ["addr_clean", "zip_clean"],      "exact",                  False),
    (["addr_nosuffix", "zip_clean"],   ["addr_nosuffix", "zip_clean"],   "unique_no_suffix",       True),
    (["addr_compact", "zip_clean"],    ["addr_compact", "zip_clean"],    "unique_compact",         True),
    (["addr_clean", "county_fips"],    ["addr_clean", "attom_county_fips"],    "unique_addr_county",     True),
    (["addr_nosuffix", "county_fips"], ["addr_nosuffix", "attom_county_fips"], "unique_nosuffix_county", True),
    (["addr_compact", "county_fips"],  ["addr_compact", "attom_county_fips"],  "unique_compact_county",  True),
]

# the key columns, as opposed to the ATTOM values we actually want to carry over
KEY_COLUMNS = ("addr_clean", "addr_nosuffix", "addr_compact", "zip_clean", "attom_county_fips")


def quote_sql(value: str) -> str:
    # escape single quotes so a value is safe to drop into a SQL string
    return "'" + value.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    # the state, the Builty permits, the ATTOM file, and where the output goes
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--permits", default=None)
    parser.add_argument("--attom", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--diagnostics", default=None)
    parser.add_argument("--tmp", default="/tmp")
    parser.add_argument("--threads", default=4, type=int)
    parser.add_argument("--memory", default="32GB")
    parser.add_argument("--max-temp", default="200GB")
    return parser.parse_args()


def open_duckdb(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    # ATTOM is far too big for memory, so give DuckDB a cap and somewhere to spill
    Path(args.tmp).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
    con.execute(f"SET memory_limit={quote_sql(args.memory)}")
    con.execute(f"SET max_temp_directory_size={quote_sql(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")
    return con


def addr_clean_sql(column: str) -> str:
    # scrub an ATTOM address down to a comparable form: lowercase, no punctuation,
    # single spaces, USPS abbreviations, and run-together street names split apart
    expr = f'replace(replace(lower(trim("{column}")), \',\', \'\'), \'.\', \'\')'
    expr = f"regexp_replace({expr}, '\\s+', ' ', 'g')"
    expr = f"regexp_replace({expr}, '\\s+city of.*$', '', 'g')"
    for long_form, short in ABBREVIATIONS:
        expr = f"regexp_replace({expr}, '\\b{long_form}\\b', '{short}', 'g')"
    expr = f"replace({expr}, '#', 'apt ')"
    for joined, spaced in RUN_TOGETHER:
        expr = f"regexp_replace({expr}, '\\b{joined}\\b', '{spaced}', 'g')"
    return f"trim(regexp_replace({expr}, '\\s+', ' ', 'g'))"


def addr_nosuffix_sql(clean: str) -> str:
    # drop the unit number and the street type, so "12 Main St" and "12 Main" agree
    no_unit = f"regexp_replace({clean}, '\\s+(apt|unit|ste)\\s+.*$', '')"
    return f"regexp_replace({no_unit}, '\\s+({STREET_SUFFIXES})$', '')"


def addr_compact_sql(clean: str) -> str:
    # squeeze the spaces out too, which catches the odd "Grove Way" vs "Groveway"
    return f"replace({addr_nosuffix_sql(clean)}, ' ', '')"


def zip_clean_sql(column: str) -> str:
    # keep the leading five digits and throw away any zip+4 tail. A property with
    # no usable zip comes back as an empty string rather than null, so the tier
    # guard below can spot it; null would survive as the text "nan" and look real
    return (f"coalesce(regexp_extract(replace(upper(trim(\"{column}\")), ' ', ''), "
            f"'^(\\d{{5}})', 1), '')")


def county_clean_sql(column: str, state_fips: str) -> str:
    # ATTOM writes county FIPS a few different ways, so rebuild the 5-digit version.
    # Empty string for a missing one, for the same reason as the zip above
    digits = f"regexp_extract(trim(\"{column}\"), '(\\d+)', 1)"
    return f"""
        coalesce(
            CASE WHEN length({digits}) = 5 THEN {digits}
                 WHEN length({digits}) = 4 AND starts_with({digits}, {quote_sql(state_fips)})
                      THEN {quote_sql(state_fips)} || lpad(substr({digits}, 3), 3, '0')
                 WHEN length({digits}) = 3 THEN {quote_sql(state_fips)} || {digits}
                 ELSE {digits} END, '')
    """

#more clean ups, with a lot being exceptions/cases caught after inspection and with Claude
def clean_address(addresses: pd.Series, locality: pd.Series, state: pd.Series) -> pd.Series:
    # the permit-side twin of addr_clean_sql, plus the extra mess that only Builty has
    text = addresses.fillna("").str.lower().str.strip()

    # anything after the first comma is city/state/zip, not the street address
    text = text.str.split(",").str[0]
    text = text.str.replace(",", "", regex=False).str.replace(".", "", regex=False)

    # strip a trailing zip, in either the zip+4 or the run-together form
    text = text.str.replace(r'\b(\d{5})-\d{4}\b', r'\1', regex=True)
    text = text.str.replace(r'\b(\d{5})\d{4}\b', r'\1', regex=True)
    text = text.str.replace(r'\b\d{4,5}\b\s*$', '', regex=True)

    # some sources tack the state and town onto the end of the street address
    states_short = set(state.fillna("").str.lower().str.strip()) - {""}
    states_full = {STATE_NAMES.get(code, code) for code in states_short}
    for name in sorted(states_full, key=len, reverse=True):
        text = text.str.replace(rf'\b{re.escape(name)}\b\s*$', '', regex=True)

    localities = locality.fillna("").str.lower().str.strip()
    localities = localities.str.replace(r'[/\-()\.,]', ' ', regex=True)
    localities = localities.str.replace(r'\s+', ' ', regex=True).str.strip()
    state_tail = "|".join(re.escape(code) for code in states_short | states_full)
    for town in sorted(set(localities) - {""}, key=len, reverse=True):
        text = text.str.replace(
            rf'\b{re.escape(town)}(?:\s+beach)?(?:\s+(?:{state_tail}))?\b\s*$', '', regex=True)

    # Norwalk exports addresses as "STREET (house number)", so flip those round
    text = text.str.replace(r'^(.+?)\s*\((\d{1,5})\)(?:\s+.*)?$', r'\2 \1', regex=True)
    text = text.str.replace(r'^0+(\d+)\s+', r'\1 ', regex=True)

    # Old Saybrook hangs a parcel marker off the end
    text = text.str.replace(r'-\d+\s*$', '', regex=True)
    text = text.str.replace(r'\s+city of.*$', '', regex=True)

    # now the same abbreviations ATTOM gets, plus a few Builty-only spellings
    for long_form, short in ABBREVIATIONS:
        text = text.str.replace(rf'\b{long_form}\b', short, regex=True)
    text = text.str.replace(r'#', 'apt ', regex=True)
    text = text.str.replace(r'\bcr\b', 'cir', regex=True)
    text = text.str.replace(r'\bpk\b', 'park', regex=True)
    text = text.str.replace(r'\bla\b$', 'ln', regex=True)

    # a doubled-up street type such as "oak trl trl" is a data entry slip
    text = text.str.replace(r'\b(trl|rd|st|ave|dr|ln|ct)\s+\1\b$', r'\1', regex=True)
    text = text.str.replace(r'\btrl\s+trl\b', 'trl', regex=True)

    # two addresses we could only fix by hand
    text = text.str.replace(r'^152 overshores dr e$', '152 overshores e', regex=True)
    text = text.str.replace(r'\s+milton$', '', regex=True)

    return text.str.replace(r'\s+', ' ', regex=True).str.strip()


def recover_zip(zipcodes: pd.Series, addresses: pd.Series) -> pd.Series:
    # use the zip field when it has one, otherwise dig a zip out of the address text
    stated = zipcodes.fillna("").str.strip().str.extract(r'^(\d{5})')[0].fillna("")
    text = addresses.fillna("").astype(str)
    found = (text.str.extract(r'\b(\d{5})-\d{4}\b')[0]
             .fillna(text.str.extract(r'\b(\d{5})\d{4}\b')[0])
             .fillna(text.str.extract(r'\b(\d{5})\b')[0]).fillna(""))
    return stated.mask(stated == "", found)


def load_permits(con: duckdb.DuckDBPyConnection, path: Path, state: str) -> pd.DataFrame:
    # read the national Builty elevation file and keep this state's rows
    if path.suffix.lower() == ".dta":
        permits = pd.read_stata(path, convert_categoricals=False)
    else:
        permits = con.execute(f"SELECT * FROM read_parquet({quote_sql(str(path))})").df()
    permits.columns = [column.upper() for column in permits.columns]
    return permits[permits["STATE"].fillna("").str.upper().eq(state)].copy()


def add_permit_dates(permits: pd.DataFrame) -> None:
    # the canonical file gives one YEAR per property; older permit-level extracts
    # instead carry three raw date fields, so fall back through those
    if "YEAR" in permits.columns:
        permits["permit_year"] = pd.to_numeric(permits["YEAR"], errors="coerce")
        permits["permit_date"] = pd.to_datetime(
            permits["permit_year"].astype("Int64").astype(str) + "-01-01", errors="coerce")
    else:
        permits["permit_date"] = pd.to_datetime(permits["DATE_ISSUED"], errors="coerce")
        for fallback in ["DATE_SUBMITTED", "DATE_FINALED"]:
            permits["permit_date"] = permits["permit_date"].fillna(
                pd.to_datetime(permits[fallback], errors="coerce"))
        permits["permit_year"] = permits["permit_date"].dt.year


def attom_column_map(con: duckdb.DuckDBPyConnection, attom_input: str) -> tuple[dict, bool]:
    # this runs against either the geocoded panel or the raw ATTOM extract, which
    # name their columns differently, so work out which schema we were handed
    columns = {row[0] for row in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({quote_sql(attom_input)})").fetchall()}
    raw = "PROPERTYADDRESSFULL" in columns
    if not raw:
        return {"address": "property_address_full", "zip": "zip", "county": "countycode",
                "blockgroup": "censusblockgroupfips", "raw": False, "columns": columns}, False
    return {"address": "PROPERTYADDRESSFULL", "zip": "PROPERTYADDRESSZIP",
            "county": "SITUSSTATECOUNTYFIPS", "blockgroup": None,
            "raw": True, "columns": columns}, True

#use taxassessed as market value and information is often unavailable and missing
def attom_select_sql(schema: dict) -> str:
    # line up the fields we want under one set of names regardless of which
    # ATTOM file we read. A None source just becomes an empty column
    raw, columns = schema["raw"], schema["columns"]
    optional = lambda name: name if raw and name in columns else (None if raw else name.lower())
    wanted = [
        (schema["address"], schema["address"]),
        (schema["zip"], "attom_zipcode"),
        (schema["county"], schema["county"]),
        ("ATTOMID" if raw else "attomid", "ATTOMID"),
        ("TAXASSESSEDVALUETOTAL" if raw else "assessed_value_total", "TAXASSESSEDVALUETOTAL"),
        ("TAXASSESSEDVALUEIMPROVEMENTS" if raw else "assessed_value_improvements", "TAXASSESSEDVALUEIMPROVEMENTS"),
        ("PREVIOUSASSESSEDVALUE" if raw else "previous_assessed_value", "PREVIOUSASSESSEDVALUE"),
        ("TAXYEARASSESSED" if raw else "year", "TAXYEARASSESSED"),
        ("YEARBUILT" if raw else "construction_year", "YEARBUILT"),
        ("YEARBUILTEFFECTIVE" if raw else "construction_year", "YEARBUILTEFFECTIVE"),
        (schema["blockgroup"], "censusblockgroupfips"),
        (None if raw else "geocode_match", "geocode_match"),
        (optional("LONGITUDE"), "longitude"),
        (optional("LATITUDE"), "latitude"),
    ]
    return ", ".join(f'"{source}" AS "{target}"' if source else f"'' AS \"{target}\""
                     for source, target in wanted)


def load_attom(con: duckdb.DuckDBPyConnection, attom_input: str, schema: dict,
               permits: pd.DataFrame, state_fips: str) -> pd.DataFrame:
    # ATTOM has tens of millions of rows and we only care about the handful of
    # addresses that appear in Builty, so push that filter down into the scan
    con.register("permit_keys", permits[["addr_clean", "addr_nosuffix", "addr_compact"]].drop_duplicates())
    address, clean = schema["address"], addr_clean_sql(schema["address"])

    attom = con.execute(f"""
        SELECT {attom_select_sql(schema)},
               {clean} AS addr_clean,
               {addr_nosuffix_sql(clean)} AS addr_nosuffix,
               {addr_compact_sql(clean)} AS addr_compact,
               {zip_clean_sql("attom_zipcode")} AS zip_clean,
               {county_clean_sql(schema["county"], state_fips)} AS attom_county_fips,
               1 AS attom_record_present
        FROM read_parquet({quote_sql(attom_input)})
        WHERE "{address}" IS NOT NULL AND trim("{address}") != ''
          AND ({clean} IN (SELECT addr_clean FROM permit_keys)
               OR {addr_nosuffix_sql(clean)} IN (SELECT addr_nosuffix FROM permit_keys)
               OR {addr_compact_sql(clean)} IN (SELECT addr_compact FROM permit_keys))
    """).df()

    attom = attom.drop(columns=[address, schema["county"]])
    attom["attom_assessment_year"] = pd.to_numeric(attom.pop("TAXYEARASSESSED"), errors="coerce")
    return attom


def apply_temporal_match(permits: pd.DataFrame, attom: pd.DataFrame, value_columns: list[str],
                         permit_keys: list[str], attom_keys: list[str],
                         tier: str, require_unique: bool) -> pd.DataFrame:
    # only permits that no earlier tier has claimed are still up for grabs
    unmatched = permits["attom_match_tier"] == "unmatched"

    # a candidate needs both key parts filled in and an assessment year to rank on
    candidates = attom[attom[attom_keys[0]].astype(str).str.len().gt(0)
                       & attom[attom_keys[1]].astype(str).str.len().gt(0)
                       & attom["attom_assessment_year"].notna()].copy()

    # on the looser tiers, only trust the key when it points at a single property
    if require_unique:
        counts = candidates.groupby(attom_keys)["ATTOMID"].nunique().rename("n").reset_index()
        candidates = candidates.merge(counts, on=attom_keys, how="left")
        candidates = candidates[candidates["n"] <= 1].drop(columns="n")

    candidates = candidates[attom_keys + value_columns].rename(columns=dict(zip(attom_keys, permit_keys)))
    matches = permits.loc[unmatched, ["permit_row_id", "permit_year"] + permit_keys].merge(
        candidates, on=permit_keys, how="left")
    matches = matches[matches["attom_record_present"].notna()]

    # a property has many assessment years, so pick one: the latest year at or
    # before the permit if there is one, otherwise the earliest year after it
    missing_year = matches["permit_year"].isna()
    is_prior = matches["attom_assessment_year"] <= matches["permit_year"]
    matches = matches.assign(
        is_prior=is_prior,
        rank=np.where(missing_year, -matches["attom_assessment_year"],
                      np.where(is_prior, -matches["attom_assessment_year"],
                               matches["attom_assessment_year"])))

    # break any remaining tie on ATTOMID, so reruns give the same answer every time
    matches["attomid_tie"] = matches["ATTOMID"].astype("string").fillna("")
    matches = matches.sort_values(["permit_row_id", "is_prior", "rank", "attomid_tie"],
                                  ascending=[True, False, True, True],
                                  kind="mergesort").drop_duplicates("permit_row_id")
    matches["attom_value_asof"] = np.where(
        missing_year.loc[matches.index], "undated",
        np.where(matches["is_prior"], "prior", "post"))

    # write the winners back onto the permit rows they belong to
    by_id = permits.set_index("permit_row_id", drop=False)
    winners = matches.set_index("permit_row_id")
    for column in value_columns + ["attom_value_asof"]:
        by_id.loc[winners.index, column] = winners[column]
    by_id.loc[winners.index, "attom_match_tier"] = tier
    return by_id.reset_index(drop=True)


def write_diagnostics(permits: pd.DataFrame, path: Path) -> None:
    """Write two views of the match: a per-tier summary and a county detail file.

    The summary is the one to read first. It is one row per tier plus a total,
    so a reviewer can see the whole state's match rate without aggregating
    anything, and it lines up with {state}_tier_diagnostics.csv from the
    assignment step. The detail file is for locating *where* a bad rate lives.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # Two coverage flags drive every number below: did the permit pick up an
    # ATTOM value, and did it pick up a block group.
    flagged = permits.assign(has_prop_value=permits["prop_value"].notna(),
                             has_blockgroup=permits["attom_has_blockgroup"].eq(1))

    def coverage(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        # Count permits and the two coverage flags over whatever keys are given,
        # then turn the counts into rates. dropna=False keeps rows with a missing
        # county or year visible instead of dropping them from the totals.
        out = (frame.groupby(keys, dropna=False)
               .agg(n=("permit_row_id", "size"),
                    n_prop_value=("has_prop_value", "sum"),
                    n_blockgroup=("has_blockgroup", "sum")).reset_index())
        out["prop_value_rate"] = out["n_prop_value"] / out["n"]
        out["blockgroup_rate"] = out["n_blockgroup"] / out["n"]
        return out

    # Per-tier summary, ordered by the tier ladder so it reads top-down like the
    # matching itself, with unmatched permits last.
    by_tier = coverage(flagged, ["attom_match_tier"])
    # MATCH_TIERS rows are (permit_keys, attom_keys, tier_label, require_unique)
    order = [tier for _, _, tier, _ in MATCH_TIERS] + ["unmatched"]
    by_tier["tier_number"] = by_tier["attom_match_tier"].map(
        {tier: i for i, tier in enumerate(order, start=1)})
    by_tier = by_tier.sort_values("tier_number").reset_index(drop=True)
    by_tier["permit_share"] = by_tier["n"] / len(permits)

    # A total row makes the headline match rate readable without any arithmetic.
    total = coverage(flagged.assign(_all=1), ["_all"]).drop(columns="_all")
    total.insert(0, "attom_match_tier", "TOTAL")
    total["tier_number"] = len(order) + 1
    total["permit_share"] = 1.0

    summary_path = path.with_name(f"{path.stem}_by_tier.csv")
    pd.concat([by_tier, total], ignore_index=True).to_csv(summary_path, index=False)

    # County x year x tier detail, so a single bad county or vintage is findable.
    coverage(flagged, ["county_fips", "permit_year", "attom_match_tier"]).to_csv(path, index=False)
    print(f"Saved: {summary_path}")


def reorder_columns(permits: pd.DataFrame) -> pd.DataFrame:
    # put the interesting columns up front so the file is readable by eye
    front = ["county_fips", "zip_clean", "permit_year", "permit_date", "BUILTY_ID", "addr_clean",
             "prop_value", "pre_flood_assessed_value", "attom_assessment_year", "attom_value_asof",
             "attom_zipcode", "TAXASSESSEDVALUETOTAL", "TAXASSESSEDVALUEIMPROVEMENTS",
             "PREVIOUSASSESSEDVALUE", "YEARBUILT", "YEARBUILTEFFECTIVE", "attom_match_tier",
             "censusblockgroupfips", "geocode_match", "longitude", "latitude",
             "attom_has_blockgroup", "PROJECT_VALUE", "val_cost_ratio"]
    ordered = [c for c in front if c in permits.columns]
    rest = [c for c in permits.columns if c not in ordered]
    return permits[ordered + rest].sort_values(["county_fips", "permit_year"]).reset_index(drop=True)


def main() -> None:
    args = parse_args()
    state = args.state.upper()
    data = Path(args.data)

    permits_path = Path(args.permits) if args.permits else data / "build" / "builty_elevations_zipfilled.dta"
    attom_input = args.attom or str(data / "raw" / "attom" / f"attom_{state.lower()}.parquet")
    out_path = Path(args.out) if args.out else data / "build" / f"{state.lower()}_attom_permits.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = open_duckdb(args)

    # bring in this state's elevation permits
    permits = load_permits(con, permits_path, state)

    # a couple of states have no elevation permits at all, so write the empty
    # shell the next script expects and stop here
    if len(permits) == 0:
        for column, dtype in [("ATTOMID", "string"), ("permit_year", "float64"),
                              ("attom_match_tier", "string")]:
            permits[column] = pd.Series(dtype=dtype)
        permits.to_parquet(out_path, index=False)
        print(f"{state}: 0 permits, wrote empty output")
        return

    # work out when each elevation happened and which county it sits in
    add_permit_dates(permits)
    state_fips = permits["FIPS_STATE"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(2)
    county_fips = permits["FIPS_COUNTY"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(3)
    permits["county_fips"] = state_fips + county_fips

    # build the three address keys and the zip key the match ladder runs on
    permits["permit_row_id"] = np.arange(len(permits))
    permits["addr_clean"] = clean_address(permits["STREET_ADDRESS"], permits["LOCALITY"], permits["STATE"])
    permits["zip_clean"] = recover_zip(permits["ZIPCODE"], permits["STREET_ADDRESS"])
    permits["addr_nosuffix"] = (permits["addr_clean"]
                                .str.replace(rf'\s+({STREET_SUFFIXES})$', "", regex=True).str.strip()
                                .str.replace(r'\s+(apt|unit|ste)\s+.*$', '', regex=True).str.strip())
    permits["addr_compact"] = permits["addr_nosuffix"].str.replace(" ", "", regex=False)

    # pull the matching ATTOM records and clean their addresses the same way
    schema, _ = attom_column_map(con, attom_input)
    attom = load_attom(con, attom_input, schema, permits, state_fips.iloc[0])

    # start every permit off unmatched, with blank columns waiting to be filled
    value_columns = [c for c in attom.columns if c not in KEY_COLUMNS]
    for column in value_columns:
        permits[column] = pd.Series(pd.NA, index=permits.index, dtype="object")
    permits["attom_match_tier"] = "unmatched"
    permits["attom_value_asof"] = pd.Series(pd.NA, index=permits.index, dtype="object")

    # run the ladder, each tier only picking up what the tighter ones left behind
    for permit_keys, attom_keys, tier, require_unique in MATCH_TIERS:
        permits = apply_temporal_match(permits, attom, value_columns,
                                       permit_keys, attom_keys, tier, require_unique)

    # where Builty had no zip but the matched ATTOM record does, borrow theirs
    attom_zip = permits["attom_zipcode"].fillna("").astype(str).str.extract(r"^(\d{5})", expand=False).fillna("")
    borrowed = permits["zip_clean"].eq("") & attom_zip.ne("") & permits["attom_match_tier"].ne("unmatched")
    permits.loc[borrowed, "zip_clean"] = attom_zip[borrowed]
    permits.loc[borrowed, "ZIPCODE"] = attom_zip[borrowed]
    if "ZIPCODE_SOURCE" not in permits.columns:
        permits["ZIPCODE_SOURCE"] = ""
    permits.loc[borrowed, "ZIPCODE_SOURCE"] = "attom"

    # the value columns came back as objects, so make them numbers again
    for column in ["TAXASSESSEDVALUETOTAL", "TAXASSESSEDVALUEIMPROVEMENTS", "PREVIOUSASSESSEDVALUE",
                   "YEARBUILT", "YEARBUILTEFFECTIVE", "attom_assessment_year", "PROJECT_VALUE"]:
        if column not in permits.columns:
            permits[column] = np.nan
        permits[column] = pd.to_numeric(permits[column], errors="coerce")

    # the headline property value, falling back to the prior year where needed
    permits["pre_flood_assessed_value"] = permits["TAXASSESSEDVALUETOTAL"].fillna(permits["PREVIOUSASSESSEDVALUE"])
    permits["prop_value"] = permits["pre_flood_assessed_value"]

    # how the house was worth against what the elevation cost
    usable = ((permits["prop_value"] > 0) & (permits["PROJECT_VALUE"] > 0)).fillna(False)
    permits["val_cost_ratio"] = np.where(usable, permits["prop_value"] / permits["PROJECT_VALUE"], np.nan)
    permits["attom_has_blockgroup"] = (
        permits["censusblockgroupfips"].fillna("").astype(str).str.len() == 12).astype(int)

    # save the matched file and its coverage table
    diagnostics_path = (Path(args.diagnostics) if args.diagnostics
                        else out_path.with_name(f"{out_path.stem}_attom_diagnostics.csv"))
    write_diagnostics(permits, diagnostics_path)
    permits = reorder_columns(permits)
    permits.to_parquet(out_path, index=False)

    matched = int((permits["attom_match_tier"] != "unmatched").sum())
    print(f"{state}: {len(permits):,} permits, {matched:,} matched "
          f"({matched / len(permits):.0%}), saved {out_path.name}")


if __name__ == "__main__":
    main()

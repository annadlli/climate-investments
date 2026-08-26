"""
Merge ATTOM property values onto the NFIP policy universe.

Authors: Anna Li
Date: 2026-08-04

Description:
    NFIP policy records carry no street address (lat/long are coarsened), so
    ATTOM cannot be joined to NFIP parcel-to-parcel. What NFIP does carry is
    censusBlockGroupFips, and ATTOM now carries a block group too - geocoded
    from its addresses by geocode_attom.py / build_attom_geocoded.py. This
    script aggregates the geocoded ATTOM panel to value cells and merges those
    cells onto NFIP policy-years with a tiered cascade, finest geography and
    construction-year grain first:

        1. block group x construction year
        2. block group x construction 5-year bin
        3. block group x construction decade
        4. block group
        5. ZIP x construction decade
        6. ZIP
        7. county x construction decade
        8. county

    Each NFIP row takes the first tier that hits; the tier is recorded in
    attom_tier, so any tier can be dropped downstream. Within a tier, ATTOM tax
    year is matched as-of the policy year: the latest assessment at or before
    policy_year, falling back to the earliest assessment after it when ATTOM
    history starts later (flagged in attom_value_asof, as in
    build_attom_onto_permits.py).

    Sample restriction: ATTOM is filtered to single-family residences
    (PROPERTYUSESTANDARDIZED 385) to match the NFIP single-family universe from
    clean_nfip_policies.do. Pass --use-codes all to keep every property type.

    Output stays at NFIP policy-year level. The attached values are block-group
    (or ZIP/county) cell aggregates, not exact parcel matches.

    Input  : {data}/clean/nfip_policies_state/{st}.dta   (clean_nfip_policies.do)
             {data}/build/{st}_attom_geocoded.parquet    (build_attom_geocoded.py)
    Output : {data}/build/{st}_nfip_attom.parquet + .dta
             {data}/build/{st}_nfip_attom_tiers.csv      (coverage diagnostics)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


# cell aggregates carried onto every matched nfip row
VALUE_COLS = [
    "attom_n_properties",
    "attom_mean_market_total",
    "attom_median_market_total",
    "attom_mean_assessed_total",
    "attom_median_assessed_total",
]

# tier label -> join keys, finest first. every tier also matches as-of tax year.
TIERS = [
    ("blockgroup_construction_year",   ["bg_key", "construction_year"]),
    ("blockgroup_construction_5yr",    ["bg_key", "construction_5yr"]),
    ("blockgroup_construction_decade", ["bg_key", "construction_decade"]),
    ("blockgroup",                     ["bg_key"]),
    ("zip_construction_decade",        ["zip_key", "construction_decade"]),
    ("zip",                            ["zip_key"]),
    ("county_construction_decade",     ["county_key", "construction_decade"]),
    ("county",                         ["county_key"]),
]

# nfip columns the merge needs; everything else rides along untouched
NFIP_KEYS = ["censusblockgroupfips", "zipcode", "countycode", "construction_year", "policy_year"]


def quote(s: str) -> str:
    # sql-safe strings
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Merge geocoded ATTOM value cells onto NFIP policy-years."
    )
    p.add_argument("--state", required=True)
    p.add_argument("--data",  required=True, help="Data root (from master.do).")
    p.add_argument("--nfip",  default=None, help="Cleaned NFIP policy-year .dta.")
    p.add_argument("--attom", default=None, help="Geocoded ATTOM panel .parquet.")
    p.add_argument("--out",   default=None, help="Output .parquet; .dta is written alongside.")
    p.add_argument("--diagnostics", default=None)
    p.add_argument(
        "--bg-vintage", default="current", choices=["current", "2010"],
        help="ATTOM block-group vintage to match on. NFIP block groups are 2010-vintage "
             "in the FEMA file, so '2010' is the like-for-like join when that column exists.",
    )
    p.add_argument(
        "--use-codes", default="385",
        help="Comma-separated ATTOM PROPERTYUSESTANDARDIZED codes to keep "
             "(385 = single-family residence). Pass 'all' to keep every property type.",
    )
    p.add_argument(
        "--min-cell", default=1, type=int,
        help="Drop cells built from fewer than this many distinct ATTOM properties.",
    )
    p.add_argument("--no-dta", action="store_true", help="Skip the Stata output.")
    p.add_argument("--tmp",      default="/tmp")
    p.add_argument("--threads",  default=4, type=int)
    p.add_argument("--memory",   default="32GB")
    p.add_argument("--max-temp", default="800GB")
    return p.parse_args()


def resolve_nfip(data: Path, state: str, override: str | None) -> Path:
    if override:
        return Path(override)
    path = data / "clean" / "nfip_policies_state" / f"{state}.dta"
    if not path.exists():
        raise FileNotFoundError(
            f"No cleaned NFIP file at {path}; run clean_nfip_policies.do for {state.upper()} first."
        )
    return path


def resolve_attom(data: Path, state: str, override: str | None) -> Path:
    if override:
        return Path(override)
    path = data / "build" / f"{state}_attom_geocoded.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"No geocoded ATTOM panel at {path}; run build_attom_geocoded.py for {state.upper()} first."
        )
    return path


# ---------------------------------------------------------------------------
# key normalization
# ---------------------------------------------------------------------------
# Geographic ids are labels spelled with digits, and both sources have lost
# leading zeros somewhere upstream (FEMA on ZIP, Dewey on ZIP and county FIPS).
# Pull the digits out, restore the padding, and blank anything still the wrong
# width so it can never join on a truncated code.
def digits_sql(col: str) -> str:
    return f"regexp_replace(cast({col} AS varchar), '[^0-9]', '', 'g')"


def padded_key_sql(col: str, width: int) -> str:
    d = digits_sql(col)
    return (
        f"CASE WHEN length({d}) BETWEEN {width - 1} AND {width} "
        f"THEN lpad({d}, {width}, '0') ELSE '' END"
    )


# ATTOM's own zip column is blank wherever the leading zero was stripped
# upstream, so fall back to the ZIP written into the address line.
def attom_zip_sql() -> str:
    from_col = padded_key_sql('"zip"', 5)
    from_addr = (
        "CASE WHEN regexp_extract(property_address_full, '(\\d{5})(-\\d{4})?\\s*$', 1) != '' "
        "THEN regexp_extract(property_address_full, '(\\d{5})(-\\d{4})?\\s*$', 1) ELSE '' END"
    )
    return f"CASE WHEN {from_col} != '' THEN {from_col} ELSE {from_addr} END"


# ---------------------------------------------------------------------------
# inputs
# ---------------------------------------------------------------------------
def load_nfip(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    # stata is a pandas read; everything after this stays in duckdb
    nfip = pd.read_stata(path, convert_categoricals=False)
    missing = [c for c in NFIP_KEYS if c not in nfip.columns]
    if missing:
        raise KeyError(f"NFIP file {path} is missing {missing}")
    con.register("nfip_raw", nfip)
    con.execute(f"""
        CREATE OR REPLACE TABLE nfip AS
        SELECT
            row_number() OVER () AS _id,
            * EXCLUDE (censusblockgroupfips, zipcode, countycode),
            {padded_key_sql('censusblockgroupfips', 12)} AS censusblockgroupfips,
            {padded_key_sql('zipcode', 5)}               AS zipcode,
            {padded_key_sql('countycode', 5)}            AS countycode,
            {padded_key_sql('censusblockgroupfips', 12)} AS bg_key,
            {padded_key_sql('zipcode', 5)}               AS zip_key,
            {padded_key_sql('countycode', 5)}            AS county_key,
            (construction_year // 5)  * 5  AS construction_5yr,
            (construction_year // 10) * 10 AS construction_decade
        FROM nfip_raw
    """)
    con.unregister("nfip_raw")
    return con.execute("SELECT count(*) FROM nfip").fetchone()[0]


def load_attom(con: duckdb.DuckDBPyConnection, path: Path, bg_vintage: str,
               use_codes: str) -> tuple[int, int]:
    cols = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({quote(str(path))})"
    ).fetchall()}

    bg_col = "censusblockgroupfips2010" if bg_vintage == "2010" else "censusblockgroupfips"
    if bg_col not in cols:
        raise KeyError(
            f"{path} has no {bg_col} column; rebuild it or pass --bg-vintage current"
        )

    if use_codes.strip().lower() == "all":
        use_filter = "TRUE"
    else:
        codes = ", ".join(quote(c.strip()) for c in use_codes.split(",") if c.strip())
        use_filter = f"trim(coalesce(property_use_std, '')) IN ({codes})"

    con.execute(f"""
        CREATE OR REPLACE TABLE attom AS
        SELECT
            attomid,
            year,
            {padded_key_sql(bg_col, 12)}      AS bg_key,
            {attom_zip_sql()}                 AS zip_key,
            {padded_key_sql('countycode', 5)} AS county_key,
            construction_year,
            (construction_year // 5)  * 5  AS construction_5yr,
            (construction_year // 10) * 10 AS construction_decade,
            CASE WHEN market_value_total   > 0 THEN market_value_total   END AS market_total,
            CASE WHEN assessed_value_total > 0 THEN assessed_value_total END AS assessed_total
        FROM read_parquet({quote(str(path))})
        WHERE year BETWEEN 1980 AND 2035
          AND (market_value_total > 0 OR assessed_value_total > 0)
          AND {use_filter}
    """)
    n_rows, n_props = con.execute(
        "SELECT count(*), count(DISTINCT attomid) FROM attom"
    ).fetchone()
    return n_rows, n_props


# ---------------------------------------------------------------------------
# tiered cell merge
# ---------------------------------------------------------------------------
# One cell table per tier: the tier's keys x ATTOM tax year. Built on demand so
# a tier nothing is left to match against is never aggregated.
def build_cells(con: duckdb.DuckDBPyConnection, keys: list[str], min_cell: int) -> int:
    key_sql = ", ".join(keys)
    notnull = " AND ".join(f"{k} IS NOT NULL" for k in keys)
    blank = " AND ".join(f"{k} != ''" for k in keys if k.endswith("_key"))
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cells AS
        SELECT {key_sql}, year,
            count(DISTINCT attomid) AS attom_n_properties,
            avg(market_total)       AS attom_mean_market_total,
            median(market_total)    AS attom_median_market_total,
            avg(assessed_total)     AS attom_mean_assessed_total,
            median(assessed_total)  AS attom_median_assessed_total
        FROM attom
        WHERE {notnull}{' AND ' + blank if blank else ''}
        GROUP BY {key_sql}, year
        HAVING count(DISTINCT attomid) >= {min_cell}
    """)
    return con.execute("SELECT count(*) FROM cells").fetchone()[0]


# Match the still-unmatched NFIP rows against this tier's cells, taking the
# latest assessment at or before the policy year, else the earliest one after.
def apply_tier(con: duckdb.DuckDBPyConnection, keys: list[str], label: str) -> int:
    join = " AND ".join(f"e.{k} = c.{k}" for k in keys)
    notnull = " AND ".join(f"e.{k} IS NOT NULL" for k in keys)
    blank = " AND ".join(f"e.{k} != ''" for k in keys if k.endswith("_key"))
    con.execute(f"""
        INSERT INTO matches
        SELECT
            e._id,
            {quote(label)} AS attom_tier,
            c.year         AS attom_year,
            CASE WHEN c.year <= e.policy_year THEN 'prior' ELSE 'post' END AS attom_value_asof,
            c.attom_n_properties,
            c.attom_mean_market_total,
            c.attom_median_market_total,
            c.attom_mean_assessed_total,
            c.attom_median_assessed_total
        FROM nfip AS e
        INNER JOIN cells AS c ON {join}
        WHERE e.policy_year IS NOT NULL
          AND {notnull}{' AND ' + blank if blank else ''}
          AND NOT EXISTS (SELECT 1 FROM matches m WHERE m._id = e._id)
        QUALIFY row_number() OVER (
            PARTITION BY e._id
            ORDER BY (c.year <= e.policy_year) DESC,
                     CASE WHEN c.year <= e.policy_year THEN -c.year ELSE c.year END
        ) = 1
    """)
    return con.execute(
        f"SELECT count(*) FROM matches WHERE attom_tier = {quote(label)}"
    ).fetchone()[0]


def main() -> None:
    args = parse_args()
    state = args.state.lower()
    data = Path(args.data)
    nfip_path = resolve_nfip(data, state, args.nfip)
    attom_path = resolve_attom(data, state, args.attom)
    out_path = Path(args.out) if args.out else data / "build" / f"{state}_nfip_attom.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostics_path = (
        Path(args.diagnostics) if args.diagnostics
        else out_path.with_name(f"{out_path.stem}_tiers.csv")
    )

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote(args.tmp)}")
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET max_temp_directory_size={quote(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    print(f"NFIP  : {nfip_path}")
    print(f"ATTOM : {attom_path}  (block groups: {args.bg_vintage}, use codes: {args.use_codes})")

    n_nfip = load_nfip(con, nfip_path)
    n_rows, n_props = load_attom(con, attom_path, args.bg_vintage, args.use_codes)
    print(f"NFIP policy-years          : {n_nfip:,}")
    print(f"ATTOM property-years kept  : {n_rows:,} ({n_props:,} properties)")

    con.execute("""
        CREATE OR REPLACE TABLE matches (
            _id BIGINT, attom_tier VARCHAR, attom_year INTEGER, attom_value_asof VARCHAR,
            attom_n_properties BIGINT, attom_mean_market_total DOUBLE,
            attom_median_market_total DOUBLE, attom_mean_assessed_total DOUBLE,
            attom_median_assessed_total DOUBLE
        )
    """)

    for label, keys in TIERS:
        remaining = con.execute(
            "SELECT count(*) FROM nfip e WHERE e.policy_year IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM matches m WHERE m._id = e._id)"
        ).fetchone()[0]
        if remaining == 0:
            print(f"  tier {label:<32} skipped (nothing left to match)")
            continue
        n_cells = build_cells(con, keys, args.min_cell)
        n_hits = apply_tier(con, keys, label)
        cumulative = con.execute("SELECT count(*) FROM matches").fetchone()[0]
        print(f"  tier {label:<32} {n_cells:>10,} cells  matched {n_hits:>9,}  "
              f"(cumulative {cumulative:>9,} / {n_nfip:,})")

    con.execute(f"""
        CREATE OR REPLACE TABLE nfip_attom AS
        SELECT
            n.* EXCLUDE (_id, construction_5yr, construction_decade),
            coalesce(m.attom_tier, 'unmatched') AS attom_tier,
            m.attom_year,
            m.attom_value_asof,
            {', '.join('m.' + c for c in VALUE_COLS)}
        FROM nfip AS n
        LEFT JOIN matches AS m USING (_id)
    """)

    matched = con.execute(
        "SELECT count(*) FROM nfip_attom WHERE attom_tier != 'unmatched'"
    ).fetchone()[0]
    print(f"ATTOM value attached: {matched:,} / {n_nfip:,} ({matched / max(n_nfip, 1):.1%})")

    # coverage by tier and by policy year, for the cluster log and for QA
    diagnostics = con.execute("""
        SELECT attom_tier, policy_year,
            count(*) AS n,
            sum(CASE WHEN attom_value_asof = 'prior' THEN 1 ELSE 0 END) AS n_prior,
            sum(CASE WHEN attom_value_asof = 'post'  THEN 1 ELSE 0 END) AS n_post,
            avg(attom_median_market_total)   AS mean_cell_median_market,
            avg(attom_median_assessed_total) AS mean_cell_median_assessed
        FROM nfip_attom
        GROUP BY attom_tier, policy_year
        ORDER BY attom_tier, policy_year
    """).df()
    diagnostics.to_csv(diagnostics_path, index=False)

    con.execute(f"COPY nfip_attom TO {quote(str(out_path))} (FORMAT parquet)")
    print(f"Saved: {out_path}")
    print(f"Saved: {diagnostics_path}")

    if not args.no_dta:
        dta_path = out_path.with_suffix(".dta")
        df = con.execute("SELECT * FROM nfip_attom").df()
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].fillna("").astype(str)
        df.to_stata(dta_path, write_index=False, version=118)
        print(f"Saved: {dta_path}")

    con.close()


if __name__ == "__main__":
    main()

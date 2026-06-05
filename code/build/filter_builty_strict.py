"""
Create a strict Builty flood/home-elevation parquet from the raw Builty file.

This script is the Python version of the strict Builty filter. It starts from
the raw `builty_all.parquet`, keeps likely home elevation permits, removes
common false positives like elevators and builder elevation/model names, and
writes a parquet that can feed the HMA and ATTOM merge scripts. Flood-context
terms are retained as diagnostics and can optionally be required.

Example:
    python filter_builty_strict.py \
      --input data/raw/builty_all.parquet \
      --out data/clean/all_elevation.parquet
"""

import argparse
from pathlib import Path

import duckdb
import pandas as pd


STRICT_PATTERNS = [
    r"(^| )house elevation($| )",
    r"(^| )home elevation($| )",
    r"(^| )residential house elevation($| )",
    r"flood damage house elevation",
    r"flood damaged house elevation",
    r"elevat(e|ing) (existing )?(house|home)( |$)",
    r"rais(e|ed|ing) (the )?(house|home)( |$)",
    r"(^| )(house|home)( |$).{0,40}rais(e|ed|ing)",
    r"(^| )(house|home)( |$).{0,40}elevat",
    r"lowest floor of the house.{0,50}elevat",
    r"raise house to fema",
    r"raised to meet elevation requirements",
    r"elevation of existing home",
    r"floodplain.{0,40}elevat",
    r"elevat.{0,40}floodplain",
    r"flood zone.{0,40}(elevat|rais)",
    r"(elevat|rais).{0,40}flood zone",
    r"(rais|elevat).{0,40}base flood elevation",
    r"base flood elevation.{0,40}(rais|increas|meet|comply|above|compli)",
    r"freeboard",
    r"nfip.{0,40}(rais|elevat)",
    r"(rais|elevat).{0,40}nfip",
    r"hazard mitigation.{0,40}(rais|elevat| home | house |structur|residen)",
    r"(rais|elevat).{0,40}hazard mitigation",
    r"substantially (damaged|improved).{0,40}(rais|elevat)",
    r"(rais|elevat).{0,40}substantially (damaged|improved)",
    r"sfr.{0,30}(elevat|rais)",
    r"(elevat|rais).{0,30}sfr",
    r"residential.{0,30}(elevat|rais).{0,30}(flood|fema|bfe|mitigation)",
    r"lift(ed|ing).{0,40}(house|home|structur|residen|dwelling|out of)",
    r"(^| )(house|home)( |$).{0,40}lift(ed|ing)",
    r"(structur|residen|dwelling).{0,40}lift(ed|ing)",
    r"lift(ed|ing).{0,40}(floodplain|flood|fema|icc)",
    r"out of (the )?floodplain",
    r"jack(ed|ing) up.{0,40}(house|home|structur|residen)",
]

FLOOD_CONTEXT_PATTERNS = [
    r"flood",
    r"fema",
    r"bfe",
    r"base flood elevation",
    r"floodplain",
    r"flood plain",
    r"flood zone",
    r"freeboard",
    r"nfip",
    r"icc",
    r"hazard mitigation",
    r"mitigation",
    r"substantially damaged",
    r"substantial damage",
    r"substantially improved",
    r"substantial improvement",
    r"storm surge",
]

FALSE_POSITIVE_PATTERNS = [
    r"flood damage repair",
    r"reconstruct|reconstruction|rebuild|new construction",
    r"elevator",
    r"pool",
    r"mobile.{0,20}home.{0,20}move.{0,20}in",
    r"manufactured.{0,20}home.{0,20}move.{0,20}in",
    r"mobile.{0,20}home.{0,20}set.{0,20}up",
    r"elevation.{0,20}certificate|elevation.{0,20}assessor|elevation assessor's permit",
    r"model house|elevatoions",
    r"determination of substantial conformance.{0,120}elevations?",
    r"site modifications.{0,80}elevations?",
    r"townhouse|town house",
    r"elevation *#? *[0-9]+",
    r"(house type|model|master *file|masterfile).{0,100}elevation",
    r"elevation.{0,100}(house type|model|master *file|masterfile)",
    r"(build new|new).{0,80}(home|townhouse|town house|sfd|single family|custom home|residence).{0,100}elevation",
    r"(unit|lot) +[a-z0-9]+.{0,40}elevation",
    r"plan *#? *[0-9a-z-]+.{0,80}elevations?",
    r"elevations?.{0,80}(std gate|standard plan|master|tract|lot [0-9]+|nsfr|new sfr|new house)",
    r"(new sfr|new house|new single family|single family home|single family residential).{0,100}elevations?",
    r"elevations?.{0,100}(new sfr|new house|new single family|single family home|single family residential)",
    r"(front|rear|side|north|south|east|west|left|right).{0,30}elevations?",
    r"elevations?.{0,30}(front|rear|side|north|south|east|west|left|right)",
    r"(left|right) swing elevation",
    r"plan[: ]+[0-9a-z ]+.*elevation[: ]+[a-z]",
    r"elevation (drawing|plan|view|sheet|detail)",
    r"elevation ['\"]?[a-z][0-9]?['\"]?( |$|,|;|:|\\.)",
    r"elevations? ['\"]?[a-zivx]+[0-9]?['\"]?( |$|,|;|:|\\.)",
    r"['\"]?[a-z][0-9]?['\"]? elevation",
    r"repeat.{0,40}elevation",
    r"elevation[- ]+[a-z][0-9]?( |$|,|;|:|\\.)",
    r"(grade|grading|pad|site|curb|street|road|drain) elevation",
    r"raised slab|elevated slab",
    r"raised ranch",
    r"rais(e|ed|ing).{0,40}roof|roof.{0,40}rais(e|ed|ing)",
    r"rais(e|ed|ing).{0,40}ceiling|ceiling.{0,40}rais(e|ed|ing)",
    r"rais(e|ed|ing).{0,40}(porch|deck|entry|garden|loop|meter|panel|wire|collar ties)",
    r"(porch|deck|entry|garden|loop|meter|panel|wire|collar ties).{0,40}rais(e|ed|ing)",
    r"service.{0,40}rais(e|ed|ing)|rais(e|ed|ing).{0,40}service",
    r"shower faucet|shower plumbing",
    r"fence permit|rock ?wall|privacy fence",
    r"back yard|front yard|side yard|rear yard",
    r"tree removal|tree pruning|prun(e|ing)|canopy|oak tree|laurel oak|roots.{0,60}(foundation|sidewalk|patio|deck)",
    r"elevat(e|ed|ing).{0,60}(tree|canopy|limb|roof)|(?:tree|canopy|limb).{0,60}elevat(e|ed|ing)",
    r"sign.{0,80}house raising|house raising.{0,80}sign",
    r"code compliance.{0,100}house raising",
    r"generator",
    r"water heater",
    r"signage|wall sign|channel letters",
    r"illuminated.{0,30}(sign|letter|cabinet)",
    r"finished floor",
    r"minimum ffe|minimun ffe|min ffe",
    r"elevation certificate",
    r"flood plain determination",
    r"raise (the )?roof",
    r"raise (the )?bar",
    r"patio addition rear elevation",
    r"front elevation refacing",
    r"new (home|sfr|single family|residence).{0,80}(plan |elevation [a-z]( |$))",
    r"(plan |master plan ).{0,30}elevation [a-z]( |$)",
    r"new (sfr|single family).{0,30}existing elevation",
]


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def regex_any_sql(column: str, patterns: list[str]) -> str:
    return " OR\n            ".join(
        f"regexp_matches({column}, {quote_sql(pattern)})" for pattern in patterns
    )


def existing_columns(con: duckdb.DuckDBPyConnection, parquet_path: str) -> set[str]:
    path_sql = quote_sql(parquet_path)
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({path_sql}) LIMIT 1"
    ).fetchall()
    return {row[0] for row in rows}


def project_root() -> Path:
    candidates = [Path.cwd(), *Path(__file__).resolve().parents]
    for candidate in candidates:
        if (candidate / "data").exists() and (candidate / "torch").exists():
            return candidate
    return Path.cwd()


def default_input_path(root: Path) -> Path:
    for filename in ["builty_all.parquet", "all_builty.parquet"]:
        path = root / "data" / "raw" / filename
        if path.exists():
            return path
    return root / "data" / "raw" / "builty_all.parquet"


def build_source_select(cols: set[str], min_year: int | None, residential_only: bool) -> tuple[str, str]:
    derived = []

    if "PERMIT_DATE" not in cols:
        date_candidates = [c for c in ["DATE_ISSUED", "DATE_SUBMITTED", "DATE_FINALED"] if c in cols]
        if not date_candidates:
            raise KeyError("Need PERMIT_DATE or one of DATE_ISSUED/DATE_SUBMITTED/DATE_FINALED.")
        derived.append(f"coalesce({', '.join(date_candidates)}) AS PERMIT_DATE")

    if "STREET" not in cols:
        if "STREET_ADDRESS" not in cols:
            raise KeyError("Need STREET or STREET_ADDRESS for ATTOM address matching.")
        derived.append("STREET_ADDRESS AS STREET")

    if "COUNTY_FIPS" not in cols:
        if "FIPS_COUNTY" not in cols:
            raise KeyError("Need COUNTY_FIPS or FIPS_COUNTY.")
        derived.append("FIPS_COUNTY AS COUNTY_FIPS")

    if "JOB_VALUE" not in cols and "PROJECT_VALUE" in cols:
        derived.append("PROJECT_VALUE AS JOB_VALUE")

    if "RESIDENTIAL" not in cols and "PROPERTY_TYPE" in cols:
        derived.append("CASE WHEN lower(PROPERTY_TYPE) = 'residential' THEN 1 ELSE 0 END AS RESIDENTIAL")

    select_items = ["*"] + derived
    source_select = ",\n        ".join(select_items)

    filters = ["flood_elev_final = 1"]
    if min_year is not None:
        filters.append(f"permit_year >= {int(min_year)}")
    if residential_only:
        if "RESIDENTIAL" not in cols and "PROPERTY_TYPE" not in cols:
            raise KeyError("Residential-only filter needs RESIDENTIAL or PROPERTY_TYPE.")
        filters.append("RESIDENTIAL = 1")

    return source_select, " AND ".join(filters)


def main() -> None:
    root = project_root()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=str(default_input_path(root)),
        help="Raw Builty parquet, usually builty_all.parquet.",
    )
    parser.add_argument(
        "--out",
        default=str(root / "data" / "clean" / "all_elevation.parquet"),
        help="Strict filtered output parquet.",
    )
    parser.add_argument("--diagnostics", default=None, help="Optional CSV with row counts.")
    parser.add_argument("--tmp", default="/tmp", help="DuckDB temp directory.")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="32GB")
    parser.add_argument("--min-year", type=int, default=None)
    parser.add_argument("--keep-nonresidential", action="store_true")
    parser.add_argument(
        "--require-flood-context",
        action="store_true",
        help="Require flood/FEMA/BFE/freeboard/etc. context in addition to the strict home-elevation text hit.",
    )
    parser.add_argument("--where", default=None, help="Optional SQL WHERE clause for testing/subsets.")
    parser.add_argument("--limit", type=int, default=None, help="Optional testing limit.")
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"SET memory_limit={quote_sql(args.memory)}")
    con.execute("SET preserve_insertion_order=false")

    input_path = str(Path(args.input))
    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cols = existing_columns(con, input_path)
    source_select, final_where = build_source_select(
        cols=cols,
        min_year=args.min_year,
        residential_only=not args.keep_nonresidential,
    )
    context_filter_sql = "AND flood_adaptation_context = 1" if args.require_flood_context else ""

    path_sql = quote_sql(input_path)
    source_relation = f"read_parquet({path_sql})"
    if args.where:
        source_relation = f"(SELECT * FROM {source_relation} WHERE {args.where})"
    if args.limit is not None:
        source_relation = f"(SELECT * FROM {source_relation} LIMIT {int(args.limit)})"

    desc_clean = (
        "regexp_replace("
        "regexp_replace(lower(coalesce(DESCRIPTION, '')), '[/()\\.,-]', ' ', 'g'), "
        "'\\\\s+', ' ', 'g')"
    )
    strict_sql = regex_any_sql("desc_l", STRICT_PATTERNS)
    context_sql = regex_any_sql("desc_l", FLOOD_CONTEXT_PATTERNS)
    falsepos_sql = regex_any_sql("desc_l", FALSE_POSITIVE_PATTERNS)

    query_body = f"""
        WITH source AS (
            SELECT
                {source_select}
            FROM {source_relation}
        ),
        prepared AS (
            SELECT
                *,
                TRY_CAST(substr(CAST(PERMIT_DATE AS VARCHAR), 1, 4) AS INTEGER) AS permit_year,
                {desc_clean} AS desc_l
            FROM source
        ),
        flagged AS (
            SELECT
                *,
                CASE WHEN ({strict_sql}) THEN 1 ELSE 0 END AS flood_elev_strict,
                CASE WHEN ({context_sql}) THEN 1 ELSE 0 END AS flood_adaptation_context,
                CASE WHEN ({falsepos_sql}) THEN 1 ELSE 0 END AS flood_elev_falsepos
            FROM prepared
        ),
        final AS (
            SELECT
                *,
                CASE
                    WHEN flood_elev_strict = 1
                        {context_filter_sql}
                        AND flood_elev_falsepos = 0 THEN 1
                    ELSE 0
                END AS flood_elev_final
            FROM flagged
        )
        SELECT *
        FROM final
    """

    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print("Writing strict Builty elevation parquet...")
    con.execute(
        f"""
        COPY (
            {query_body}
            WHERE {final_where}
        ) TO {quote_sql(str(output_path))} (FORMAT PARQUET)
        """
    )

    diagnostics_path = Path(args.diagnostics) if args.diagnostics else output_path.with_name(
        f"{output_path.stem}_diagnostics.csv"
    )
    stats = con.execute(
        f"""
        WITH all_rows AS (
            {query_body}
        )
        SELECT 'input_rows' AS metric, count(*)::DOUBLE AS value FROM all_rows
        UNION ALL
        SELECT 'strict_text_rows', sum(flood_elev_strict)::DOUBLE FROM all_rows
        UNION ALL
        SELECT 'flood_context_rows', sum(flood_adaptation_context)::DOUBLE FROM all_rows
        UNION ALL
        SELECT 'false_positive_rows', sum(flood_elev_falsepos)::DOUBLE FROM all_rows
        UNION ALL
        SELECT 'final_text_rows', sum(flood_elev_final)::DOUBLE FROM all_rows
        UNION ALL
        SELECT 'output_rows', count(*)::DOUBLE FROM read_parquet({quote_sql(str(output_path))})
        """
    ).df()
    stats.to_csv(diagnostics_path, index=False)

    print(stats.to_string(index=False))
    print(f"Diagnostics: {diagnostics_path}")


if __name__ == "__main__":
    main()

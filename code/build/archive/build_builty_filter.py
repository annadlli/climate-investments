"""
Build Builty residential flood/home-elevation permit filters.

SUPERSEDED 2026-07-16 -- do not revive. Replaced by clean/extract_builty.py (the
per-state duckdb extract) + clean/clean_builty.do (the elevation filter, in Stata
where the judgement calls are legible). Its output, clean/all_builty_elevations.dta,
is misnamed: it holds the LOOSE candidate set including false positives (it is full
of elevators), not finished elevations.

Authors: Anna Li and Vendela Norman
Date: 2026-06-17

Description:
    Reads raw Builty permits,
    keeps residential records whose lowercased DESCRIPTION matches broad
    flood/elevation candidate terms, and flags false positives with exception
    overrides for genuine existing-house elevation work. Drops false positives.

Notes / Sources:
    Input defaults to {data}/raw/builty_all.parquet. Output defaults to
    {data}/build/all_builty_elevations.parquet and excludes false positives. Use
    --keep-false-positives to reproduce the loose review output.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


STRICT_PATTERNS = [
    r"elevat",
    r"rais(e|ed|ing)",
    r"lift(ed|ing)",
    r"jack(ed|ing) up",
    r"flood",
    r"fema",
    r"bfe",
    r"base flood elevation",
    r"finished floor elevation",
    r"ffe",
    r"freeboard",
    r"nfip",
    r"icc",
    r"floodplain",
    r"flood plain",
    r"flood zone",
    r"hazard mitigation",
    r"substantial damage",
    r"substantially damaged",
    r"substantial improvement",
    r"substantially improved",
    r"storm surge",
    r"out of (the )?floodplain",
]

FALSE_POSITIVE_PATTERNS = [
    r"sewer|sanitary|septic",
    r"electrical permit",
    r"rewire",
    r"smoke detectors?",
    r"plumbing permit",
    r"mechanical permit",
    r"inspection",
    r"meter",
    r"pad",
    r"boat",
    r"fence",
    r"transfer",
    r"gas",
    r"a/c",
    r"new single family",
    r"new home",
    r"roof",
    r"window|windows",
    r"kitchen",
    r"bath",
    r"bedroom",
    r"outlet",
    r"wall",
    r"interior",
    r"demolish|demolition|demo",
    r"accessory",
    r"carport",
    r"porch",
    r"res - plumbing",
    r"res - mechanical",
    r"sheds/garage/",
    r"elevator",
    r"pool",
    r"mobile home",
    r"townhouse|town house",
    r"duplex",
    r"triplex",
    r"condo",
    r"guest house",
    r"solar|photovoltaic",
    r"signage|wall sign|channel letters|illuminated.{0,30}(sign|letter|cabinet)",
    r"tree removal|tree pruning|prun(e|ing)|canopy|oak tree|laurel oak",
    r"rais(e|ed|ing).{0,40}roof|roof.{0,40}rais(e|ed|ing)",
    r"rais(e|ed|ing).{0,40}ceiling|ceiling.{0,40}rais(e|ed|ing)",
    r"raise (the )?roof",
    r"raise (the )?bar",
    r"shower faucet|shower plumbing",
    r"generator",
    r"water heater",
    r"flood ?lights?",
    r"fire sprinklers?",
    r"suppression",
    r"fence permit|privacy fence|rock ?wall",
    r"plan elevations?",
    r"drawing",
    r"(north|side|front|rear|south|east|west|left|right).{0,30}elevations?",
    r"elevations?.{0,30}(north|side|front|rear|south|east|west|left|right)",
    r"elevation (drawing|plan|view|sheet|detail)",
    r"raised ranch",
    r"(raised|raise|raising).{0,40}curb|curb.{0,40}(raised|raise|raising)",
    r"patio",
]

FALSE_POSITIVE_EXCEPTION_PATTERNS = [
    r"raising for flood requirements",
    r"permit to elevate",
    r"house raising",
    r"fema house raising",
    r"raising existing house",
    r"elevate house",
    r"temp service.{0,80}elevate house",
    r"construction during house raising",
    r"power disconnected.{0,80}house raising",
    r"raised an existing mh",
    r"final elevation.{0,80}original slab.{0,80}raised portion",
    r"raise house",
    r"raise.{0,40}side of house",
    r"raise.{0,40}side of single family dwelling",
    r"raising the house",
    r"raising height of the building",
    r"lifting and moving house",
    r"elevating existing sfd",
    r"house.{0,40}(raised|lifted)",
    r"home.{0,40}(raised|lifted)",
    r"house is to be raised",
    r"house can be raised",
    r"house was raised",
    r"home was lifted",
    r"lifting home to level",
    r"raise home above flood level",
    r"house to be lifted",
    r"house lifting",
    r"raised house",
    r"raised home",
    r"house being moved",
    r"existing home to be moved",
    r"house raise",
    r"lifted up house",
    r"raise up existing single family house",
    r"raise sfd",
    r"raise exist\\.? sfd",
    r"reconnect.{0,80}(plumbing|sewer|water|gas).{0,80}(raised|lifted|raise|lifting)",
    r"(plumbing|sewer|water|gas).{0,80}(reconnect|disconnect|cap).{0,80}(raise|raised|lift|lifted)",
    r"(disconnect|cap|reconnect).{0,80}(plumbing|sewer|water|gas).{0,80}(raise|raised|lift|lifted)",
    r"pipe dwv and water for house that was raised",
    r"water & drain work for raised home",
    r"raise existing structure",
    r"structure has been raised",
    r"structure will be raised",
    r"helical piles.{0,120}lift structure",
    r"pier and beam foundation.{0,120}south side.{0,40}raised",
    r"house raised between [0-9]+[-–][0-9]+ inches",
    r"foundation raise|raise foundation",
    r"foundation raise.{0,80}carport|carport.{0,80}foundation raise",
    r"foundation.{0,80}raise|raise.{0,80}foundation",
    r"raise.{0,80}install new foundation for existing",
    r"lift house",
    r"lifting of existing house",
    r"lifting the existing house",
    r"house.{0,80}will be lifted",
    r"moved house",
    r"raise existing s ?f ?r",
    r"raise existing single story (residence|home)",
    r"lift house above hightide",
    r"raise house off (of )?the foundation",
    r"raise off (of )?the foundation.{0,80}move house",
    r"stabilizing foundation.{0,80}lifting",
    r"set the house back down to existing elevation",
    r"elevat(e|ing).{0,40}existing s ?f res",
    r"rais(e|ing).{0,80}house.{0,120}windows?|lift(ing)? house.{0,120}windows?",
    r"elevat(e|ing).{0,40}(and remodel )?exi(st|sit)ing structure.{0,120}windows?",
    r"remodel & repair existing house.{0,120}raise house two feet|raise house two feet",
    r"elevation of living space",
]


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def regex_any_sql(column: str, patterns: list[str]) -> str:
    if not patterns:
        return "FALSE"
    return " OR\n                ".join(
        f"regexp_matches({column}, {quote_sql(pattern)})" for pattern in patterns
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        required=True,
        help="Dropbox data root. Input/output paths derive from this root.",
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Optional raw Builty parquet. Defaults to {data}/raw/builty_all.parquet.",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output parquet. Defaults to {data}/build/all_builty_elevations.parquet.",
    )
    parser.add_argument(
        "--diagnostics",
        default=None,
        help="Optional diagnostics CSV. Defaults next to output parquet.",
    )
    parser.add_argument(
        "--keep-false-positives",
        action="store_true",
        help="Write all candidate_flag == 1 rows and keep falsepos_flag for review.",
    )
    parser.add_argument("--tmp", default="/tmp", help="DuckDB temporary directory.")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="32GB")
    parser.add_argument("--where", default=None, help="Optional SQL WHERE clause for testing.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for testing.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    input_path = Path(args.input) if args.input else data / "raw" / "builty_all.parquet"
    output_path = (
        Path(args.out) if args.out else data / "build" / "all_builty_elevations.parquet"
    )
    diagnostics_path = (
        Path(args.diagnostics)
        if args.diagnostics
        else output_path.with_name(f"{output_path.stem}_diagnostics.csv")
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
    con.execute(f"SET threads={int(args.threads)}")
    con.execute(f"SET memory_limit={quote_sql(args.memory)}")
    con.execute("SET preserve_insertion_order=false")

    source_relation = f"read_parquet({quote_sql(str(input_path))})"
    if args.where:
        source_relation = f"(SELECT * FROM {source_relation} WHERE {args.where})"
    if args.limit is not None:
        source_relation = f"(SELECT * FROM {source_relation} LIMIT {int(args.limit)})"

    strict_sql = regex_any_sql("desc_l", STRICT_PATTERNS)
    false_sql = regex_any_sql("desc_l", FALSE_POSITIVE_PATTERNS)
    exception_sql = regex_any_sql("desc_l", FALSE_POSITIVE_EXCEPTION_PATTERNS)
    falsepos_sql = f"(({false_sql}) AND NOT ({exception_sql}))"
    output_where = "candidate_flag = 1" if args.keep_false_positives else "final_flag = 1"

    candidate_query = f"""
        WITH prepared AS (
            SELECT
                *,
                lower(coalesce(DESCRIPTION, '')) AS desc_l
            FROM {source_relation}
            WHERE PROPERTY_TYPE = 'Residential'
        ),
        candidate_flagged AS (
            SELECT
                *,
                CASE WHEN ({strict_sql}) THEN 1 ELSE 0 END AS candidate_flag
            FROM prepared
        ),
        candidates AS (
            SELECT *
            FROM candidate_flagged
            WHERE candidate_flag = 1
        ),
        flagged AS (
            SELECT
                *,
                CASE WHEN ({falsepos_sql}) THEN 1 ELSE 0 END AS falsepos_flag
            FROM candidates
        ),
        final AS (
            SELECT
                *,
                CASE
                    WHEN candidate_flag = 1 AND falsepos_flag = 0 THEN 1
                    ELSE 0
                END AS final_flag
            FROM flagged
        )
        SELECT *
        FROM final
    """

    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print(f"Mode:   {'loose review' if args.keep_false_positives else 'final filtered'}")
    print("Writing filtered parquet...", flush=True)
    con.execute(
        f"""
        COPY (
            {candidate_query}
            WHERE {output_where}
        ) TO {quote_sql(str(output_path))} (FORMAT PARQUET)
        """
    )

    print("Building diagnostics...", flush=True)
    residential_rows = con.execute(
        f"""
        SELECT count(*)::DOUBLE
        FROM {source_relation}
        WHERE PROPERTY_TYPE = 'Residential'
        """
    ).fetchone()[0]
    summary = con.execute(
        f"""
        WITH candidate_rows AS (
            {candidate_query}
        )
        SELECT 'residential_rows' AS metric, {residential_rows}::DOUBLE AS value
        UNION ALL
        SELECT 'candidate_rows', count(*)::DOUBLE FROM candidate_rows
        UNION ALL
        SELECT 'candidate_false_positive_rows', sum(falsepos_flag)::DOUBLE FROM candidate_rows
        UNION ALL
        SELECT 'candidate_final_rows', sum(final_flag)::DOUBLE FROM candidate_rows
        UNION ALL
        SELECT 'output_rows', count(*)::DOUBLE FROM read_parquet({quote_sql(str(output_path))})
        """
    ).df()
    summary.to_csv(diagnostics_path, index=False)

    falsepos_summary = con.execute(
        f"""
        SELECT
            falsepos_flag,
            count(*) AS n,
            round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
        FROM read_parquet({quote_sql(str(output_path))})
        GROUP BY falsepos_flag
        ORDER BY falsepos_flag
        """
    ).df()

    print(summary.to_string(index=False))
    print("\nOutput rows by falsepos_flag:")
    print(falsepos_summary.to_string(index=False))
    print(f"\nDiagnostics: {diagnostics_path}")


if __name__ == "__main__":
    main()

"""
Describe YEARBUILT / YEARBUILTEFFECTIVE coverage in ATTOM parquet files.

This is a diagnostic for the Wagner construction-tier cells. It reports, by
state, how often ATTOM has usable construction-year information after applying
the same basic filters used by build_attom_wagner_cells.py.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


DEFAULT_STATES = [
    "al", "ct", "de", "fl", "ga", "la", "ma", "md", "me", "ms",
    "nc", "nh", "nj", "ny", "pa", "ri", "sc", "tx", "va", "vt",
]


def quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print and save ATTOM YEARBUILT coverage descriptives."
    )
    p.add_argument("--data", required=True, help="Data root containing state parquet dirs.")
    p.add_argument("--output", default=None, help="CSV output path.")
    p.add_argument("--tmp", default="/tmp")
    p.add_argument("--threads", default=4, type=int)
    p.add_argument("--memory", default="32GB")
    p.add_argument("--max-temp", default="800GB")
    p.add_argument("--states", nargs="*", default=DEFAULT_STATES)
    return p.parse_args()


def resolve_parquet(data: Path, state: str) -> Path | None:
    candidates = [
        data / state / f"attom_{state}.parquet",
        data / "raw" / "attom" / f"attom_{state}.parquet",
    ]
    return next((p for p in candidates if p.exists()), None)


def summarize_state(con: duckdb.DuckDBPyConnection, path: Path, state: str) -> dict[str, object]:
    con.execute("DROP VIEW IF EXISTS attom_raw")
    con.execute(f"CREATE VIEW attom_raw AS SELECT * FROM read_parquet({quote(str(path))})")

    row = con.execute("""
        WITH typed AS (
            SELECT
                TRY_CAST(YEARBUILT AS INTEGER) AS yearbuilt,
                TRY_CAST(YEARBUILTEFFECTIVE AS INTEGER) AS yearbuilteffective,
                TRY_CAST(TAXYEARASSESSED AS INTEGER) AS tax_year,
                TRY_CAST(TAXMARKETVALUETOTAL AS DOUBLE) AS market_total
            FROM attom_raw
        ),
        flagged AS (
            SELECT
                *,
                yearbuilt BETWEEN 1700 AND 2027 AS yearbuilt_valid,
                yearbuilteffective BETWEEN 1700 AND 2027 AS yearbuilteffective_valid,
                COALESCE(
                    CASE WHEN yearbuilt BETWEEN 1700 AND 2027 THEN yearbuilt END,
                    CASE WHEN yearbuilteffective BETWEEN 1700 AND 2027 THEN yearbuilteffective END
                ) AS construction_year,
                tax_year BETWEEN 1980 AND 2035 AND market_total > 0 AS build_filter
            FROM typed
        )
        SELECT
            COUNT(*) AS n_rows,
            SUM(yearbuilt IS NOT NULL) AS n_yearbuilt_present,
            SUM(yearbuilt IS NULL) AS n_yearbuilt_missing,
            SUM(yearbuilt_valid) AS n_yearbuilt_valid,
            SUM(yearbuilteffective IS NOT NULL) AS n_yearbuilteffective_present,
            SUM(yearbuilteffective IS NULL) AS n_yearbuilteffective_missing,
            SUM(yearbuilteffective_valid) AS n_yearbuilteffective_valid,
            SUM(construction_year IS NOT NULL) AS n_construction_year_valid,
            SUM(build_filter) AS n_build_filter_rows,
            SUM(build_filter AND yearbuilt IS NOT NULL) AS n_build_filter_yearbuilt_present,
            SUM(build_filter AND yearbuilt_valid) AS n_build_filter_yearbuilt_valid,
            SUM(build_filter AND yearbuilteffective IS NOT NULL) AS n_build_filter_yearbuilteffective_present,
            SUM(build_filter AND yearbuilteffective_valid) AS n_build_filter_yearbuilteffective_valid,
            SUM(build_filter AND construction_year IS NOT NULL) AS n_build_filter_construction_year_valid,
            MIN(construction_year) AS min_construction_year,
            MAX(construction_year) AS max_construction_year,
            COUNT(DISTINCT construction_year) AS n_distinct_construction_year
        FROM flagged
    """).df().iloc[0].to_dict()

    row["state"] = state
    row["parquet"] = str(path)
    return row


def add_rates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rate_pairs = [
        ("pct_yearbuilt_present", "n_yearbuilt_present", "n_rows"),
        ("pct_yearbuilt_valid", "n_yearbuilt_valid", "n_rows"),
        ("pct_yearbuilteffective_present", "n_yearbuilteffective_present", "n_rows"),
        ("pct_yearbuilteffective_valid", "n_yearbuilteffective_valid", "n_rows"),
        ("pct_construction_year_valid", "n_construction_year_valid", "n_rows"),
        (
            "pct_build_filter_construction_year_valid",
            "n_build_filter_construction_year_valid",
            "n_build_filter_rows",
        ),
    ]
    for out_col, numerator, denominator in rate_pairs:
        df[out_col] = df[numerator] / df[denominator].where(df[denominator] != 0)
    return df


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    output = Path(args.output) if args.output else data / "build" / "attom_yearbuilt_descriptives.csv"
    output.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote(args.tmp)}")
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET max_temp_directory_size={quote(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    rows: list[dict[str, object]] = []
    for state in [s.lower() for s in args.states]:
        path = resolve_parquet(data, state)
        if path is None:
            print(f"Skipping {state}: no ATTOM parquet found")
            continue
        print(f"Summarizing {state}: {path}")
        rows.append(summarize_state(con, path, state))

    con.close()
    if not rows:
        raise RuntimeError("No ATTOM parquet files were summarized.")

    df = add_rates(pd.DataFrame(rows))
    first = ["state", "parquet"]
    other = [c for c in df.columns if c not in first]
    df = df[first + other].sort_values("state")
    df.to_csv(output, index=False)

    display_cols = [
        "state",
        "n_rows",
        "n_build_filter_rows",
        "pct_yearbuilt_present",
        "pct_yearbuilt_valid",
        "pct_yearbuilteffective_present",
        "pct_yearbuilteffective_valid",
        "pct_build_filter_construction_year_valid",
        "n_distinct_construction_year",
        "min_construction_year",
        "max_construction_year",
    ]
    print("")
    print(df[display_cols].to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("")
    print(f"Saved: {output}")


if __name__ == "__main__":
    main()

"""
Author: Anna Li
Date: 2024-06-25
Description: Compile raw ATTOM Dewey batch downloads into per-state parquet files.
The Dewey pulls can contain multiple states in one endpoint. This script reads
the same manifest used for import_dewey.py, filters each batch to the states
listed in its `states` column, and writes one ATTOM parquet per state.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
import duckdb

#state abbreviations to FIPS codes mapping
STATE_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MS": "28",
    "NH": "33",
    "NJ": "34",
    "NY": "36",
    "NC": "37",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "VT": "50",
}


def quote_sql(value: str) -> str:
    # Escape SQL string literals for DuckDB queries
    return "'" + value.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()


def read_manifest(path: Path) -> list[dict[str, str]]:
    # Read the manifest file as rows; this contains the list of batches and their associated states
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def parquet_files(batch_dir: Path) -> list[str]:
    # List all parquet files in a batch folder
    return sorted(str(path) for path in batch_dir.glob("*.snappy.parquet"))


def describe_columns(con: duckdb.DuckDBPyConnection, files: list[str]) -> set[str]:
    # Inspect the parquet and return the available columns
    files_sql = "[" + ", ".join(quote_sql(path) for path in files) + "]"
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet({files_sql}, union_by_name=true) LIMIT 1").fetchall()
    return {row[0].upper() for row in desc}


def state_filter_sql(state: str, columns: set[str]) -> str:
    # Build a state filter from any available ATTOM state columns
    fips = STATE_FIPS[state]
    filters: list[str] = []
    if "SITUSSTATECODE" in columns: #2 - letter state abbreviation
        filters.append(f"upper(trim(cast(SITUSSTATECODE AS varchar))) = {quote_sql(state)}")

    if "SITUSSTATECOUNTYFIPS" in columns:
        # Extract the numeric county FIPS, left-pad it to five digits, and
        # compare its first two digits (the state FIPS) with the target state.
        filters.append(
            "substr(lpad(regexp_extract(trim(cast(SITUSSTATECOUNTYFIPS AS varchar)), "
            f"'(\\d+)', 1), 5, '0'), 1, 2) = {quote_sql(fips)}"
        )
    if filters:
        return "(" + " OR ".join(filters) + ")"
    # Fallback to an always-false filter if no state column exists
    return "FALSE"


def main() -> None:
    # Setup inputs and database connection
    args = parse_args()
    data = Path(args.data)
    run_dir = data / "raw" / "dewey" / args.run_id
    manifest = read_manifest(Path(args.manifest))

    con = duckdb.connect()

    state_to_files: dict[str, list[str]] = {}
    for row in manifest:
        states = [state.strip().upper() for state in row["states"].replace(",", " ").split() if state.strip()]
        batch_dir = run_dir / row["folder"]
        files = parquet_files(batch_dir)

        # Add batch files to each declared state
        for state in states:
            if state in STATE_FIPS:
                state_to_files.setdefault(state, []).extend(files)

    # Process batch parquet files state-by-state and write output parquet files
    for state, files in sorted(state_to_files.items()):
        files = sorted(set(files))
        columns = describe_columns(con, files)
        where = state_filter_sql(state, columns)
        files_sql = "[" + ", ".join(quote_sql(path) for path in files) + "]"
        state_lower = state.lower()
        out_dir = data / state_lower
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"attom_{state_lower}.parquet"

        print(f"\nWriting {state}: {out_path}")
        print(f"  Source files: {len(files)}")
        print(f"  State filter: {where}")

        # Copy rows for this state into a single parquet file
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet({files_sql}, union_by_name=true)
                WHERE {where}
            ) TO {quote_sql(str(out_path))} (FORMAT PARQUET, COMPRESSION SNAPPY)
            """
        )
        n = con.execute(f"SELECT count(*) FROM read_parquet({quote_sql(str(out_path))})").fetchone()[0]
        print(f"  Rows: {n:,}")

    print("\nDone.")


if __name__ == "__main__":
    main()

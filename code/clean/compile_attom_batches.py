"""
Compile raw ATTOM Dewey batch downloads into per-state parquet files.

The Dewey pulls can contain multiple states in one endpoint. This script reads
the same manifest used for import_dewey.py, filters each batch to the states
listed in its `states` column, and writes one ATTOM parquet per state:

    {data}/{state}/attom_{state}.parquet
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import duckdb


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
    return "'" + value.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="Data root, e.g. /scratch/adl9602/tx/data")
    parser.add_argument("--manifest", required=True, help="CSV manifest used for Dewey import.")
    parser.add_argument(
        "--run-id",
        required=True,
        help="Dewey run folder name, e.g. run_20260625_115712.",
    )
    parser.add_argument("--tmp", default="/scratch/adl9602/tx/tmp", help="DuckDB temp directory.")
    parser.add_argument("--threads", default=4, type=int)
    parser.add_argument("--memory", default="64GB")
    return parser.parse_args()


def read_manifest(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {"name", "folder", "states"}
    if not rows:
        raise ValueError(f"Manifest {path} has no rows.")
    missing = required - set(rows[0])
    if missing:
        raise ValueError(f"Manifest {path} is missing column(s): {', '.join(sorted(missing))}")
    return rows


def parquet_files(batch_dir: Path) -> list[str]:
    return sorted(str(path) for path in batch_dir.glob("*.snappy.parquet"))


def valid_parquet(path: str, con: duckdb.DuckDBPyConnection | None = None) -> bool:
    try:
        with open(path, "rb") as handle:
            header = handle.read(4)
            handle.seek(-4, 2)
            footer = handle.read(4)
        if header != b"PAR1" or footer != b"PAR1":
            return False
        if con is not None:
            con.execute(f"SELECT count(*) FROM read_parquet({quote_sql(path)})").fetchone()
        return True
    except (OSError, duckdb.Error):
        return False


def split_valid_files(files: list[str], con: duckdb.DuckDBPyConnection) -> tuple[list[str], list[str]]:
    good: list[str] = []
    bad: list[str] = []
    for path in files:
        if valid_parquet(path, con):
            good.append(path)
        else:
            bad.append(path)
    return good, bad


def describe_columns(con: duckdb.DuckDBPyConnection, files: list[str]) -> set[str]:
    files_sql = "[" + ", ".join(quote_sql(path) for path in files) + "]"
    desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet({files_sql}, union_by_name=true) LIMIT 1").fetchall()
    return {row[0].upper() for row in desc}


def state_filter_sql(state: str, columns: set[str]) -> str:
    fips = STATE_FIPS[state]
    filters: list[str] = []
    for col in [
        "SITUSSTATECODE",
        "SITUSSTATE",
        "PROPERTYADDRESSSTATE",
        "PROPERTYADDRESSSTATECODE",
        "MAILADDRESSSTATE",
        "MAILADDRESSSTATECODE",
        "OWNERADDRESSSTATE",
        "OWNERADDRESSSTATECODE",
        "STATE",
    ]:
        if col in columns:
            filters.append(f"upper(trim(cast({col} AS varchar))) = {quote_sql(state)}")
    if "SITUSSTATECOUNTYFIPS" in columns:
        filters.append(
            "substr(lpad(regexp_extract(trim(cast(SITUSSTATECOUNTYFIPS AS varchar)), "
            f"'(\\\\d+)', 1), 5, '0'), 1, 2) = {quote_sql(fips)}"
        )
    if filters:
        return "(" + " OR ".join(filters) + ")"
    raise ValueError("Cannot find a usable state column in ATTOM batch.")


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    run_dir = data / "raw" / "dewey" / args.run_id
    manifest = read_manifest(Path(args.manifest))

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
    con.execute(f"SET memory_limit={quote_sql(args.memory)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    state_to_files: dict[str, list[str]] = {}
    corrupt_files: list[str] = []
    for row in manifest:
        states = [state.strip().upper() for state in row["states"].replace(",", " ").split() if state.strip()]
        if not states:
            print(f"Skipping {row['name']}: no states listed.")
            continue
        batch_dir = run_dir / row["folder"]
        files = parquet_files(batch_dir)
        if not files:
            print(f"Skipping {row['name']}: no parquet files found in {batch_dir}.")
            continue
        files, bad = split_valid_files(files, con)
        if bad:
            corrupt_files.extend(bad)
            print(f"Found {len(bad)} corrupt parquet file(s) in {batch_dir}.")
        if not files:
            print(f"Skipping {row['name']}: no valid parquet files found in {batch_dir}.")
            continue
        for state in states:
            if state not in STATE_FIPS:
                raise ValueError(f"Unknown state abbreviation in manifest: {state}")
            state_to_files.setdefault(state, []).extend(files)

    if corrupt_files:
        corrupt_list_path = run_dir / "corrupt_parquet_files.txt"
        corrupt_list_path.write_text("\n".join(sorted(corrupt_files)) + "\n")
        print("\nCorrupt or incomplete parquet files found:")
        for path in sorted(corrupt_files):
            print(f"  {path}")
        print(f"\nWrote corrupt-file list: {corrupt_list_path}")
        raise RuntimeError(
            "Delete the corrupt files listed above (or use corrupt_parquet_files.txt), rerun import_dewey.py with "
            "--run-id and --skip-exists to redownload only missing files, then rerun this compiler."
        )

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
        if n == 0:
            raise RuntimeError(
                f"{state} output has zero rows. Check the batch/state mapping or ATTOM state columns."
            )

    print("\nDone.")


if __name__ == "__main__":
    main()

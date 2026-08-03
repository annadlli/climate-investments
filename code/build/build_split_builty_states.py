"""
Build per-state Builty elevation permit parquet files.

Authors: Anna Li and Vendela Norman
Date: 2026-06-17

Description:
    Splits the filtered Builty elevation permit file into one parquet per state.

Notes / Sources:
    Input defaults to {data}/build/all_builty_elevations.parquet, which is produced by
    build_builty_filter.py. Outputs default to
    {data}/build/{state}_flood_elevation_strict.parquet.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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
        help="Optional filtered Builty parquet. Defaults to {data}/build/all_builty_elevations.parquet.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Optional output root. Defaults to {data}/build.",
    )
    parser.add_argument("--states", nargs="+", default=["TX", "VA"])
    parser.add_argument(
        "--filename-pattern",
        default="{state_lower}_flood_elevation_strict.parquet",
        help="Available fields: {state}, {state_lower}.",
    )
    parser.add_argument("--tmp", default="/tmp", help="DuckDB temporary directory.")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="32GB")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    input_path = (
        Path(args.input) if args.input else data / "build" / "all_builty_elevations.parquet"
    )
    out_dir = Path(args.out_dir) if args.out_dir else data / "build"
    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
    con.execute(f"SET threads={int(args.threads)}")
    con.execute(f"SET memory_limit={quote_sql(args.memory)}")
    con.execute("SET preserve_insertion_order=false")

    input_sql = quote_sql(str(input_path))
    for state in [state.upper() for state in args.states]:
        state_lower = state.lower()
        filename = args.filename_pattern.format(state=state, state_lower=state_lower)
        out_path = out_dir / filename
        out_path.parent.mkdir(parents=True, exist_ok=True)

        n_rows = con.execute(
            f"""
            SELECT count(*)
            FROM read_parquet({input_sql})
            WHERE upper(STATE) = {quote_sql(state)}
            """
        ).fetchone()[0]

        print(f"Writing {state} ({n_rows:,} rows): {out_path}")
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet({input_sql})
                WHERE upper(STATE) = {quote_sql(state)}
            ) TO {quote_sql(str(out_path))} (FORMAT PARQUET)
            """
        )

    con.close()


if __name__ == "__main__":
    main()

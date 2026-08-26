"""
Authors: Anna Li
Date: 2026-08-19

Convert a parquet file to Stata .dta format.

This is a small utility for one-off inspection/conversion jobs, for example:

    python code/build/parquet_to_dta.py \
        --input /path/to/vt.parquet \
        --output data/build/vt.dta

Use --columns for large ATTOM files so the resulting .dta stays manageable.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def quote_sql(value: str) -> str:
    # escape a path or literal for DuckDB SQL
    return "'" + value.replace("'", "''") + "'"


def quote_ident(value: str) -> str:
    # escape a column name; ATTOM headers contain characters DuckDB needs quoted
    return '"' + value.replace('"', '""') + '"'


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert parquet to Stata .dta.")
    p.add_argument("--input", required=True, help="Input parquet path.")
    p.add_argument("--output", required=True, help="Output .dta path.")
    p.add_argument(
        "--columns",
        nargs="*",
        help="Optional column list to keep. Defaults to all columns.",
    )
    p.add_argument(
        "--where",
        default=None,
        help="Optional SQL WHERE condition, written using parquet column names.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for a smaller diagnostic extract.",
    )
    p.add_argument(
        "--writer",
        choices=["pyreadstat", "pandas"],
        default="pyreadstat",
        help="DTA writer backend. pyreadstat is usually faster for wide files.",
    )
    p.add_argument(
        "--chunk-rows",
        type=int,
        default=None,
        help=(
            "Write multiple .dta files with this many rows each. "
            "Output path becomes *_part001.dta, *_part002.dta, etc."
        ),
    )
    return p.parse_args()


def stata_safe_strings(df: pd.DataFrame) -> pd.DataFrame:
    # Stata has no missing-string type, so a null becomes "". Missing text and
    # empty text are therefore indistinguishable downstream; keys that matter
    # are padded upstream in the matching scripts, not here.
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].fillna("").astype(str)
    return df


def stata_safe_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Convert nullable pandas numerics to dtypes supported by DTA writers."""
    # DTA writers reject pandas nullable extension dtypes (Int64, boolean).
    # Demote to float64 when nulls are present so the missing values survive,
    # and to a plain int otherwise so the column stays compact.
    for col in df.columns:
        series = df[col]
        if pd.api.types.is_bool_dtype(series.dtype):
            df[col] = (
                series.astype("float64") if series.isna().any()
                else series.astype(np.int8)
            )
        elif (
            pd.api.types.is_integer_dtype(series.dtype)
            and pd.api.types.is_extension_array_dtype(series.dtype)
        ):
            df[col] = (
                series.astype("float64") if series.isna().any()
                else series.astype("int64")
            )
    return df


def write_dta(df: pd.DataFrame, out_path: Path, writer: str) -> None:
    # fix dtypes and names before handing the frame to a DTA writer
    df = stata_safe_strings(df)
    df = stata_safe_numerics(df)

    rename_map = stata_safe_names(list(df.columns))
    changed = {k: v for k, v in rename_map.items() if k != v}
    if changed:
        print(f"Renaming {len(changed):,} columns to satisfy Stata variable-name limits")
    df = df.rename(columns=rename_map)

    if writer == "pyreadstat":
        import pyreadstat

        pyreadstat.write_dta(df, str(out_path), version=15)
    else:
        df.to_stata(out_path, write_index=False, version=118)


def stata_safe_names(columns: list[str]) -> dict[str, str]:
    """Return Stata-safe variable names, preserving as much ATTOM naming as possible."""
    # Stata caps variable names at 32 characters and allows only [A-Za-z0-9_],
    # starting with a letter or underscore. Truncation can collide, so names are
    # de-duplicated with a numeric suffix; `explicit` pins the one ATTOM column
    # whose automatic truncation would otherwise be unreadable.
    out: dict[str, str] = {}
    used: set[str] = set()
    explicit = {
        "attom_assessed_value_improvements": "attom_assessed_improvements",
    }

    for col in columns:
        base = explicit.get(col, re.sub(r"[^A-Za-z0-9_]", "_", col))
        if not re.match(r"[A-Za-z_]", base):
            base = "v_" + base
        base = base[:32]

        name = base
        i = 1
        while name.lower() in used:
            suffix = f"_{i}"
            name = base[: 32 - len(suffix)] + suffix
            i += 1

        used.add(name.lower())
        out[col] = name

    return out


def main() -> None:
    args = parse_args()
    in_path = Path(args.input).expanduser()
    out_path = Path(args.output).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.columns:
        select_sql = ", ".join(quote_ident(c) for c in args.columns)
    else:
        select_sql = "*"

    con = duckdb.connect()
    print(f"Reading parquet: {in_path}")
    print(f"Writing dta:     {out_path}")

    base_from = f"FROM read_parquet({quote_sql(str(in_path))})"
    where_sql = f" WHERE {args.where}" if args.where else ""

    # Chunked mode exists for files too wide or too long to hold in memory as a
    # single frame; each part is a standalone .dta to be appended in Stata.
    if args.chunk_rows:
        total = con.execute(f"SELECT count(*) {base_from}{where_sql}").fetchone()[0]
        if args.limit:
            total = min(total, args.limit)
        print(f"Writing {total:,} rows in chunks of {args.chunk_rows:,}")

        n_parts = (total + args.chunk_rows - 1) // args.chunk_rows
        for part in range(n_parts):
            offset = part * args.chunk_rows
            limit = min(args.chunk_rows, total - offset)
            part_path = out_path.with_name(
                f"{out_path.stem}_part{part + 1:03d}{out_path.suffix}"
            )
            sql = (
                f"SELECT {select_sql} {base_from}{where_sql} "
                f"LIMIT {limit} OFFSET {offset}"
            )
            print(f"Part {part + 1:03d}/{n_parts:03d}: rows {offset + 1:,}-{offset + limit:,}")
            df = con.execute(sql).df()
            write_dta(df, part_path, args.writer)
    else:
        sql = f"SELECT {select_sql} {base_from}{where_sql}"
        if args.limit:
            sql += f" LIMIT {args.limit}"
        df = con.execute(sql).df()
        print(f"Loaded {len(df):,} rows and {len(df.columns):,} columns")
        write_dta(df, out_path, args.writer)

    print("Done.")


if __name__ == "__main__":
    main()

"""
Combine per-state ATTOM value/Wagner cell files into convenience files.

The per-state build step writes separate files by geography and construction
tier, for example:
    va_attom_value_county_year.dta
    va_attom_wagner_zip_constr_5yr.dta

This script appends those files into one long table with metadata columns that
identify the state, source type, geography level, and tier. It can write either
one all-state file or one combined file per state.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


OUTPUT_COLUMNS = [
    "state",
    "source_type",
    "geo_level",
    "tier",
    "source_file",
    "year",
    "countycode",
    "zip_key",
    "construction_year",
    "construction_5yr",
    "construction_decade",
    "attom_n_properties",
    "attom_mean_market_total",
    "attom_median_market_total",
]

NUMERIC_COLUMNS = [
    "year",
    "construction_year",
    "construction_5yr",
    "construction_decade",
    "attom_n_properties",
    "attom_mean_market_total",
    "attom_median_market_total",
]


FILENAME_RE = re.compile(
    r"^(?P<state>[a-z]{2})_attom_(?P<source_type>value|wagner)_"
    r"(?P<geo_level>county|zip)_(?P<tier>year|constr_year|constr_5yr|constr_decade)\.dta$"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Append ATTOM per-state value/Wagner cell .dta files."
    )
    p.add_argument(
        "--input-dir",
        default="data/build",
        help="Directory containing *_attom_*.dta files.",
    )
    p.add_argument(
        "--output",
        default="data/build/attom_cells_combined.dta",
        help=(
            "Combined output path. Extension may be .dta, .parquet, or .csv. "
            "With --by-state, this is treated as an output directory unless it "
            "has no suffix."
        ),
    )
    p.add_argument(
        "--by-state",
        action="store_true",
        help="Write one combined output file per state.",
    )
    p.add_argument(
        "--output-format",
        choices=["dta", "parquet", "csv"],
        default=None,
        help="Output format for --by-state. Defaults to --output suffix or dta.",
    )
    p.add_argument(
        "--states",
        nargs="*",
        help="Optional state abbreviations to include, e.g. --states tx va.",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help="Fail if any matching file cannot be read.",
    )
    return p.parse_args()


def find_inputs(input_dir: Path, states: set[str] | None) -> list[Path]:
    files: list[Path] = []
    for path in sorted(input_dir.glob("*_attom_*.dta")):
        match = FILENAME_RE.match(path.name)
        if not match:
            continue
        if states and match.group("state") not in states:
            continue
        files.append(path)
    return files


def normalize_file(path: Path) -> pd.DataFrame:
    match = FILENAME_RE.match(path.name)
    if not match:
        raise ValueError(f"Unexpected ATTOM filename shape: {path.name}")

    df = pd.read_stata(path, preserve_dtypes=False)
    info = match.groupdict()

    if "policy_year" in df.columns:
        df = df.rename(columns={"policy_year": "year"})

    df.insert(0, "source_file", path.name)
    df.insert(0, "tier", info["tier"])
    df.insert(0, "geo_level", info["geo_level"])
    df.insert(0, "source_type", info["source_type"])
    df.insert(0, "state", info["state"])

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[OUTPUT_COLUMNS]
    for col in ["state", "source_type", "geo_level", "tier", "source_file"]:
        df[col] = df[col].astype(str)
    for col in ["countycode", "zip_key"]:
        df[col] = df[col].astype("string").fillna("")
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def write_output(df: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    suffix = output.suffix.lower()

    if suffix == ".dta":
        df.to_stata(output, write_index=False, version=118)
    elif suffix == ".parquet":
        df.to_parquet(output, index=False)
    elif suffix == ".csv":
        df.to_csv(output, index=False)
    else:
        raise ValueError("Output extension must be .dta, .parquet, or .csv")


def output_dir_and_suffix(output: Path, output_format: str | None) -> tuple[Path, str]:
    if output_format:
        suffix = "." + output_format.lower().lstrip(".")
    elif output.suffix:
        suffix = output.suffix.lower()
    else:
        suffix = ".dta"

    if suffix not in {".dta", ".parquet", ".csv"}:
        raise ValueError("Output format must be dta, parquet, or csv")

    out_dir = output if not output.suffix else output.parent
    return out_dir, suffix


def write_by_state(frames: list[pd.DataFrame], output: Path, output_format: str | None) -> None:
    combined = pd.concat(frames, ignore_index=True)
    out_dir, suffix = output_dir_and_suffix(output, output_format)
    out_dir.mkdir(parents=True, exist_ok=True)

    for state, state_df in combined.groupby("state", sort=True):
        state_out = out_dir / f"{state}_attom_cells_combined{suffix}"
        write_output(state_df.reset_index(drop=True), state_out)
        n_files = state_df["source_file"].nunique()
        print(f"Wrote {len(state_df):,} rows from {n_files:,} files -> {state_out}")


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output = Path(args.output)
    states = {s.lower() for s in args.states} if args.states else None

    files = find_inputs(input_dir, states)
    if not files:
        raise FileNotFoundError(f"No matching ATTOM .dta files found in {input_dir}")

    frames: list[pd.DataFrame] = []
    failures: list[tuple[Path, Exception]] = []
    for path in files:
        try:
            frame = normalize_file(path)
        except Exception as exc:
            if args.strict:
                raise
            failures.append((path, exc))
            print(f"Skipping unreadable file: {path.name} ({exc})")
            continue
        frames.append(frame)
        print(f"Loaded {path.name:45} {len(frame):>10,} rows")

    if not frames:
        raise RuntimeError("All matching files failed to load.")

    print("")
    if args.by_state:
        write_by_state(frames, output, args.output_format)
    else:
        combined = pd.concat(frames, ignore_index=True)
        write_output(combined, output)
        print(f"Wrote {len(combined):,} rows from {len(frames):,} files -> {output}")

    if failures:
        print(f"Skipped {len(failures):,} unreadable files:")
        for path, exc in failures:
            print(f"  {path.name}: {exc}")


if __name__ == "__main__":
    main()

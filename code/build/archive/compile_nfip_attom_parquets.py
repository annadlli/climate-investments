"""Append state NFIP--ATTOM parquet outputs and write one Stata dataset."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


STATA_RENAMES = {
    "attom_assessed_value_improvements": "attom_assessed_improvements",
}


def state_from_path(path: Path) -> str:
    suffix = "_nfip_attom_property.parquet"
    if not path.name.endswith(suffix):
        raise ValueError(f"Unexpected state filename: {path.name}")
    return path.name.removesuffix(suffix).upper()


def stata_safe(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rename = dict(STATA_RENAMES)
    used: set[str] = set()
    for original in frame.columns:
        candidate = rename.get(original, original)
        candidate = re.sub(r"[^A-Za-z0-9_]", "_", candidate)
        if not re.match(r"^[A-Za-z_]", candidate):
            candidate = "v_" + candidate
        candidate = candidate[:32]
        base = candidate
        counter = 2
        while candidate.lower() in used:
            suffix = f"_{counter}"
            candidate = base[: 32 - len(suffix)] + suffix
            counter += 1
        used.add(candidate.lower())
        rename[original] = candidate

    mapping = pd.DataFrame({
        "parquet_name": list(frame.columns),
        "stata_name": [rename[c] for c in frame.columns],
    })
    frame = frame.rename(columns=rename)

    # pandas' Stata writer does not support nullable extension integers and
    # booleans. Preserve complete integer columns; use numeric missing values
    # for nullable columns. Convert strings to plain Python objects.
    for column in frame.columns:
        series = frame[column]
        if pd.api.types.is_bool_dtype(series.dtype):
            frame[column] = (
                series.astype("int8") if not series.isna().any()
                else series.astype("float64")
            )
        elif pd.api.types.is_integer_dtype(series.dtype):
            frame[column] = (
                series.astype("int64") if not series.isna().any()
                else series.astype("float64")
            )
        elif pd.api.types.is_string_dtype(series.dtype) or series.dtype == object:
            frame[column] = series.map(
                lambda value: None if pd.isna(value) else str(value)
            ).astype(object)
    return frame, mapping


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--pattern", default="*_nfip_attom_property.parquet",
        help="Glob pattern within --input-dir.",
    )
    parser.add_argument(
        "--states", nargs="+",
        help="Optional required state list; otherwise compile every matching file.",
    )
    parser.add_argument("--diagnostics", required=True)
    parser.add_argument("--name-map", required=True)
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    files = sorted(input_dir.glob(args.pattern))
    if not files:
        raise FileNotFoundError(
            f"No files matching {args.pattern!r} under {input_dir}"
        )

    found = {state_from_path(path): path for path in files}
    if len(found) != len(files):
        raise ValueError("State filenames are not unique")
    if args.states:
        required = {state.upper() for state in args.states}
        missing = required - set(found)
        extra = set(found) - required
        if missing or extra:
            raise ValueError(
                f"State-file mismatch: missing={sorted(missing)}, extra={sorted(extra)}"
            )

    frames = []
    for state, path in sorted(found.items()):
        print(f"Reading {state}: {path}", flush=True)
        frame = pd.read_parquet(path)
        if "state" not in frame:
            raise KeyError(f"{path} is missing state")
        observed = set(frame["state"].dropna().astype(str).str.upper().str.strip())
        if observed != {state}:
            raise ValueError(f"{path} contains states {sorted(observed)}")
        frames.append(frame)

    result = pd.concat(frames, ignore_index=True)
    if "property_id" not in result or result["property_id"].isna().any():
        raise ValueError("Compiled data require nonmissing property_id")
    if result["property_id"].duplicated().any():
        raise ValueError("property_id is not unique across compiled states")
    if "assigned_attomid" in result:
        assigned = result.loc[result["assigned_attomid"].notna(), "assigned_attomid"]
        if assigned.astype(str).duplicated().any():
            raise ValueError("assigned_attomid is not unique across compiled states")

    diagnostics = result.groupby("state", as_index=False).agg(
        nfip_properties=("property_id", "size"),
        attom_matched=("assigned_attomid", lambda value: value.notna().sum()),
        with_attom_value=("attom_value_year", lambda value: value.notna().sum()),
        builty_attom_nfip=("builty_elevated", lambda value: value.fillna(0).eq(1).sum()),
        singleton_assignments=("cell_singleton", lambda value: value.fillna(0).eq(1).sum()),
    )
    diagnostics["attom_match_rate"] = (
        diagnostics["attom_matched"] / diagnostics["nfip_properties"]
    )
    diagnostics["value_rate_among_matches"] = (
        diagnostics["with_attom_value"] / diagnostics["attom_matched"]
    )

    result, mapping = stata_safe(result)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    Path(args.diagnostics).parent.mkdir(parents=True, exist_ok=True)
    Path(args.name_map).parent.mkdir(parents=True, exist_ok=True)
    result.to_stata(output, write_index=False, version=118)
    diagnostics.to_csv(args.diagnostics, index=False)
    mapping.to_csv(args.name_map, index=False)

    print(diagnostics.to_string(index=False))
    print(f"Saved {len(result):,} observations from {len(found)} states: {output}")
    print(f"Saved diagnostics: {args.diagnostics}")
    print(f"Saved Stata name map: {args.name_map}")


if __name__ == "__main__":
    main()

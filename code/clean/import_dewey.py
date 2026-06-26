"""
Import raw Dewey data extracts.

Authors: Anna Li and Vendela Norman
Date: 2026-06-17

Description:
    Downloads selected Dewey external-data products into the raw data folder.

Notes / Sources:
    To replicate this step, you will need access to Dewey and a Dewey API key.
    Dewey datasets downloaded for this project include Builty building permits
    and ATTOM property/assessor-history records. Real API keys and endpoint URLs
    should not be committed to git; replace the placeholder endpoint values below
    only in a private/local copy or before running in a secure environment.
"""

from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime
from pathlib import Path


DEWEY_ENDPOINTS = {
    # Builty building permit records used to create raw/builty_all.parquet.
    "builty_all": "DEWEY_ENDPOINT_URL_FOR_BUILTY_BUILDING_PERMITS",
    # ATTOM property/assessor-history records used for state permit matching.
    "attom_tx": "DEWEY_ENDPOINT_URL_FOR_ATTOM_TEXAS",
    "attom_va": "DEWEY_ENDPOINT_URL_FOR_ATTOM_VIRGINIA",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download licensed Dewey extracts with placeholder endpoints."
    )
    parser.add_argument(
        "--data",
        required=True,
        help="Data root. Downloads are written under {data}/raw/dewey unless --out-dir is passed.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("DEWEY_API_KEY"),
        help="Dewey API key. Defaults to the DEWEY_API_KEY environment variable.",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        help=(
            "Named built-in Dewey datasets to download. Ignored when --manifest "
            "is passed unless --datasets is also used to filter manifest rows."
        ),
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "Optional private CSV with one Dewey endpoint per row. Required columns: "
            "name, endpoint. Optional column: folder. Each row downloads to its own folder."
        ),
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run folder name. Defaults to run_YYYYMMDD_HHMMSS.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Optional output root. Defaults to {data}/raw/dewey.",
    )
    parser.add_argument(
        "--skip-exists",
        action="store_true",
        help="Skip files that already exist in the destination folder.",
    )
    return parser.parse_args()


def clean_folder_name(value: str) -> str:
    return value.strip().strip("/").replace(" ", "_")


def endpoint_for(dataset: str) -> str:
    endpoint = DEWEY_ENDPOINTS[dataset]
    if endpoint.startswith("DEWEY_ENDPOINT_URL_"):
        raise ValueError(
            f"The Dewey endpoint for '{dataset}' is still a placeholder. "
            "Fill it from a Dewey account before running this download step."
        )
    return endpoint


def read_manifest(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"name", "endpoint"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"Manifest {path} is missing required column(s): {', '.join(sorted(missing))}"
            )

        for line_number, row in enumerate(reader, start=2):
            name = (row.get("name") or "").strip()
            endpoint = (row.get("endpoint") or "").strip()
            folder = clean_folder_name(row.get("folder") or name)
            if not name or not endpoint:
                raise ValueError(f"Manifest {path} has a blank name or endpoint on line {line_number}.")
            rows.append({"name": name, "endpoint": endpoint, "folder": folder})

    if not rows:
        raise ValueError(f"Manifest {path} has no endpoint rows.")
    return rows


def built_in_rows(datasets: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    unknown = sorted(set(datasets) - set(DEWEY_ENDPOINTS))
    if unknown:
        raise ValueError(f"Unknown built-in dataset(s): {', '.join(unknown)}")
    for dataset in datasets:
        rows.append(
            {
                "name": dataset,
                "endpoint": endpoint_for(dataset),
                "folder": clean_folder_name(dataset),
            }
        )
    return rows


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise ValueError("Pass --api-key or set DEWEY_API_KEY.")

    if args.manifest:
        downloads = read_manifest(Path(args.manifest))
        if args.datasets:
            wanted = set(args.datasets)
            downloads = [row for row in downloads if row["name"] in wanted]
            if not downloads:
                raise ValueError("No manifest rows matched --datasets.")
    else:
        downloads = built_in_rows(args.datasets or sorted(DEWEY_ENDPOINTS))

    data = Path(args.data)
    out_root = Path(args.out_dir) if args.out_dir else data / "raw" / "dewey"
    run_id = args.run_id if args.run_id else datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"Run directory: {run_dir}")
    print(f"Downloads: {', '.join(row['name'] for row in downloads)}")

    import deweydatapy as ddp

    for row in downloads:
        dataset = row["name"]
        dataset_dir = run_dir / row["folder"]
        dataset_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nFetching {dataset} file list from Dewey...")
        files = ddp.get_file_list(args.api_key, row["endpoint"], print_info=True)

        print(f"Downloading {dataset} files to {dataset_dir}...")
        ddp.download_files(files, str(dataset_dir), skip_exists=args.skip_exists)

    print("\nAll Dewey downloads complete.")


if __name__ == "__main__":
    main()

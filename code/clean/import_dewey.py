"""
Import raw Dewey data extracts.

Authors: Anna Li
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
import deweydatapy as ddp


# Built-in endpoints are placeholders for private Dewey URLs. A private manifest supplies the real endpoints without storing licensed URLs in this file.

#optional way of downloading if manifest is not wanted
DEWEY_ENDPOINTS = {
    # Builty building permit records used to create raw/builty_all.parquet.
    "builty_all": "DEWEY_ENDPOINT_URL_FOR_BUILTY_BUILDING_PERMITS",
    
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download licensed Dewey extracts with placeholder endpoints."
    )

    # Data location and Dewey credentials.
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

    # Dataset selection. --datasets can also restrict a manifest to a subset.
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

    # Output controls keep download runs separate and allow a run to be resumed.
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
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip files that already exist in the destination folder.",
    )
    return parser.parse_args()


def clean_folder_name(value: str) -> str:
    # Convert labels such as "ATTOM Texas" into portable folder names.
    return value.strip().strip("/").replace(" ", "_")


def read_manifest(path: Path) -> list[dict[str, str]]:
    # Read each private endpoint and its output folder from the manifest.
    with path.open(newline="") as handle:
        return [
            {
                "name": row["name"].strip(),
                "endpoint": row["endpoint"].strip(),
                "folder": clean_folder_name(row.get("folder") or row["name"]),
            }
            for row in csv.DictReader(handle)
        ]


def built_in_rows(datasets: list[str]) -> list[dict[str, str]]:
    # Convert the selected built-in endpoints to the same format as manifest rows.
    return [
        {
            "name": dataset,
            "endpoint": DEWEY_ENDPOINTS[dataset],
            "folder": clean_folder_name(dataset),
        }
        for dataset in datasets
    ]


def main() -> None:
    args = parse_args()

    # Use private manifest endpoints when supplied; otherwise use built-ins.
    if args.manifest:
        downloads = read_manifest(Path(args.manifest))
        if args.datasets:
            wanted = set(args.datasets)
            downloads = [row for row in downloads if row["name"] in wanted]
    else:
        downloads = built_in_rows(args.datasets or sorted(DEWEY_ENDPOINTS))

    # Keep each run separate so its batches can be compiled together later.
    data = Path(args.data)
    out_root = Path(args.out_dir) if args.out_dir else data / "raw" / "dewey"
    run_id = args.run_id if args.run_id else datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"Run directory: {run_dir}")
    print(f"Downloads: {', '.join(row['name'] for row in downloads)}")

    # Dewey returns a table of file links for each endpoint, then downloads
    # those files into the folder assigned to that manifest row.
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

"""
Import raw Dewey data extracts.

Authors: Anna Li and Vendela Norman
Date: 2026-06-17

Description:
    Downloads selected Dewey external-data products into the raw data folder.
    The script reads dataset names and Dewey endpoint URLs from a private
    manifest CSV. Do not commit real Dewey API keys or endpoint URLs.

Notes / Sources:
    To replicate this step, obtain Dewey access to the licensed data products
    used in this project. The Dewey downloads used here include Builty building
    permit records and ATTOM property/assessor-history records. Copy
    code/clean/dewey_manifest_template.csv to {data}/raw/dewey/dewey_manifest.csv
    or another private location, then fill in the endpoint_url values from your
    Dewey account.
"""

from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime
from pathlib import Path


REQUIRED_MANIFEST_COLUMNS = {"dataset", "endpoint_url"}
PLACEHOLDER_MARKERS = ("PLACEHOLDER", "TODO", "DEWEY_ENDPOINT_URL")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download licensed Dewey extracts listed in a private manifest CSV."
    )
    parser.add_argument(
        "--data",
        required=True,
        help="Data root. Downloads are written under {data}/raw/dewey unless --out-dir is passed.",
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help=(
            "Private CSV with columns dataset, endpoint_url, and optional notes/source. "
            "Defaults to {data}/raw/dewey/dewey_manifest.csv."
        ),
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
        help="Optional subset of manifest dataset names to download.",
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
    return parser.parse_args()


def read_manifest(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(
            f"Dewey manifest not found: {path}\n"
            "Create a private manifest from code/clean/dewey_manifest_template.csv. "
            "You will need Dewey access to the Builty and ATTOM products used by this project."
        )

    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        raise ValueError(f"Dewey manifest is empty: {path}")

    missing = REQUIRED_MANIFEST_COLUMNS - set(rows[0])
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise ValueError(f"Dewey manifest is missing required column(s): {missing_cols}")

    cleaned = []
    for row in rows:
        dataset = row.get("dataset", "").strip()
        endpoint_url = row.get("endpoint_url", "").strip()
        if not dataset:
            raise ValueError("Every Dewey manifest row must have a nonempty dataset name.")
        if not endpoint_url:
            raise ValueError(f"Dewey manifest row '{dataset}' is missing endpoint_url.")
        if any(marker in endpoint_url.upper() for marker in PLACEHOLDER_MARKERS):
            raise ValueError(
                f"Dewey manifest row '{dataset}' still has a placeholder endpoint_url. "
                "Fill it from the Dewey data portal before running."
            )
        cleaned.append({"dataset": dataset, "endpoint_url": endpoint_url})

    return cleaned


def select_datasets(rows: list[dict[str, str]], requested: list[str] | None) -> list[dict[str, str]]:
    if requested is None:
        return rows

    available = {row["dataset"] for row in rows}
    missing = sorted(set(requested) - available)
    if missing:
        raise ValueError(
            "Requested Dewey dataset(s) not in manifest: "
            + ", ".join(missing)
            + f"\nAvailable datasets: {', '.join(sorted(available))}"
        )
    return [row for row in rows if row["dataset"] in set(requested)]


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise ValueError("Pass --api-key or set DEWEY_API_KEY.")

    data = Path(args.data)
    manifest = Path(args.manifest) if args.manifest else data / "raw" / "dewey" / "dewey_manifest.csv"
    out_root = Path(args.out_dir) if args.out_dir else data / "raw" / "dewey"
    run_id = args.run_id if args.run_id else datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest_rows = select_datasets(read_manifest(manifest), args.datasets)

    import deweydatapy as ddp

    print(f"Manifest:      {manifest}")
    print(f"Run directory: {run_dir}")
    print(f"Datasets:      {', '.join(row['dataset'] for row in manifest_rows)}")

    for row in manifest_rows:
        dataset = row["dataset"]
        dataset_dir = run_dir / dataset
        dataset_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nFetching {dataset} file list from Dewey...")
        files = ddp.get_file_list(args.api_key, row["endpoint_url"], print_info=True)

        print(f"Downloading {dataset} files to {dataset_dir}...")
        ddp.download_files(files, str(dataset_dir))

    print("\nAll Dewey downloads complete.")


if __name__ == "__main__":
    main()

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
        default=sorted(DEWEY_ENDPOINTS),
        choices=sorted(DEWEY_ENDPOINTS),
        help="Dewey datasets to download.",
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


def endpoint_for(dataset: str) -> str:
    endpoint = DEWEY_ENDPOINTS[dataset]
    if endpoint.startswith("DEWEY_ENDPOINT_URL_"):
        raise ValueError(
            f"The Dewey endpoint for '{dataset}' is still a placeholder. "
            "Fill it from a Dewey account before running this download step."
        )
    return endpoint


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise ValueError("Pass --api-key or set DEWEY_API_KEY.")

    endpoints = {dataset: endpoint_for(dataset) for dataset in args.datasets}

    data = Path(args.data)
    out_root = Path(args.out_dir) if args.out_dir else data / "raw" / "dewey"
    run_id = args.run_id if args.run_id else datetime.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"Run directory: {run_dir}")
    print(f"Datasets: {', '.join(args.datasets)}")

    import deweydatapy as ddp

    for dataset in args.datasets:
        dataset_dir = run_dir / dataset
        dataset_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nFetching {dataset} file list from Dewey...")
        files = ddp.get_file_list(args.api_key, endpoints[dataset], print_info=True)

        print(f"Downloading {dataset} files to {dataset_dir}...")
        ddp.download_files(files, str(dataset_dir))

    print("\nAll Dewey downloads complete.")


if __name__ == "__main__":
    main()

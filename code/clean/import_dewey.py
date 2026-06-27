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
import time
from datetime import datetime
from urllib.parse import unquote, urlparse
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
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip files that already exist in the destination folder.",
    )
    parser.add_argument(
        "--retries",
        default=10,
        type=int,
        help="Number of download attempts per file.",
    )
    parser.add_argument(
        "--retry-sleep",
        default=60,
        type=int,
        help="Seconds to wait after a failed download attempt.",
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


def file_name_from_row(row: dict[str, object]) -> str:
    for col in ["filename", "file_name", "name", "object_name", "path", "file", "key"]:
        value = row.get(col)
        if value is not None and str(value).strip():
            return Path(str(value).strip()).name

    link = str(row["link"])
    parsed = urlparse(link)
    name = Path(unquote(parsed.path)).name
    if not name:
        raise ValueError("Could not infer Dewey filename from file-list row.")
    return name


def valid_parquet_file(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            header = handle.read(4)
            handle.seek(-4, 2)
            footer = handle.read(4)
        return header == b"PAR1" and footer == b"PAR1"
    except OSError:
        return False


def robust_download_files(files, dest_dir: Path, skip_exists: bool, retries: int, retry_sleep: int) -> None:
    import requests

    records = files.to_dict("records")
    total = len(records)

    for i, row in enumerate(records, start=1):
        if "link" not in row or not str(row["link"]).strip():
            raise ValueError("Dewey file list is missing required 'link' column.")

        filename = file_name_from_row(row)
        dest_path = dest_dir / filename
        part_path = dest_path.with_name(dest_path.name + ".part")

        if skip_exists and dest_path.exists() and dest_path.stat().st_size > 0:
            print(f"Skipping existing {i}/{total}: {dest_path}", flush=True)
            continue

        if part_path.exists():
            part_path.unlink()

        print(f"Downloading {i}/{total}: {dest_path}", flush=True)
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                with requests.get(str(row["link"]), stream=True, timeout=(30, 300)) as response:
                    response.raise_for_status()
                    expected = int(response.headers.get("Content-Length", "0") or 0)
                    written = 0
                    with part_path.open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                handle.write(chunk)
                                written += len(chunk)

                if expected and written != expected:
                    raise IOError(f"Incomplete download: wrote {written} bytes, expected {expected}")
                if dest_path.suffix == ".parquet" and not valid_parquet_file(part_path):
                    raise IOError("Incomplete parquet download: missing PAR1 header/footer")

                part_path.replace(dest_path)
                break
            except (OSError, requests.RequestException) as exc:
                last_error = exc
                if part_path.exists():
                    part_path.unlink()
                if attempt == retries:
                    raise RuntimeError(f"Failed downloading {dest_path} after {retries} attempts.") from exc
                print(
                    f"  attempt {attempt}/{retries} failed: {exc}. Retrying in {retry_sleep}s...",
                    flush=True,
                )
                time.sleep(retry_sleep)

        if last_error is None:
            continue


def main() -> None:
    args = parse_args()
    if not args.api_key:
        raise ValueError("Pass --api-key or set DEWEY_API_KEY.")
    if args.retries < 1:
        raise ValueError("--retries must be at least 1.")

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
        robust_download_files(
            files,
            dataset_dir,
            skip_exists=args.skip_exists,
            retries=args.retries,
            retry_sleep=args.retry_sleep,
        )

    print("\nAll Dewey downloads complete.")


if __name__ == "__main__":
    main()

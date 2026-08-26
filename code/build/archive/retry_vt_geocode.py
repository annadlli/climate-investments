"""Retry state ATTOM Census geocoding with alternate address cleaning.

Reads only ATTOMIDs whose existing geocode has missing coordinates. Successful
retry results are written separately and never overwrite production files.
"""

from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path

import duckdb
import pandas as pd

from geocode_attom import geocode


def quote(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


SUFFIXES = {
    "STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD", "ROAD": "RD",
    "DRIVE": "DR", "COURT": "CT", "CIRCLE": "CIR", "LANE": "LN",
    "PLACE": "PL", "PARKWAY": "PKWY", "HIGHWAY": "HWY", "TERRACE": "TER",
    "TRAIL": "TRL", "SQUARE": "SQ", "ROUTE": "RTE",
}
DIRECTIONS = {"NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W"}


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).upper().replace("\n", " ").replace("\r", " ")
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_street(value: object) -> str:
    text = clean_text(value)
    # Units often prevent Census matches but do not change the building point.
    text = re.sub(r"\s+(APT|APARTMENT|UNIT|STE|SUITE|BLDG|BUILDING)\s+.*$", "", text)
    words = text.split()
    words = [DIRECTIONS.get(word, SUFFIXES.get(word, word)) for word in words]
    return " ".join(words)


def clean_zip(value: object) -> str:
    if pd.isna(value):
        return ""
    match = re.match(r"\s*(\d{5})", str(value))
    return match.group(1) if match else ""


def address_id(row: pd.Series) -> str:
    value = "|".join(str(row[c]) for c in ["street", "city", "state", "zip"])
    return hashlib.md5(value.encode()).hexdigest()[:16]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default="vt")
    parser.add_argument("--raw", required=True)
    parser.add_argument("--geocoded", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--fills", required=True)
    parser.add_argument("--diagnostics", required=True)
    parser.add_argument("--memory", default="48GB")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--chunk-size", type=int, default=10000)
    parser.add_argument("--timeout", type=int, default=1800)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)
    (work / "duckdb_tmp").mkdir(parents=True, exist_ok=True)
    Path(args.fills).parent.mkdir(parents=True, exist_ok=True)
    Path(args.diagnostics).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET temp_directory={quote(work / 'duckdb_tmp')}")
    con.execute("SET preserve_insertion_order=false")

    # One row per currently ungeocoded ATTOMID, joined to the raw structured
    # address fields. max() is deterministic and mirrors the main geocoder.
    raw = con.execute(f"""
        WITH missing AS (
            SELECT cast(attomid AS varchar) AS attomid
            FROM read_parquet({quote(args.geocoded)})
            GROUP BY 1
            HAVING max(longitude) IS NULL OR max(latitude) IS NULL
        )
        SELECT
            cast(r.ATTOMID AS varchar) AS attomid,
            max(trim(cast(r.PROPERTYADDRESSFULL AS varchar))) AS full_address,
            max(trim(cast(r.PROPERTYADDRESSCITY AS varchar))) AS city,
            max(trim(cast(r.PROPERTYADDRESSSTATE AS varchar))) AS state,
            max(trim(cast(r.PROPERTYADDRESSZIP AS varchar))) AS zip_raw
        FROM read_parquet({quote(args.raw)}) r
        INNER JOIN missing m ON cast(r.ATTOMID AS varchar)=m.attomid
        GROUP BY 1
        ORDER BY 1
    """).df()
    con.close()

    retry = pd.DataFrame({
        "attomid": raw["attomid"].astype("string"),
        "street": raw["full_address"].map(clean_street),
        "city": raw["city"].map(clean_text),
        "state": raw["state"].map(clean_text),
        "zip": raw["zip_raw"].map(clean_zip),
    })
    retry.loc[retry["state"].eq(""), "state"] = args.state.upper()
    retry["eligible_retry"] = retry["street"].ne("") & (
        retry["zip"].ne("") | (retry["city"].ne("") & retry["state"].ne(""))
    )
    eligible = retry.loc[retry["eligible_retry"]].copy()
    eligible["addrid"] = eligible.apply(address_id, axis=1)

    xwalk = eligible[["attomid", "addrid"]].drop_duplicates()
    addresses = (eligible.drop_duplicates("addrid")
                 [["addrid", "street", "city", "state", "zip"]]
                 .sort_values("addrid"))
    retry.to_parquet(work / "vt_retry_universe.parquet", index=False)
    xwalk.to_parquet(work / "attomid_xwalk.parquet", index=False)
    addresses.to_parquet(work / "addresses.parquet", index=False)

    geocode(
        addresses, work, "Public_AR_Current", "Current_Current",
        args.chunk_size, args.workers, args.timeout,
    )

    result_path = work / "blockgroups_by_address.parquet"
    if not result_path.exists():
        raise RuntimeError("Retry batches are incomplete; resubmit the job to resume cached batches.")
    results = pd.read_parquet(result_path)
    fills = xwalk.merge(results, on="addrid", how="left", validate="many_to_one")
    successful = fills.loc[
        fills["longitude"].notna() & fills["latitude"].notna()
    ].copy()
    successful.to_parquet(args.fills, index=False)

    diagnostics = pd.DataFrame([{
        "missing_coordinate_attomids": len(retry),
        "eligible_retry_attomids": len(eligible),
        "ineligible_missing_address_or_location": int((~retry["eligible_retry"]).sum()),
        "unique_retry_addresses": len(addresses),
        "successful_retry_addresses": int(results["longitude"].notna().sum()),
        "successful_retry_attomids": successful["attomid"].nunique(),
        "retry_attomid_success_percent": round(
            100 * successful["attomid"].nunique() / max(len(eligible), 1), 2
        ),
    }])
    diagnostics.to_csv(args.diagnostics, index=False)
    print(diagnostics.to_string(index=False))
    print(f"Saved {args.fills}")
    print(f"Saved {args.diagnostics}")


if __name__ == "__main__":
    main()

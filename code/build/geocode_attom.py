"""
Authors: Anna Li
Date: 2026-08-08

Geocoding attom addresses. Clean the ATTOM addresses, drop duplicates, and send the
unique addresses to the Census batch geocoder. Output one file per state with the
matched block group and coordinates.
Desired variables are censusblockgroupfips, longitude, latitude. The output is used to merge the ATTOM.
"""

from __future__ import annotations

import argparse
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import pandas as pd
import requests

BATCH_URL = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"

# Census batch return columns (no header in the response CSV); id is addrid.
RESULT_COLS = [
    "id", "input_address", "match", "match_type", "matched_address",
    "lonlat", "tigerline_id", "side", "statefp", "countyfp", "tract", "block",
]
USE_COLS = ["id", "match", "match_type", "lonlat", "statefp", "countyfp", "tract", "block"]


def quote(s: str) -> str:
    # sql-safe strings
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    # state, data, sample, duckdb + census knobs
    p = argparse.ArgumentParser(description="Extract ATTOM addresses and geocode them to block groups.")
    p.add_argument("--state",      required=True)
    p.add_argument("--data",       required=True, help="Data root (from master.do).")
    p.add_argument("--sample",     default=0, type=int, help="Pilot: keep only the first N properties.")
    p.add_argument("--memory",     default="24GB", help="DuckDB memory cap; keep below the SLURM allocation.")
    p.add_argument("--benchmark",  default="Public_AR_Current")
    p.add_argument("--vintage",    default="Current_Current")
    p.add_argument("--chunk-size", default=10000, type=int, help="Max 10,000 (Census limit).")
    p.add_argument("--workers",    default=4, type=int, help="Parallel batch uploads.")
    p.add_argument("--timeout",    default=1800, type=int, help="Per-batch timeout in seconds.")
    return p.parse_args()


def resolve_parquet(data: Path, state: str) -> Path:
    # find the attom parquet for this state
    for c in [data / state / f"attom_{state}.parquet",
              data / "raw" / "attom" / f"attom_{state}.parquet",
              data / f"attom_{state}.parquet"]:
        if c.exists():
            return c
    raise FileNotFoundError(f"No ATTOM parquet for {state} under {data}")


# phase a: extract + dedupe addresses
def extract_addresses(parquet: Path, state: str, sample: int, memory: str, tmp: Path):
    # one row per attomid, one addrid per cleaned address
    con = duckdb.connect()
    # duckdb memory cap; spill to tmp if needed
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit={quote(memory)}")
    con.execute(f"SET temp_directory={quote(str(tmp))}")
    sample_source = ""
    sample_join = ""
    if sample > 0:
        sample_source = f"""
            sample_ids AS (
                SELECT DISTINCT ATTOMID
                FROM read_parquet({quote(str(parquet))})
                WHERE (PROPERTYADDRESSSTREETNAME IS NOT NULL OR PROPERTYADDRESSFULL IS NOT NULL)
                ORDER BY ATTOMID
                LIMIT {sample}
            ),
        """
        sample_join = "INNER JOIN sample_ids USING (ATTOMID)"
    # build a cleaned street line, then hash it for addrid
    df = con.execute(f"""
        WITH {sample_source}
        per_prop AS (
            -- max() FILTER(non-empty), not any_value(): deterministic across
            -- runs, so cached chunk files always match a fresh extraction.
            SELECT
                ATTOMID,
                max(trim(cast(PROPERTYADDRESSHOUSENUMBER        AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSHOUSENUMBER        AS varchar)) != '') AS housenum,
                max(trim(cast(PROPERTYADDRESSSTREETDIRECTION    AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSSTREETDIRECTION    AS varchar)) != '') AS predir,
                max(trim(cast(PROPERTYADDRESSSTREETNAME         AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSSTREETNAME         AS varchar)) != '') AS stname,
                max(trim(cast(PROPERTYADDRESSSTREETSUFFIX       AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSSTREETSUFFIX       AS varchar)) != '') AS suffix,
                max(trim(cast(PROPERTYADDRESSSTREETPOSTDIRECTION AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSSTREETPOSTDIRECTION AS varchar)) != '') AS postdir,
                max(trim(cast(PROPERTYADDRESSFULL               AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSFULL               AS varchar)) != '') AS fulladdr,
                -- exclude junk city values holding the state abbreviation, so
                -- max() cannot prefer them over a real city name
                max(trim(cast(PROPERTYADDRESSCITY               AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSCITY               AS varchar)) != ''
                            AND upper(trim(cast(PROPERTYADDRESSCITY    AS varchar))) != upper({quote(state)})) AS city,
                max(trim(cast(PROPERTYADDRESSSTATE              AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSSTATE              AS varchar)) != '') AS state,
                max(trim(cast(PROPERTYADDRESSZIP                AS varchar)))
                    FILTER (trim(cast(PROPERTYADDRESSZIP                AS varchar)) != '') AS zip_raw
            FROM read_parquet({quote(str(parquet))})
            {sample_join}
            GROUP BY ATTOMID
        ),
        built AS (
            SELECT
                cast(ATTOMID AS varchar) AS attomid,
                CASE
                    WHEN housenum IS NOT NULL AND housenum != '' AND stname IS NOT NULL AND stname != ''
                    THEN trim(concat_ws(' ', housenum, predir, stname, suffix, postdir))
                    ELSE coalesce(fulladdr, '')
                END AS street,
                coalesce(city, '')  AS city,
                coalesce(state, '') AS state,
                CASE
                    WHEN zip_raw IS NULL OR trim(zip_raw) = '' THEN ''
                    WHEN lpad(regexp_extract(trim(zip_raw), '^(\\d+)', 1), 5, '0') = '00000' THEN ''
                    ELSE lpad(regexp_extract(trim(zip_raw), '^(\\d+)', 1), 5, '0')
                END AS zip
            FROM per_prop
        )
        SELECT
            attomid, street, city, state, zip,
            substr(md5(concat_ws('|', street, city, state, zip)), 1, 16) AS addrid
        FROM built
        WHERE street != ''
        ORDER BY attomid
    """).df()
    con.close()
    # strip embedded newlines so the batch format stays clean
    for c in ["street", "city", "state", "zip"]:
        df[c] = df[c].str.replace(r"[\r\n]+", " ", regex=True)
    return df


# phase b: geocode via census api
#
def write_chunks(df: pd.DataFrame, chunk_dir: Path, chunk_size: int) -> list[Path]:
    # Census accepts CSV rows of the form: id,street,city,state,zip
    # Write one chunk file per slice of the unique-address list so the batch
    chunk_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(0, len(df), chunk_size):
        path = chunk_dir / f"chunk_{i // chunk_size:05d}.csv"
        if not path.exists():
            df.iloc[i:i + chunk_size].to_csv(path, index=False, header=False, quoting=csv.QUOTE_ALL)
        paths.append(path)
    return paths


def geocode_chunk(chunk: Path, out: Path, benchmark: str, vintage: str, timeout: int) -> str:
    # If a result file already exists and is non-empty, skip rerun; allow rerunning after failures
    if out.exists() and out.stat().st_size > 0:
        return "cached"

    # Add attempts to handle network failures
    for attempt in range(5):
        try:
            with open(chunk, "rb") as fh:
                r = requests.post(
                    BATCH_URL,
                    files={"addressFile": (chunk.name, fh, "text/csv")},
                    data={"benchmark": benchmark, "vintage": vintage},
                    timeout=timeout,
                )
            r.raise_for_status()
            tmp = out.with_suffix(".tmp")
            tmp.write_text(r.text, encoding="utf-8")
            tmp.replace(out)
            return "ok"
        except requests.RequestException as e:
            # Retry network and HTTP failures only
            if attempt == 4:
                return f"failed: {e}"
            time.sleep(90 * (attempt + 1))
    return "failed"


def prune_bad_results(chunks: list[Path], result_dir: Path) -> int:
    # If a saved output file is corrupt or incomplete relative to its input chunk, drop it so the chunk will be geocoded again on the next run.
    pruned = 0
    for c in chunks:
        outf = result_dir / f"{c.stem}_out.csv"
        if not outf.exists():
            continue
        n_in = sum(1 for _ in open(c, "rb"))
        n_out = sum(1 for _ in open(outf, "rb"))
        if n_in != n_out:
            print(f"  pruning {outf.name}: {n_out} result rows vs {n_in} input rows")
            outf.unlink()
            pruned += 1
    return pruned


def preflight(chunk_dir: Path, work: Path, benchmark: str, vintage: str) -> None:
    # test geocoder to ensure it is working before full batch run
    first = sorted(chunk_dir.glob("chunk_*.csv"))[0]
    probe = work / "probe.csv"
    with open(first, encoding="utf-8") as fh, open(probe, "w", encoding="utf-8") as out:
        for i, line in enumerate(fh):
            if i >= 20:
                break
            out.write(line)
    with open(probe, "rb") as fh:
        r = requests.post(
            BATCH_URL,
            files={"addressFile": (probe.name, fh, "text/csv")},
            data={"benchmark": benchmark, "vintage": vintage},
            timeout=300,
        )
    r.raise_for_status()
    n = sum(1 for ln in r.text.splitlines() if ln.strip())
    print(f"Preflight probe ok: HTTP {r.status_code}, {n} result rows")


def combine_results(result_dir: Path) -> pd.DataFrame:
    # combine chunk outputs into one addrid-level file
    frames = [pd.read_csv(f, names=RESULT_COLS, usecols=USE_COLS, dtype=str, on_bad_lines="skip")
              for f in sorted(result_dir.glob("chunk_*_out.csv"))]
    res = pd.concat(frames, ignore_index=True)
    del frames
    matched = res["match"].str.strip() == "Match"
    res["censusblockgroupfips"] = ""
    res.loc[matched, "censusblockgroupfips"] = (
        res.loc[matched, "statefp"].str.strip().str.zfill(2)
        + res.loc[matched, "countyfp"].str.strip().str.zfill(3)
        + res.loc[matched, "tract"].str.strip().str.zfill(6)
        + res.loc[matched, "block"].str.strip().str.zfill(4).str[0]
    )
    lonlat = res["lonlat"].fillna("").str.split(",", expand=True)
    res["longitude"] = pd.to_numeric(lonlat[0], errors="coerce")
    res["latitude"]  = pd.to_numeric(lonlat[1] if 1 in lonlat else pd.NA, errors="coerce")
    keep = res[["id", "match", "match_type", "censusblockgroupfips", "longitude", "latitude"]].copy()
    for c in ["id", "match", "match_type", "censusblockgroupfips"]:
        keep[c] = keep[c].fillna("").astype(str)
    return (keep.sort_values(["id", "censusblockgroupfips"], ascending=[True, False])
                .drop_duplicates(subset="id", keep="first")
                .reset_index(drop=True)
                .rename(columns={"id": "addrid"}))

#break address list into Census-sized chunks and merge back to one file for output
def geocode(addrs: pd.DataFrame, work: Path, benchmark: str, vintage: str,
            chunk_size: int, workers: int, timeout: int) -> None:
    chunk_dir = work / "chunks"
    result_dir = work / "results"
    result_dir.mkdir(parents=True, exist_ok=True)

    chunks = write_chunks(addrs, chunk_dir, min(chunk_size, 10000))
    print(f"Batches: {len(chunks)} (benchmark={benchmark}, vintage={vintage})")

    pruned = prune_bad_results(chunks, result_dir)
    if pruned:
        print(f"Pruned {pruned} corrupt cached results; they will be re-geocoded.")

    jobs = {c: result_dir / f"{c.stem}_out.csv" for c in chunks}
    pending = [c for c, o in jobs.items() if not (o.exists() and o.stat().st_size > 0)]
    if pending:
        # Probe the geocoder before committing to the full batch. Preflight
        # exists to stop a doomed run, so let a failure here propagate: a broken
        # endpoint or chunk format otherwise costs hours and yields empty files.
        preflight(chunk_dir, work, benchmark, vintage)

    done = failed = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(geocode_chunk, c, o, benchmark, vintage, timeout): c
            for c, o in jobs.items()
        }
        for fut in as_completed(futures):
            status = fut.result()
            done += 1
            if status.startswith("failed"):
                failed += 1
                print(f"  {futures[fut].name}: {status}")
            if done % 25 == 0 or done == len(chunks):
                print(f"  {done}/{len(chunks)} batches done ({failed} failed)")

    pruned = prune_bad_results(chunks, result_dir)
    missing = [c.stem for c in chunks if not (result_dir / f"{c.stem}_out.csv").exists()]
    if missing:
        print(f"Incomplete: {len(missing)} of {len(chunks)} batches are still missing "
              f"({failed} failed this run, {pruned} pruned as corrupt).")
        return

    res = combine_results(result_dir)
    n_match = (res["censusblockgroupfips"] != "").sum()
    print(f"Geocoded {n_match:,} / {len(res):,} unique addresses to a block group "
          f"({n_match / max(len(res), 1):.1%})")
    out = work / "blockgroups_by_address.parquet"
    res.to_parquet(out, index=False)
    print(f"Saved: {out}")


def main() -> None:
    args    = parse_args()
    state   = args.state.lower()
    data    = Path(args.data)
    parquet = resolve_parquet(data, state)

    # sample runs get their own work dir
    tag  = f"{state}_sample{args.sample}" if args.sample > 0 else state
    work = data / "build" / "attom_geocode" / f"{tag}_addr"
    work.mkdir(parents=True, exist_ok=True)

    # clean and dedupe addresses
    print(f"Reading addresses: {parquet}")
    df = extract_addresses(parquet, state, args.sample, args.memory, work / "duckdb_tmp")
    addrs = (df.drop_duplicates("addrid").sort_values("addrid")
               [["addrid", "street", "city", "state", "zip"]])
    print(f"Distinct properties with a street address: {len(df):,} "
          f"-> {len(addrs):,} unique addresses "
          f"({1 - len(addrs) / max(len(df), 1):.1%} fewer geocoder requests)")
    df[["attomid", "addrid"]].to_parquet(work / "attomid_xwalk.parquet", index=False)
    addrs.to_parquet(work / "addresses.parquet", index=False)
    print(f"Saved: {work}/attomid_xwalk.parquet + addresses.parquet")

    # geocode the unique addresses
    geocode(addrs, work, args.benchmark, args.vintage,
            args.chunk_size, args.workers, args.timeout)


if __name__ == "__main__":
    main()

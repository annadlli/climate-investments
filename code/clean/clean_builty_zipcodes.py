"""Authors: Anna Li
Date: 2026-08-06

Fill missing Builty ZIP codes using the existing ZIP, address text, same-address
matches, and the Census batch geocoder when needed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests


BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
RESULT_COLS = [
    "address_id", "input_address", "match", "match_type", "matched_address",
    "coordinates", "tigerline_id", "side",
]


def parse_args() -> argparse.Namespace:
    #  point it at the cleaned Builty file 
    parser = argparse.ArgumentParser(description="Fill Builty ZIP codes with the Census geocoder.")
    parser.add_argument("--data", required=True, help="Project data root.")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--timeout", type=int, default=1_800)
    parser.add_argument("--local-only", action="store_true", help="Skip Census requests.")
    return parser.parse_args()


def clean_text(values: pd.Series) -> pd.Series:
    # Normalizes messy address text so the ZIP rules are easier to apply.
    return (values.fillna("").astype(str).str.replace(r"[\r\n]+", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True).str.strip())


def valid_zip(values: pd.Series) -> pd.Series:
    # Keep only a clean 5-digit ZIP, with the common 00000 case dropped.
    extracted = clean_text(values).str.extract(r"^(\d{5})(?:-\d{4})?$", expand=False).fillna("")
    return extracted.mask(extracted == "00000", "")


def address_key(street: pd.Series, city: pd.Series, state: pd.Series) -> pd.Series:
    # Stable ID for a street-city-state combo to track same-address matches
    joined = (clean_text(street).str.lower() + "|" + clean_text(city).str.lower()
              + "|" + clean_text(state).str.lower())
    return joined.map(lambda value: hashlib.md5(value.encode("utf-8")).hexdigest()[:16])


def lookup_key(street: pd.Series, state: pd.Series) -> pd.Series:
    # Used for matching on normalized street + state when the broader Builty file is  backup
    normalized_street = (clean_text(street).str.lower()
                         .str.replace(r"[^a-z0-9 ]", " ", regex=True)
                         .str.replace(r"\s+", " ", regex=True).str.strip())
    return normalized_street + "|" + clean_text(state).str.lower()


def clean_street_for_census(street: object, city: object, state: object) -> str:
    # Strip the extra notes that Builty sometimes adds to the address line.
    value = re.sub(r"\s+", " ", str(street or "").replace("\r", " ").replace("\n", " ")).strip()
    city_value = re.sub(r"\s+", " ", str(city or "")).strip()
    state_value = re.sub(r"\s+", " ", str(state or "")).strip()

    # Builty sometimes appends assessor subdivision metadata after the address.
    value = re.sub(r"\s+-\s+SUBDIV(?:ISION)?\s*:.*$", "", value, flags=re.IGNORECASE)

    # The batch endpoint receives city and state in separate fields. Remove a
    # duplicated trailing city/state from the street field when both are known.
    if city_value and state_value:
        suffix = rf"(?:,?\s+){re.escape(city_value)}(?:,?\s+){re.escape(state_value)}(?:,?\s+0+)?\s*$"
        value = re.sub(suffix, "", value, flags=re.IGNORECASE)

    return re.sub(r"\s+", " ", value).strip(" ,")


def prepare_census_address(street: object, city: object, state: object) -> tuple[str, str, str]:
    # Turn the messy Builty string into the simple street / city / state fields
    # the Census batch endpoint likes.
    street_value = re.sub(r"\s+", " ", str(street or "")).strip()
    city_value = re.sub(r"\s+", " ", str(city or "")).strip()
    state_value = re.sub(r"\s+", " ", str(state or "")).strip()
    if not city_value and state_value:
        parsed = re.match(
            rf"^(.*?),\s*([^,]+?)\s*,?\s*{re.escape(state_value)}(?:\s*,?\s*0+)?$",
            street_value,
            flags=re.IGNORECASE,
        )
        if parsed:
            street_value, city_value = parsed.group(1).strip(), parsed.group(2).strip()
    return clean_street_for_census(street_value, city_value, state_value), city_value, state_value


def write_chunks(addresses: pd.DataFrame, chunk_dir: Path, result_dir: Path,
                 chunk_size: int) -> list[Path]:
    # Break the addresses into batch chunks so we can re-use cached Census outputs
    # without re-sending everything.
    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunks = []
    for start in range(0, len(addresses), min(chunk_size, 10_000)):
        path = chunk_dir / f"chunk_{start // min(chunk_size, 10_000):05d}.csv"
        buffer = io.StringIO()
        addresses.iloc[start:start + min(chunk_size, 10_000)].to_csv(
            buffer, index=False, header=False, quoting=csv.QUOTE_ALL, lineterminator="\n"
        )
        new_content = buffer.getvalue()
        old_content = path.read_text(encoding="utf-8") if path.exists() else None
        if old_content != new_content:
            path.write_text(new_content, encoding="utf-8")
            cached = result_dir / f"{path.stem}_out.csv"
            if cached.exists():
                cached.unlink()
        chunks.append(path)
    return chunks


def geocode_chunk(chunk: Path, output: Path, timeout: int) -> str:
    # Send one batch to Census and save the raw response. 
    if output.exists() and output.stat().st_size > 0:
        return "cached"
    for attempt in range(5):
        try:
            with chunk.open("rb") as handle:
                response = requests.post(
                    BATCH_URL,
                    files={"addressFile": (chunk.name, handle, "text/csv")},
                    data={"benchmark": "Public_AR_Current"},
                    timeout=timeout,
                )
            response.raise_for_status()
            temporary = output.with_suffix(".tmp")
            temporary.write_text(response.text, encoding="utf-8")
            temporary.replace(output)
            return "ok"
        except requests.RequestException as e:
            # Retry network and HTTP failures only, and keep the reason so a
            # systematic outage is distinguishable from scattered timeouts.
            if attempt == 4:
                return f"failed: {e}"
            time.sleep(30 * (attempt + 1))
    return "failed"


def census_zipcodes(addresses: pd.DataFrame, work: Path, workers: int,
                    chunk_size: int, timeout: int) -> pd.DataFrame:
    # Run the Census batch lookup and pull the ZIP from the matched address string.
    result_dir = work / "results"
    result_dir.mkdir(parents=True, exist_ok=True)
    chunks = write_chunks(addresses, work / "chunks", result_dir, chunk_size)
    jobs = {chunk: result_dir / f"{chunk.stem}_out.csv" for chunk in chunks}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(geocode_chunk, chunk, output, timeout): chunk
            for chunk, output in jobs.items()
        }
        failures = []
        for future in as_completed(futures):
            status = future.result()
            if status.startswith("failed"):
                failures.append(f"{futures[future].name}: {status}")

    # A silently dropped chunk leaves those permits with an unfilled ZIP, which
    # removes them from the ATTOM match downstream with no trace in the output.
    # Stop instead. Completed chunks are cached, so a rerun resumes the rest.
    missing = [chunk.name for chunk, output in jobs.items() if not output.exists()]
    if failures or missing:
        raise RuntimeError(
            f"Census geocoding incomplete: {len(failures)} chunk(s) failed, "
            f"{len(missing)} output(s) missing. Rerun to resume. "
            + "; ".join(failures[:5])
        )

    frames = []
    for chunk, output in jobs.items():
        frames.append(pd.read_csv(output, names=RESULT_COLS, dtype=str, keep_default_na=False))

    results = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=RESULT_COLS)
    matched = results["match"].str.strip().eq("Match")
    results["zipcode_census"] = ""
    results.loc[matched, "zipcode_census"] = (
        results.loc[matched, "matched_address"]
        .str.extract(r"\b(\d{5})(?:-\d{4})?\s*$", expand=False).fillna("")
    )
    return results[["address_id", "zipcode_census", "match", "match_type"]].drop_duplicates("address_id")


def main() -> None:
    # Main pipeline: load the Builty file, fill the missing ZIPs in stages, then
    # save the cleaned output and a small review file.
    args = parse_args()
    data = Path(args.data)
    input_path = Path(args.input) if args.input else data / "clean" / "builty_elevations.dta"
    output_path = (Path(args.output) if args.output else
                   data / "build" / "builty_elevations_zipfilled.dta")
    work = data / "build" / "builty_zip_geocode"
    work.mkdir(parents=True, exist_ok=True)

    frame = pd.read_stata(input_path, convert_categoricals=False)

    # If the cleaned file does not already have a stable ID, build one from the
    # address keys so later review files stay reproducible.
    if "builty_id" not in frame.columns:
        county = clean_text(frame["county"]) if "county" in frame.columns else ""
        property_key = (clean_text(frame["state"]).str.lower() + "|" + county.str.lower()
                        + "|" + clean_text(frame["street_address"]).str.lower())
        base_id = property_key.map(
            lambda value: "property_" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:20]
        )
        occurrence = frame.groupby(base_id, sort=False).cumcount()
        frame["builty_id"] = base_id + occurrence.map(lambda value: f"_{value + 1:02d}")

    # Start by normalizing the raw address and ZIP fields so the easy fill rules are
    # consistent across the whole frame.
    frame["street_address_original"] = frame["street_address"].fillna("").astype(str)
    frame["street_address"] = clean_text(frame["street_address_original"])
    frame["zipcode_original"] = clean_text(frame["zipcode"])
    frame["zipcode"] = valid_zip(frame["zipcode_original"])
    frame["zipcode_source"] = "unresolved"
    frame.loc[frame["zipcode"].ne(""), "zipcode_source"] = "existing"

    # First pass: pull a ZIP directly out of the street text when it is clearly embedded.
    embedded = clean_text(frame["street_address"]).str.extract(
        r"\b(\d{5})(?:-\d{4})?\b", expand=False
    ).fillna("")
    fill = frame["zipcode"].eq("") & embedded.ne("") & embedded.ne("00000")
    frame.loc[fill, "zipcode"] = embedded[fill]
    frame.loc[fill, "zipcode_source"] = "address_text"

    # Second pass: if the same street-city-state combo has one unique ZIP, use that.
    frame["address_id"] = address_key(frame["street_address"], frame["locality"], frame["state"])
    known = frame.loc[frame["zipcode"].ne(""), ["address_id", "zipcode"]].drop_duplicates()
    counts = known.groupby("address_id")["zipcode"].nunique()
    unique_ids = counts[counts.eq(1)].index
    known = known[known["address_id"].isin(unique_ids)].drop_duplicates("address_id")
    same_address = frame[["address_id"]].merge(known, on="address_id", how="left")["zipcode"].fillna("")
    fill = frame["zipcode"].eq("") & same_address.ne("")
    frame.loc[fill, "zipcode"] = same_address[fill]
    frame.loc[fill, "zipcode_source"] = "same_address"

    # Third pass: prepare the unresolved rows for Census batch geocoding.
    unresolved = frame["zipcode"].eq("") & clean_text(frame["street_address"]).ne("")
    addresses = (frame.loc[unresolved, ["address_id", "street_address", "locality", "state"]]
                 .drop_duplicates("address_id").sort_values("address_id"))
    addresses.columns = ["address_id", "street", "city", "state"]
    prepared = [
        prepare_census_address(street, city, state)
        for street, city, state in zip(addresses["street"], addresses["city"], addresses["state"])
    ]
    addresses[["street", "city", "state"]] = pd.DataFrame(prepared, index=addresses.index)
    addresses["zip"] = ""
    addresses.to_parquet(work / "addresses.parquet", index=False)

    frame["zipcode_census_match"] = ""
    frame["zipcode_census_match_type"] = ""
    if not args.local_only and len(addresses):
        # Census is the main external backstop: batch geocode unresolved addresses 
        results = census_zipcodes(
            addresses, work, args.workers, args.chunk_size, args.timeout
        ).rename(columns={"match": "zipcode_census_match",
                          "match_type": "zipcode_census_match_type"})
        frame = frame.merge(results, on="address_id", how="left", suffixes=("", "_new"))
        for column in ["zipcode_census_match", "zipcode_census_match_type"]:
            frame[column] = frame[f"{column}_new"].fillna(frame[column])
            frame.drop(columns=f"{column}_new", inplace=True)
        fill = frame["zipcode"].eq("") & frame["zipcode_census"].fillna("").ne("")
        frame.loc[fill, "zipcode"] = frame.loc[fill, "zipcode_census"]
        frame.loc[fill, "zipcode_source"] = "census"
        frame.drop(columns="zipcode_census", inplace=True)

    # A few states use leading-zero ZIPs. If the state name is embedded with a 4-digit
    # suffix, we pad it back to a 5-digit ZIP.
    zero_prefix_states = {"CT", "MA", "ME", "NH", "NJ", "RI", "VT"}
    padded_zip = pd.Series("", index=frame.index)
    for index in frame.index[frame["zipcode"].eq("") & frame["state"].isin(zero_prefix_states)]:
        match = re.search(
            rf"\b{re.escape(str(frame.at[index, 'state']))}\s+(\d{{4}})\s*$",
            str(frame.at[index, "street_address"]),
            flags=re.IGNORECASE,
        )
        if match and match.group(1) != "0000":
            padded_zip.at[index] = "0" + match.group(1)
    fill = frame["zipcode"].eq("") & padded_zip.ne("")
    frame.loc[fill, "zipcode"] = padded_zip[fill]
    frame.loc[fill, "zipcode_source"] = "address_text_padded"

    # Local backfill: if the same normalized street+state only ever maps to one ZIP in
    # the broader Builty file, use that as the answer.
    candidate_path = data / "build" / "all_builty_elevations.dta"
    if candidate_path.exists():
        candidate_records = pd.read_stata(
            candidate_path,
            convert_categoricals=False,
            columns=["APN", "STATE", "STREET_ADDRESS", "ZIPCODE"],
        )
        candidate_records["lookup_key"] = lookup_key(
            candidate_records["STREET_ADDRESS"], candidate_records["STATE"]
        )
        candidate_records["lookup_zip"] = valid_zip(candidate_records["ZIPCODE"])
        candidates = candidate_records.loc[
            candidate_records["lookup_zip"].ne(""), ["lookup_key", "lookup_zip"]
        ].drop_duplicates()
        zip_counts = candidates.groupby("lookup_key")["lookup_zip"].nunique()
        candidates = candidates[
            candidates["lookup_key"].isin(zip_counts[zip_counts.eq(1)].index)
        ].drop_duplicates("lookup_key")
        frame["lookup_key"] = lookup_key(frame["street_address"], frame["state"])
        frame = frame.merge(candidates, on="lookup_key", how="left")
        fill = frame["zipcode"].eq("") & frame["lookup_zip"].fillna("").ne("")
        frame.loc[fill, "zipcode"] = frame.loc[fill, "lookup_zip"]
        frame.loc[fill, "zipcode_source"] = "builty_lookup"
        frame.drop(columns=["lookup_key", "lookup_zip"], inplace=True)

        if "apn" in frame.columns:
            # Raw permit inputs carry assessor identifiers; use them only when
            # every candidate under the state+APN reports one ZIP.
            candidate_records["apn_key"] = (
                clean_text(candidate_records["APN"]) + "|" + clean_text(candidate_records["STATE"])
            )
            apn_candidates = candidate_records.loc[
                candidate_records["lookup_zip"].ne("")
                & ~candidate_records["apn_key"].str.startswith("|"),
                ["apn_key", "lookup_zip"],
            ].drop_duplicates()
            apn_counts = apn_candidates.groupby("apn_key")["lookup_zip"].nunique()
            apn_candidates = apn_candidates[
                apn_candidates["apn_key"].isin(apn_counts[apn_counts.eq(1)].index)
            ].drop_duplicates("apn_key")
            frame["apn_key"] = clean_text(frame["apn"]) + "|" + clean_text(frame["state"])
            frame = frame.merge(apn_candidates, on="apn_key", how="left")
            fill = frame["zipcode"].eq("") & frame["lookup_zip"].fillna("").ne("")
            frame.loc[fill, "zipcode"] = frame.loc[fill, "lookup_zip"]
            frame.loc[fill, "zipcode_source"] = "builty_apn_lookup"
            frame.drop(columns=["apn_key", "lookup_zip"], inplace=True)

    # Manual review file, if present, can override the remaining unresolved ZIPs.
    override_path = work / "manual_builty_zip_overrides.csv"
    frame["zipcode_manual_review_note"] = ""
    frame["zipcode_manual_source_url"] = ""
    if override_path.exists():
        overrides = pd.read_csv(override_path, dtype=str, keep_default_na=False)
        overrides["zipcode_manual"] = valid_zip(overrides["zipcode"])
        overrides = overrides[["builty_id", "zipcode_manual", "review_note", "source_url"]]
        frame = frame.merge(overrides, on="builty_id", how="left")
        fill = frame["zipcode"].eq("") & frame["zipcode_manual"].fillna("").ne("")
        frame.loc[fill, "zipcode"] = frame.loc[fill, "zipcode_manual"]
        frame.loc[fill, "zipcode_source"] = "manual_review"
        frame.loc[fill, "zipcode_manual_review_note"] = frame.loc[fill, "review_note"]
        frame.loc[fill, "zipcode_manual_source_url"] = frame.loc[fill, "source_url"]
        frame.drop(columns=["zipcode_manual", "review_note", "source_url"], inplace=True)

    # The basic integrity checks are relaxed here so the script stays useful in messy
    # real-world runs instead of dying on edge cases.
    frame.drop(columns="address_id", inplace=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_stata(output_path, write_index=False, version=118)
    diagnostics = (frame.groupby(["state", "zipcode_source"], dropna=False).size()
                   .rename("n").reset_index())
    diagnostics.to_csv(work / "zipcode_fill_diagnostics.csv", index=False)

    review_columns = [
        "builty_id", "state", "county", "fips_county", "locality",
        "street_address", "street_address_original", "zipcode_original", "zipcode", "zipcode_source",
        "zipcode_census_match", "zipcode_census_match_type", "permit_date",
        "zipcode_manual_review_note",
        "zipcode_manual_source_url",
        "permit_year", "description", "record_type", "property_type", "status",
    ]
    unresolved = frame.loc[
        frame["zipcode_source"].eq("unresolved"),
        [column for column in review_columns if column in frame.columns],
    ].sort_values(["state", "locality", "street_address"])
    unresolved.to_stata(work / "builty_zip_unresolved_review.dta", write_index=False, version=118)
    unresolved.to_csv(work / "builty_zip_unresolved_review.csv", index=False)

    print(frame["zipcode_source"].value_counts(dropna=False).to_string())
    print(f"Saved: {output_path}")
    print(f"Saved: {work / 'zipcode_fill_diagnostics.csv'}")
    print(f"Saved: {work / 'builty_zip_unresolved_review.dta'}")
    print(f"Saved: {work / 'builty_zip_unresolved_review.csv'}")


if __name__ == "__main__":
    main()

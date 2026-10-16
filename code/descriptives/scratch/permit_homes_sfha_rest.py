"""
Author: Anna Li
Date: 2026-10-09
Claude change 10-09: new scratch helper, run by hand (not in master.do).

Flood zone of every screened ATTOM elevation permit address, all states, from FEMA's
live NFHL map service (layer 28, S_FLD_HAZ_AR), one point query per address. Gives the
SFHA restriction for the all-state elevations-by-state figure. The pipeline's own NFHL
join (2018 state geodatabases) stays the source for FL LA TX in the analysis; this is
the current effective NFHL, so the counts for those three states differ a little.

Resume-safe: every answered point is appended to the cache CSV, and a rerun queries only
the addresses not yet in it. About 38k points at 4 threads is roughly an hour.

Reads   clean/permit_elevations_zipfilled.dta  (retrofit addresses with coordinates)
Writes  <output>/tables/permit_homes_sfha_rest_points.csv   (one row per address: zone, sfha)
        <output>/tables/permit_homes_sfha_all.csv           (by state: addresses, with
                                                             coordinates, answered, in SFHA, zone A/V)
Usage:  python permit_homes_sfha_rest.py --data <data root> --output <output folder> [--limit N] [--threads 4]
"""

from __future__ import annotations

import argparse
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"
FIELDS = "FLD_ZONE,SFHA_TF,ZONE_SUBTY,DFIRM_ID"


def query_point(session: requests.Session, key: str, lon: float, lat: float) -> dict:
    params = {"geometry": f"{lon},{lat}", "geometryType": "esriGeometryPoint", "inSR": "4326",
              "spatialRel": "esriSpatialRelIntersects", "outFields": FIELDS,
              "returnGeometry": "false", "f": "json"}
    for attempt in range(5):
        try:
            r = session.get(URL, params=params, timeout=30)
            r.raise_for_status()
            body = r.json()
            if "error" in body:
                raise RuntimeError(body["error"])
            feats = body.get("features", [])
            if not feats:
                return {"key": key, "status": "no_map", "fld_zone": "", "sfha_tf": "", "zone_subty": "", "dfirm_id": ""}
            # a point on a boundary can hit two polygons; prefer an SFHA one, then the first
            feats.sort(key=lambda f: f["attributes"].get("SFHA_TF") != "T")
            a = feats[0]["attributes"]
            return {"key": key, "status": "ok", "fld_zone": a.get("FLD_ZONE") or "", "sfha_tf": a.get("SFHA_TF") or "",
                    "zone_subty": a.get("ZONE_SUBTY") or "", "dfirm_id": a.get("DFIRM_ID") or ""}
        except Exception:
            time.sleep(2 * (attempt + 1))
    return {"key": key, "status": "failed", "fld_zone": "", "sfha_tf": "", "zone_subty": "", "dfirm_id": ""}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--limit", type=int, default=None, help="Query at most this many new points (test runs).")
    p.add_argument("--threads", type=int, default=4)
    a = p.parse_args()
    tables = Path(a.output) / "tables"
    tables.mkdir(parents=True, exist_ok=True)
    cache = tables / "permit_homes_sfha_rest_points.csv"

    d = pd.read_stata(Path(a.data) / "clean" / "permit_elevations_zipfilled.dta",
                      columns=["state", "county", "street_address", "retrofit", "latitude_permit", "longitude_permit"])
    d = d[d.retrofit == 1].copy()
    d["key"] = d.state + "|" + d.county.astype(str) + "|" + d.street_address.astype(str)
    d = d.drop_duplicates("key")
    has_xy = d.latitude_permit.notna() & d.longitude_permit.notna()
    print(f"retrofit addresses {len(d):,}, with coordinates {int(has_xy.sum()):,}", flush=True)

    done = set()
    if cache.exists():
        done = set(pd.read_csv(cache, usecols=["key", "status"]).query("status != 'failed'")["key"])
    todo = d[has_xy & ~d.key.isin(done)]
    if a.limit:
        todo = todo.head(a.limit)
    print(f"cached {len(done):,}, to query {len(todo):,}", flush=True)

    cols = ["key", "status", "fld_zone", "sfha_tf", "zone_subty", "dfirm_id"]
    new_file = not cache.exists()
    n_ok = n_fail = 0
    t0 = time.time()
    with cache.open("a", newline="") as f, requests.Session() as session, ThreadPoolExecutor(a.threads) as pool:
        w = csv.DictWriter(f, fieldnames=cols)
        if new_file:
            w.writeheader()
        futures = [pool.submit(query_point, session, k, lon, lat)
                   for k, lon, lat in zip(todo.key, todo.longitude_permit, todo.latitude_permit)]
        for i, fut in enumerate(as_completed(futures), 1):
            row = fut.result()
            w.writerow(row)
            n_ok += row["status"] != "failed"
            n_fail += row["status"] == "failed"
            if i % 500 == 0:
                f.flush()
                print(f"  {i:,} / {len(futures):,} answered, {n_fail} failed, {time.time() - t0:,.0f}s", flush=True)
    print(f"queried {n_ok + n_fail:,}: {n_ok:,} answered, {n_fail:,} failed", flush=True)

    # By state
    pts = pd.read_csv(cache).drop_duplicates("key", keep="last")
    m = d.merge(pts, on="key", how="left")
    m["answered"] = m.status.isin(["ok", "no_map"])
    m["in_sfha"] = m.sfha_tf.eq("T")
    m["zone_av"] = m.fld_zone.fillna("").astype(str).str.upper().str.match(r"^(A|V)")
    m["no_map"] = m.status.eq("no_map")
    t = (m.groupby("state")
          .agg(retrofit_addresses=("key", "size"), with_coordinates=("latitude_permit", lambda s: int(s.notna().sum())),
               answered=("answered", "sum"), in_sfha=("in_sfha", "sum"), zone_av=("zone_av", "sum"), no_map=("no_map", "sum"))
          .reset_index().rename(columns={"state": "state_abbrev"}))
    t["share_sfha_of_answered"] = (t.in_sfha / t.answered.where(t.answered > 0)).round(3)
    t = t.sort_values("retrofit_addresses", ascending=False)
    t.to_csv(tables / "permit_homes_sfha_all.csv", index=False)
    print(t.to_string(index=False))
    print(t[["retrofit_addresses", "with_coordinates", "answered", "in_sfha", "zone_av", "no_map"]].sum().to_string())


if __name__ == "__main__":
    main()

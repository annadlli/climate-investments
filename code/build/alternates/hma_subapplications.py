"""
Author: Anna Li
Date: 2026-09-21

Claude change 09-21: test script, run by hand (not in master.do). Pulls the OpenFEMA
HMA Subapplications file (FEMA GO and legacy systems: HMGP, FMA, BRIC, PDM, SRL, RFC
subapplications with their status) for the sample states, the first piece of the
funding variable: denied, not selected and withdrawn elevation applications alongside
the funded ones. Property-level sites exist only for FEMA GO (FY2020+), in
HmaSubapplicationsProjectSiteInventories (not pulled here).

Writes  raw/hma_subapplications.csv                       (every field, sample states)
        ../output/tables/hma_subapplications_status.xlsx  (state x program x status, elevation activity)

Usage: python hma_subapplications.py --data <data root> --output <output folder> [--states FL LA TX]
"""
import argparse, json, urllib.request, urllib.parse
from pathlib import Path
import pandas as pd

API = "https://www.fema.gov/api/open/v2/HmaSubapplications"

def pull(state: str) -> list[dict]:
    rows, skip = [], 0
    while True:
        q = urllib.parse.quote(f"subapplicantStateAbbreviation eq '{state}'")
        url = f"{API}?$filter={q}&$top=1000&$skip={skip}"
        with urllib.request.urlopen(url, timeout=120) as r:
            page = json.load(r).get("HmaSubapplications", [])
        rows += page
        if len(page) < 1000:
            return rows
        skip += 1000

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--states", nargs="+", default=["FL", "LA", "TX"])
    a = p.parse_args()
    df = pd.DataFrame([r for st in a.states for r in pull(st)])
    out = Path(a.data) / "raw" / "hma_subapplications.csv"
    df.to_csv(out, index=False)
    print(f"{len(df):,} subapplications in {' '.join(a.states)} saved to {out}")
    df["elevation"] = df["primaryActivity"].fillna("").str.contains("Elevation")
    df["outcome"] = df["status"].map(lambda s: "funded" if s in ("Approved", "Awarded", "Closed", "Obligated", "Subaward Accepted", "Completed")
                                     else "not funded" if s in ("Not Approved / Denied", "Not Selected", "Ineligible", "Withdrawn", "Void")
                                     else "open")
    tab = (df.groupby(["subapplicantStateAbbreviation", "program", "elevation", "outcome", "status"]).size()
             .rename("subapplications").reset_index())
    summary = (df[df.elevation].groupby(["subapplicantStateAbbreviation", "outcome"]).size()
                 .unstack(fill_value=0).reset_index())
    xlsx = Path(a.output) / "tables" / "hma_subapplications_status.xlsx"
    with pd.ExcelWriter(xlsx) as w:
        summary.to_excel(w, sheet_name="elevation_by_outcome", index=False)
        tab.to_excel(w, sheet_name="state_program_status", index=False)
    print(summary.to_string(index=False))

if __name__ == "__main__":
    main()

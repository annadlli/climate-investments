"""
Import FEMA HMA Mitigated Properties (property-level mitigation records).

Author: Vendela Norman
Date: 2026-07-10

Description:
    Pulls the full national OpenFEMA HazardMitigationAssistanceMitigatedProperties
    dataset (one row per mitigated structure) to raw/. No filtering here -- the FMA
    / elevation / single-family screens live in clean_fma_mitigated.do so the
    definition choices stay revisable.

    Note: OpenFEMA serves `zip` as an integer, so leading zeros are lost on the wire
    (05061 -> 5061). We re-pad to 5 digits on import so the raw file keeps clean ZIPs.

Source: https://www.fema.gov/api/open/v4/HazardMitigationAssistanceMitigatedProperties
    (free, no API key; paginated, 10,000 rows/request max)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import requests

ENDPOINT = "https://www.fema.gov/api/open/v4/HazardMitigationAssistanceMitigatedProperties"
ENTITY = "HazardMitigationAssistanceMitigatedProperties"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True, help="Data root; file written to {data}/raw/.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = Path(args.data) / "raw" / "hma_mitigated_properties.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    # Page through the API until a short/empty page signals the end.
    records, skip, top = [], 0, 10000
    while True:
        r = requests.get(ENDPOINT, params={"$top": top, "$skip": skip}, timeout=(30, 300))
        r.raise_for_status()
        batch = r.json()[ENTITY]
        records += batch
        print(f"  pulled {len(records):,} rows", flush=True)
        if len(batch) < top:
            break
        skip += top

    df = pd.DataFrame(records)

    # Re-pad ZIP (OpenFEMA drops the leading zero) so downstream keys stay clean.
    if "zip" in df.columns:
        df["zip"] = df["zip"].astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
        df["zip"] = df["zip"].str.zfill(5)

    df.to_csv(out, index=False)
    print(f"Saved {len(df):,} records -> {out}")


if __name__ == "__main__":
    main()

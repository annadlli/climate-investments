"""
Author: Anna Li
Date: 2026-10-02

Claude change 10-02: test script, run by hand (not in master.do). Do the ATTOM building
permits and the Builty permits identify the same elevated homes? Compares the screened
retrofit addresses of the two sources in FL LA TX, matched on the ATTOM property ID
(Builty permits carry the ATTOMID their address matched to in the 09-19 step-3 run) and
on the cleaned street address + ZIP, overall and inside county-years where both feeds
report permits. Reports the year agreement of the homes both sources find.

Reads   clean/permit_elevations_zipfilled.dta (or permit_elevations.dta), the ATTOM side
        build/nfip_attom_pipeline_v2/attom_builty/{st}_attom_permits.parquet, the Builty
            permits matched to ATTOM (09-19 run; the files stay on Dropbox)
        clean/permit_coverage_county.dta, clean/builty_coverage_county.dta
Writes  ../output/tables/attom_vs_builty_homes.xlsx (sheets summary, by_county, year_gap)

Usage: python attom_vs_builty_homes.py --data <data root> --output <output folder>
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from attom_permits import clean_address, recover_zip   # the pipeline's address scrub

STATES = ["FL", "LA", "TX"]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True)
    p.add_argument("--output", required=True)
    a = p.parse_args()
    data, out = Path(a.data), Path(a.output)

    # ATTOM permits: one row per address, retrofits only
    f = data / "clean" / "permit_elevations_zipfilled.dta"
    if not f.exists():
        f = data / "clean" / "permit_elevations.dta"
    attom = pd.read_stata(f, convert_categoricals=False)
    attom = attom[attom.retrofit == 1].copy()
    attom["attomid"] = attom.attomid.fillna("").astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    attom["addr"] = clean_address(attom.street_address, attom.locality, attom.state)
    attom["zip5"] = recover_zip(attom.zipcode, attom.street_address)
    attom["county"] = attom.fips_state.str.zfill(2) + attom.fips_county.str.zfill(3)
    attom["year"] = pd.to_numeric(attom.year, errors="coerce")

    # Builty permits as matched to ATTOM in step 3 (one row per Builty address)
    builty = pd.concat([pd.read_parquet(data / "build" / "nfip_attom_pipeline_v2" / "attom_builty" / f"{st.lower()}_attom_permits.parquet")
                        .assign(state=st) for st in STATES], ignore_index=True)
    builty = builty[pd.to_numeric(builty.RETROFIT, errors="coerce") == 1].copy()
    builty["attomid"] = builty.ATTOMID.fillna("").astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    builty.loc[builty.attom_match_tier.eq("unmatched"), "attomid"] = ""
    builty["addr"] = builty.addr_clean.fillna("")
    builty["zip5"] = builty.zip_clean.fillna("")
    builty["county"] = builty.county_fips.fillna("")
    builty["year"] = pd.to_numeric(builty.YEAR, errors="coerce")

    # county-years where each feed reports anything
    acov = pd.read_stata(data / "clean" / "permit_coverage_county.dta")[["countycode", "year"]].assign(attom_feed=1)
    bcov = pd.read_stata(data / "clean" / "builty_coverage_county.dta")[["countycode", "year"]].assign(builty_feed=1)
    for df in (attom, builty):
        df["year_i"] = df.year.fillna(-1).astype(int)
    attom = attom.merge(bcov.rename(columns={"countycode": "county", "year": "year_i"}).astype({"year_i": int}), on=["county", "year_i"], how="left")
    builty = builty.merge(acov.rename(columns={"countycode": "county", "year": "year_i"}).astype({"year_i": int}), on=["county", "year_i"], how="left")
    attom["other_feed"] = attom.builty_feed.fillna(0).astype(int)
    builty["other_feed"] = builty.attom_feed.fillna(0).astype(int)

    # overlap keys
    a_ids = set(attom.attomid[attom.attomid != ""])
    b_ids = set(builty.attomid[builty.attomid != ""])
    a_addr = set(zip(attom.state, attom.addr, attom.zip5))
    b_addr = set(zip(builty.state, builty.addr, builty.zip5))
    attom["in_builty_id"] = attom.attomid.ne("") & attom.attomid.isin(b_ids)
    attom["in_builty_addr"] = [k in b_addr for k in zip(attom.state, attom.addr, attom.zip5)]
    attom["in_builty"] = attom.in_builty_id | attom.in_builty_addr
    builty["in_attom_id"] = builty.attomid.ne("") & builty.attomid.isin(a_ids)
    builty["in_attom_addr"] = [k in a_addr for k in zip(builty.state, builty.addr, builty.zip5)]
    builty["in_attom"] = builty.in_attom_id | builty.in_attom_addr

    rows = []
    for st in STATES + ["All"]:
        A = attom if st == "All" else attom[attom.state == st]
        B = builty if st == "All" else builty[builty.state == st]
        Af, Bf = A[A.other_feed == 1], B[B.other_feed == 1]
        rows.append({
            "state": st,
            "attom_retrofit_addresses": len(A), "attom_with_attomid": int((A.attomid != "").sum()),
            "builty_retrofit_addresses": len(B), "builty_with_attomid": int((B.attomid != "").sum()),
            "both_by_attomid": int(A.in_builty_id.sum()), "both_by_address": int(A.in_builty_addr.sum()),
            "both_either": int(A.in_builty.sum()),
            "attom_only": int((~A.in_builty).sum()), "builty_only": int((~B.in_attom).sum()),
            "share_of_builty_found_in_attom": round(B.in_attom.mean(), 3) if len(B) else None,
            "share_of_attom_found_in_builty": round(A.in_builty.mean(), 3) if len(A) else None,
            "attom_in_county_years_with_builty_feed": len(Af),
            "  of which in builty": int(Af.in_builty.sum()),
            "builty_in_county_years_with_attom_feed": len(Bf),
            "  of which in attom": int(Bf.in_attom.sum()),
            "share_of_builty_found_in_attom_same_feed": round(Bf.in_attom.mean(), 3) if len(Bf) else None,
            "share_of_attom_found_in_builty_same_feed": round(Af.in_builty.mean(), 3) if len(Af) else None,
        })
    summary = pd.DataFrame(rows)

    by_county = (attom.groupby(["state", "county"]).agg(attom=("addr", "size"), both=("in_builty", "sum")).reset_index()
                 .merge(builty.groupby(["state", "county"]).agg(builty=("addr", "size")).reset_index(), how="outer")
                 .fillna(0).astype({"attom": int, "both": int, "builty": int}))
    by_county["builty_only"] = (by_county.builty - by_county.both).clip(lower=0)
    by_county = by_county.sort_values(["state", "attom"], ascending=[True, False])

    # permit-year agreement among homes in both, matched on ATTOMID
    pair = (attom[attom.in_builty_id][["state", "attomid", "year"]].rename(columns={"year": "attom_year"})
            .merge(builty[builty.attomid != ""][["attomid", "year"]].rename(columns={"year": "builty_year"}), on="attomid"))
    pair["gap"] = pair.attom_year - pair.builty_year
    year_gap = pair.groupby("state").gap.describe().reset_index()
    year_gap["share_same_year"] = pair.groupby("state").gap.apply(lambda g: (g == 0).mean()).values
    year_gap["share_within_1"] = pair.groupby("state").gap.apply(lambda g: (g.abs() <= 1).mean()).values

    xlsx = out / "tables" / "attom_vs_builty_homes.xlsx"
    with pd.ExcelWriter(xlsx) as w:
        summary.to_excel(w, sheet_name="summary", index=False)
        by_county.to_excel(w, sheet_name="by_county", index=False)
        year_gap.to_excel(w, sheet_name="year_gap", index=False)
    pd.set_option("display.width", 250)
    print(summary.set_index("state").T.to_string())
    print(year_gap.to_string(index=False))
    print(f"Saved {xlsx}")


if __name__ == "__main__":
    main()

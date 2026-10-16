"""
Authors: Anna Li
Date: 2026-09-16
Claude change 09-16: new test script (Vendela 09-15: Builty coverage at the permitting
jurisdiction, not the county). Run by hand; not in master.do until it becomes part of
the final product.

Stage 1 (--crosswalk): map each Builty permit-issuing LOCALITY to an NFIP community
    number (CID) within the state, then count permits by community x year.
    Rules, in order: (1) locality name equals an incorporated NFIP community's name
    (X, CITY OF / TOWN OF / VILLAGE OF); (2) locality name equals a county or parish
    name, to that county's community; (3) otherwise the locality's modal county's
    community (unincorporated places such as Metairie). Rule 3 is the weak one: a
    mailing city can sit in unincorporated county, and it goes to the city's CID.
Stage 2 (--compare): elevation events per 1,000 insured homes by state under the
    county-year coverage already applied in analysis.dta, and under the stricter
    rule that the home's own NFIP community reports permits that year.

Inputs : clean/builty_coverage_locality.dta, --communities csv (state, cid, cname,
         countyCode, n_ids; from the raw per-state NFIP policy files), --property-cid
         dta (state, property_id, cid), analysis/analysis.dta
Outputs: --out folder: builty_locality_cid_crosswalk.csv, builty_coverage_community.csv,
         builty_coverage_crosswalk_compare.xlsx. Checks are printed, not written.
"""
from __future__ import annotations
import argparse
import re
from pathlib import Path
import pandas as pd

STATES = ["FL", "LA", "TX"]
COUNTY_WORD = {"FL": "COUNTY", "LA": "PARISH", "TX": "COUNTY"}


def norm(s: str) -> str:
    """Upper-case, punctuation out, common abbreviations expanded."""
    if not isinstance(s, str):
        return ""
    s = s.upper().replace("&", " AND ")
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\bST\b", "SAINT", s)
    s = re.sub(r"\bFT\b", "FORT", s)
    s = re.sub(r"\bMT\b", "MOUNT", s)
    s = re.sub(r"\bPT\b", "PORT", s)
    s = re.sub(r"^(CITY|TOWN|VILLAGE) OF ", "", s.strip())
    return re.sub(r"\s+", " ", s).strip()


def parse_community(cname: str, state: str) -> tuple[str, str]:
    """Return (kind, name): kind is 'county' or 'muni'; name is the normalized place."""
    if not isinstance(cname, str):
        return ("unknown", "")
    c = cname.upper().replace("*", "").strip()
    cw = COUNTY_WORD[state]
    # consolidated city-parish / county governments carry both names
    c = re.sub(r"\s*CONSOLIDATED GOVERNMENT\s*$", "", c)
    c = re.sub(r"\s*CITY-?\s*PARISH\s*$", " " + cw, c)
    m = re.match(r"^(.*?),\s*(CITY|TOWN|VILLAGE|BOROUGH)\s+OF\s*$", c)
    if m:
        return ("muni", norm(m.group(1)))
    if re.search(rf"\b{cw}\b\s*$", c):
        base = re.sub(rf"\s*\b{cw}\b\s*$", "", c)
        # "NEW ORLEANS/ORLEANS PARISH": keep the part after the slash as the parish name
        base = base.split("/")[-1]
        return ("county", norm(base))
    return ("other", norm(c))


def build_crosswalk(data: Path, communities: Path, out: Path) -> pd.DataFrame:
    loc = pd.read_stata(data / "clean" / "builty_coverage_locality.dta", convert_categoricals=False)
    loc = loc[loc.state.isin(STATES)].copy()
    loc["key"] = loc.locality.map(norm)
    com = pd.read_csv(communities, dtype={"cid": str, "countyCode": str, "state": str, "cname": str})
    com["n_ids"] = com.n_ids.astype(int)
    com = com[com.state.isin(STATES)]
    # one row per CID: modal name and county by insured count
    com = (com.sort_values("n_ids", ascending=False)
              .groupby(["state", "cid"], as_index=False)
              .agg(cname=("cname", "first"), countycode=("countyCode", "first"), n_ids=("n_ids", "sum")))
    parsed = com.apply(lambda r: parse_community(r.cname, r.state), axis=1, result_type="expand")
    com["kind"], com["key"] = parsed[0], parsed[1]
    print("communities:", com.groupby(["state", "kind"]).size().unstack(fill_value=0).to_string())

    muni = com[com.kind == "muni"].drop_duplicates(["state", "key"], keep="first")[["state", "key", "cid"]]
    muni = muni.rename(columns={"cid": "cid_muni"})
    county_by_name = com[com.kind == "county"].drop_duplicates(["state", "key"], keep="first")[["state", "key", "cid"]]
    county_by_name = county_by_name.rename(columns={"cid": "cid_cntyname"})
    # county community by county FIPS (largest county-kind CID in the county)
    county_by_fips = (com[com.kind == "county"].sort_values("n_ids", ascending=False)
                        .drop_duplicates(["state", "countycode"])[["state", "countycode", "cid"]]
                        .rename(columns={"cid": "cid_cntyfips"}))

    # locality-level table (one row per state x locality) for the rules
    lk = (loc.groupby(["state", "locality", "key"], as_index=False)
             .agg(countycode=("countycode", "first"), n_permits=("builty_n_permits", "sum")))
    lk = lk.merge(muni, on=["state", "key"], how="left")
    lk = lk.merge(county_by_name, on=["state", "key"], how="left")
    lk = lk.merge(county_by_fips, on=["state", "countycode"], how="left")
    lk["cid"] = lk.cid_muni.fillna(lk.cid_cntyname).fillna(lk.cid_cntyfips)
    lk["rule"] = pd.Series(pd.NA, index=lk.index, dtype="object")
    lk.loc[lk.cid_muni.notna(), "rule"] = "1 municipal name"
    lk.loc[lk.cid_muni.isna() & lk.cid_cntyname.notna(), "rule"] = "2 county name"
    lk.loc[lk.cid_muni.isna() & lk.cid_cntyname.isna() & lk.cid_cntyfips.notna(), "rule"] = "3 county of locality"
    lk.loc[lk.cid.isna(), "rule"] = "4 unmatched"
    tab = lk.groupby(["state", "rule"]).agg(localities=("locality", "size"), permits=("n_permits", "sum"))
    tab["permit_share"] = tab.permits / tab.groupby(level=0).permits.transform("sum")
    print("match rules (localities, permits):\n" + tab.to_string())
    print("largest unmatched localities:\n" +
          lk[lk.rule == "4 unmatched"].sort_values("n_permits", ascending=False)
            .head(15)[["state", "locality", "countycode", "n_permits"]].to_string(index=False))
    out.mkdir(parents=True, exist_ok=True)
    lk[["state", "locality", "key", "countycode", "n_permits", "cid", "rule"]].to_csv(
        out / "builty_locality_cid_crosswalk.csv", index=False)

    # community x year permit counts
    cov = loc.merge(lk[["state", "locality", "cid"]], on=["state", "locality"], how="left")
    cov = cov[cov.cid.notna()].groupby(["state", "cid", "year"], as_index=False).agg(
        builty_n_permits=("builty_n_permits", "sum"), n_localities=("locality", "nunique"))
    cov.to_csv(out / "builty_coverage_community.csv", index=False)
    print(f"community-years with permits: {len(cov):,}; communities: {cov.cid.nunique():,}")
    return cov


def compare(data: Path, property_cid: Path, cov: pd.DataFrame, out: Path) -> None:
    pc = pd.read_stata(property_cid, convert_categoricals=False)
    pc["cid"] = pc.cid.astype(str).str.strip()
    an = pd.read_stata(data / "analysis" / "analysis.dta", convert_categoricals=False,
                       columns=["state", "property_id", "policy_year", "elevation_retrofit",
                                "elevation_year", "elevation_source"])
    an = an.merge(pc, on=["state", "property_id"], how="left")
    print("analysis property-years without a CID:", f"{an.cid.isna().mean():.3%}")
    cov = cov.rename(columns={"year": "policy_year"})
    cov["policy_year"] = cov.policy_year.astype(int)
    an = an.merge(cov[["state", "cid", "policy_year", "builty_n_permits"]], on=["state", "cid", "policy_year"], how="left")
    an["covered_juris"] = an.builty_n_permits.fillna(0) > 0
    an["event"] = (an.elevation_retrofit == 1) & (an.policy_year == an.elevation_year)
    an["event_builty"] = an.event & (an.elevation_source == 3)
    rows = []
    for label, sub in [("county coverage (analysis.dta as is)", an), ("jurisdiction coverage", an[an.covered_juris])]:
        g = sub.groupby("state").agg(homes=("property_id", "nunique"), property_years=("property_id", "size"),
                                     events=("event", "sum"), events_builty=("event_builty", "sum"))
        g["events_per_1000_homes"] = 1000 * g.events / g.homes
        g["builty_events_per_1000_homes"] = 1000 * g.events_builty / g.homes
        g.insert(0, "coverage", label)
        rows.append(g.reset_index())
    res = pd.concat(rows, ignore_index=True)
    print(res.to_string(index=False))
    res.to_excel(out / "builty_coverage_crosswalk_compare.xlsx", index=False)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", required=True)
    p.add_argument("--communities", required=True, help="csv: state, cid, cname, countyCode, n_policy_years, n_ids")
    p.add_argument("--property-cid", default=None, help="dta: state, property_id, cid (needed for --compare)")
    p.add_argument("--out", required=True)
    p.add_argument("--crosswalk", action="store_true")
    p.add_argument("--compare", action="store_true")
    a = p.parse_args()
    data, out = Path(a.data), Path(a.out)
    run_all = not (a.crosswalk or a.compare)
    cov = None
    if a.crosswalk or run_all:
        cov = build_crosswalk(data, Path(a.communities), out)
    if a.compare or run_all:
        if cov is None:
            cov = pd.read_csv(out / "builty_coverage_community.csv", dtype={"cid": str, "state": str})
        compare(data, Path(a.property_cid), cov, out)


if __name__ == "__main__":
    main()

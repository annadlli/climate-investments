"""
Author: Vendela Norman
Date: 2026-09-23
Claude change 09-23: new scratch script (Builty coverage grain and sample rules).

Builty permit coverage at the NFIP community and ZIP grain, and the prevalence of
elevation events in analysis.dta under alternative sample rules (county, community,
ZIP, each from the first substantive reporting year onward, with and without the
ATTOM match). Permits are attributed to NFIP communities without name matching:
a permit with a ZIP is split across communities by the NFIP policy shares of that
ZIP; a permit without a ZIP takes its locality's ZIP mix; the remainder falls to a
harmonized name match, else the modal county's county-level community. Prints the
name-route match shares, the name-vs-ZIP disagreements, the rule table and the
top communities by events. Reads the analysis file, the panel, the link files, the
raw Builty parquet and the raw per-state policy CSVs. See TODO.md 09-23.

Run by hand (about 20 minutes, mostly the policy CSV scans):
    python3 code/descriptives/scratch/builty_coverage_community.py --data <Data root> --states "FL LA TX" --work <scratch dir>
"""

import argparse
import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyreadstat

p = argparse.ArgumentParser()
p.add_argument("--data", required=True, help="Data root (from master.do).")
p.add_argument("--states", default="FL LA TX")
p.add_argument("--work", required=True, help="Directory for intermediate parquet files.")
p.add_argument("--comm-floor", type=int, default=20, help="Expected permits for a community-year to count.")
p.add_argument("--zip-floor", type=int, default=10, help="Permits for a ZIP-year to count.")
p.add_argument("--county-floor", type=int, default=50, help="Permits for a county-year to count.")
args = p.parse_args()
data = Path(args.data)
work = Path(args.work)
work.mkdir(parents=True, exist_ok=True)
states = args.states.upper().split()
pd.set_option("display.width", 300)
pd.set_option("display.max_columns", 40)

# -----------------------------------------------------------------------------
# Section 1: NFIP communities (cid, name, county, ZIP shares) from the raw policy files
# -----------------------------------------------------------------------------

con = duckdb.connect()
for st in states:
    out = work / f"nfip_comm_{st.lower()}.parquet"
    if out.exists():
        continue
    con.execute(f"""
        COPY (SELECT '{st}' AS state, nfipRatedCommunityNumber AS cid, nfipCommunityName AS cname,
                     countyCode AS countycode, reportedZipCode AS zipcode, count(*) AS n
              FROM read_csv('{data}/clean/nfip_policies_raw/{st.lower()}.csv', header=true, all_varchar=true, ignore_errors=true)
              GROUP BY 1, 2, 3, 4, 5)
        TO '{out}' (FORMAT PARQUET)""")
comm = pd.concat([pd.read_parquet(work / f"nfip_comm_{st.lower()}.parquet") for st in states])
comm["cid"] = comm.cid.astype(str).str.strip().str.zfill(6)
comm["zipcode"] = comm.zipcode.astype(str).str.strip().str[:5]
comm = comm[comm.cid.str.match(r"^\d{6}$")]

inlist = ", ".join(f"'{s}'" for s in states)
perm_file = work / "permits_loc_zip_year.parquet"
if not perm_file.exists():
    con.execute(f"""
        COPY (SELECT STATE AS state, upper(trim(LOCALITY)) AS locality,
                     lpad(nullif(trim(ZIPCODE), ''), 5, '0') AS zipcode,
                     CASE WHEN nullif(trim(FIPS_COUNTY), '') IS NOT NULL
                          THEN lpad(trim(FIPS_STATE), 2, '0') || lpad(trim(FIPS_COUNTY), 3, '0') END AS countycode,
                     try_cast(substr(coalesce(nullif(DATE_ISSUED, ''), nullif(DATE_SUBMITTED, ''),
                                              nullif(DATE_FINALED, '')), 1, 4) AS INTEGER) AS yr,
                     count(*) AS n
              FROM read_parquet('{data}/raw/builty_all.parquet') WHERE STATE IN ({inlist})
              GROUP BY 1, 2, 3, 4, 5)
        TO '{perm_file}' (FORMAT PARQUET)""")
perm = pd.read_parquet(perm_file)
perm = perm[perm.yr.between(1990, 2026)]

# -----------------------------------------------------------------------------
# Section 2: Locality -> community crosswalk (name route for diagnostics, ZIP route for use)
# -----------------------------------------------------------------------------

cl = comm.groupby(["state", "cid", "cname"]).n.sum().reset_index().sort_values("n", ascending=False).drop_duplicates(["state", "cid"])
ccty = comm.groupby(["state", "cid", "countycode"]).n.sum().reset_index().sort_values("n", ascending=False).drop_duplicates(["state", "cid"])
cl = cl.merge(ccty[["state", "cid", "countycode"]].rename(columns={"countycode": "ccounty"}), on=["state", "cid"])

ABBREV = {"ST": "SAINT", "STE": "SAINTE", "FT": "FORT", "MT": "MOUNT", "PT": "PORT", "BCH": "BEACH", "N": "NORTH",
          "S": "SOUTH", "E": "EAST", "W": "WEST", "HTS": "HEIGHTS", "SPGS": "SPRINGS", "ISL": "ISLAND", "LK": "LAKE",
          "PK": "PARK", "VLG": "VILLAGE", "TWP": "TOWNSHIP", "JCT": "JUNCTION", "HBR": "HARBOR", "HARBOUR": "HARBOR",
          "CTR": "CENTER", "CENTRE": "CENTER", "PLS": "PLAINS", "GDNS": "GARDENS", "SHRS": "SHORES", "CO": "COUNTY"}


def norm(s):
    s = str(s).upper().replace("*", "")
    s = re.sub(r",\s*(CITY|TOWN|VILLAGE|BOROUGH|TOWNSHIP|PARISH|COUNTY)\s+OF\s*$", "", s)
    s = re.sub(r"^(CITY|TOWN|VILLAGE|BOROUGH|TOWNSHIP)\s+OF\s+", "", s)
    s = re.sub(r"\s*\((UNINCORPORATED|UNINC\.?|UNINCORP\.?)[^)]*\)", "", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return " ".join(ABBREV.get(t, t) for t in s.split())


def core(s):
    return s.str.replace(r"\b(COUNTY|PARISH|UNINCORPORATED AREAS?)\b", "", regex=True).str.strip()


cl["is_county"] = cl.cname.str.contains(r"\*|COUNTY|PARISH", regex=True)
cl["nname"] = cl.cname.map(norm)
cl["nname_core"] = core(cl.nname)

loc = perm.groupby(["state", "locality"]).n.sum().reset_index().dropna()
loc["nname"] = loc.locality.map(norm)
loc["nname_core"] = core(loc.nname)
lc = (perm.dropna(subset=["countycode"]).groupby(["state", "locality", "countycode"]).n.sum().reset_index()
      .sort_values("n", ascending=False).drop_duplicates(["state", "locality"]).rename(columns={"countycode": "lcounty"}))
loc = loc.merge(lc[["state", "locality", "lcounty"]], on=["state", "locality"], how="left")

# name route: municipal name (same county preferred), else county name
muni = cl[~cl.is_county][["state", "cid", "nname", "ccounty"]]
m1 = loc.merge(muni, on=["state", "nname"], how="left")
m1["same_cty"] = m1.ccounty == m1.lcounty
m1 = m1.sort_values(["state", "locality", "same_cty"], ascending=[True, True, False]).drop_duplicates(["state", "locality"])
loc["cid_name"] = m1.set_index(["state", "locality"]).loc[list(zip(loc.state, loc.locality)), "cid"].values
loc["rule"] = np.where(loc.cid_name.notna(), "1 municipal name", None)
cty = cl[cl.is_county][["state", "cid", "nname_core"]]
m2 = loc.merge(cty, on=["state", "nname_core"], how="left").drop_duplicates(["state", "locality"])
sel = loc.cid_name.isna() & m2.cid.notna().values
loc.loc[sel, "cid_name"] = m2.cid.values[sel]
loc.loc[sel, "rule"] = "2 county name"

# ZIP route: P(cid | state, zip) from NFIP policies, P(zip | locality) from permits
pz = comm.groupby(["state", "zipcode", "cid"]).n.sum().reset_index()
pz["p_cid_zip"] = pz.n / pz.groupby(["state", "zipcode"]).n.transform("sum")
lz = perm.dropna(subset=["zipcode"]).groupby(["state", "locality", "zipcode"]).n.sum().reset_index()
lz["p_zip_loc"] = lz.n / lz.groupby(["state", "locality"]).n.transform("sum")
lzc = lz.merge(pz[["state", "zipcode", "cid", "p_cid_zip"]], on=["state", "zipcode"])
lzc["w"] = lzc.p_zip_loc * lzc.p_cid_zip
lw = lzc.groupby(["state", "locality", "cid"]).w.sum().reset_index()
lw["wsum"] = lw.groupby(["state", "locality"]).w.transform("sum")
top = lw.sort_values("w", ascending=False).drop_duplicates(["state", "locality"]).rename(columns={"cid": "cid_zip", "w": "w_zip"})
loc = loc.merge(top[["state", "locality", "cid_zip", "w_zip"]], on=["state", "locality"], how="left")
ctyc = (cl[cl.is_county].sort_values("n", ascending=False).drop_duplicates(["state", "ccounty"])[["state", "ccounty", "cid"]]
        .rename(columns={"ccounty": "lcounty", "cid": "cid_cty"}))
loc = loc.merge(ctyc, on=["state", "lcounty"], how="left")
loc["agree"] = np.where(loc.cid_name.notna() & loc.cid_zip.notna(), loc.cid_name == loc.cid_zip, np.nan)


def wshare(df, flag):
    return df.groupby("state").apply(lambda d: (d.n * flag.loc[d.index]).sum() / d.n.sum(), include_groups=False).round(3)


print("share of permits matched by name (municipal or county rule):")
print(wshare(loc, loc.cid_name.notna()).to_string())
print("share of permits with a ZIP-route community:")
print(wshare(loc, loc.cid_zip.notna()).to_string())
both = loc[loc.agree.notna()]
print("name vs ZIP route agreement, permit-weighted:")
print(wshare(both, both.agree.astype(float)).to_string())
names = cl[["state", "cid", "cname"]]
d = both[both.agree == 0].sort_values("n", ascending=False).head(20)
d = (d.merge(names.rename(columns={"cid": "cid_name", "cname": "name_match"}), on=["state", "cid_name"], how="left")
      .merge(names.rename(columns={"cid": "cid_zip", "cname": "zip_match"}), on=["state", "cid_zip"], how="left"))
print("\nlargest name-vs-ZIP disagreements:")
print(d[["state", "locality", "n", "name_match", "zip_match", "w_zip"]].to_string(index=False))
u = loc[loc.cid_name.isna()].sort_values("n", ascending=False).head(20)
u = u.merge(names.rename(columns={"cid": "cid_zip", "cname": "zip_match"}), on=["state", "cid_zip"], how="left")
print("\nlargest localities unmatched by name:")
print(u[["state", "locality", "n", "lcounty", "zip_match", "w_zip"]].to_string(index=False))

# -----------------------------------------------------------------------------
# Section 3: Expected permits by community x year, and coverage first years
# -----------------------------------------------------------------------------

w1 = perm.dropna(subset=["zipcode"]).merge(pz[["state", "zipcode", "cid", "p_cid_zip"]], on=["state", "zipcode"])
w1["e"] = w1.n * w1.p_cid_zip
nz = perm[perm.zipcode.isna()]
lwn = lw.copy()
lwn["w"] = lwn.w / lwn.wsum
w2 = nz.merge(lwn[["state", "locality", "cid", "w"]], on=["state", "locality"])
w2["e"] = w2.n * w2.w
rest = nz.merge(lwn[["state", "locality"]].drop_duplicates(), on=["state", "locality"], how="left", indicator=True)
rest = rest[rest._merge == "left_only"].drop(columns="_merge")
rest = rest.merge(loc[["state", "locality", "cid_name", "cid_cty"]], on=["state", "locality"], how="left")
rest["cid"] = rest.cid_name.fillna(rest.cid_cty)
rest["e"] = rest.n
print(f"\npermit attribution: by own ZIP {int(w1.e.sum()):,}; by locality ZIP mix {int(w2.e.sum()):,}; "
      f"by name or county fallback {int(rest.e.sum()):,}; unassigned {int(rest.loc[rest.cid.isna(), 'n'].sum()):,}")
E = (pd.concat([w1[["state", "cid", "yr", "e"]], w2[["state", "cid", "yr", "e"]], rest.dropna(subset=["cid"])[["state", "cid", "yr", "e"]]])
     .groupby(["state", "cid", "yr"]).e.sum().reset_index())

# -----------------------------------------------------------------------------
# Section 4: Analysis sample with county, community and ZIP, and the rule table
# -----------------------------------------------------------------------------

a, _ = pyreadstat.read_dta(str(data / "analysis" / "analysis.dta"),
                           usecols=["state", "property_id", "policy_year", "elevation_source", "elevation_retrofit",
                                    "elevation_year", "attom_matched", "zipcode"])
ids, _ = pyreadstat.read_dta(str(data / "clean" / "nfip_policies_panel.dta"),
                             usecols=["state", "property_id", "property_id_state"])
ids = ids.drop_duplicates(["state", "property_id"])
links = pd.concat([pyreadstat.read_dta(str(data / "build" / "nfip_attom_property" / f"{st.lower()}_nfip_attom_property.dta"),
                                       usecols=["state", "property_id_state", "nfipratedcommunitynumber", "countycode"])[0]
                   for st in states])
links["cid"] = links.nfipratedcommunitynumber.astype(str).str.strip().str.zfill(6)
ids = ids.merge(links[["state", "property_id_state", "cid", "countycode"]], on=["state", "property_id_state"], how="left")
a = a.merge(ids[["state", "property_id", "cid", "countycode"]], on=["state", "property_id"], how="left")
a["zipcode"] = a.zipcode.astype(str).str.strip().str[:5]

homes = a.groupby(["state", "cid"]).property_id.nunique().rename("nfip_homes").reset_index()
E = E.merge(homes, on=["state", "cid"], how="left")
E["per100"] = 100 * E.e / E.nfip_homes
fc = E[(E.e >= args.comm_floor) & (E.per100 >= 1)].groupby(["state", "cid"]).yr.min().rename("cfirst").reset_index()
Z = perm.dropna(subset=["zipcode"]).groupby(["state", "zipcode", "yr"]).n.sum().reset_index()
fz = Z[Z.n >= args.zip_floor].groupby(["state", "zipcode"]).yr.min().rename("zfirst").reset_index()
C = perm.dropna(subset=["countycode"]).groupby(["state", "countycode", "yr"]).n.sum().reset_index()
fk = C[C.n >= args.county_floor].groupby(["state", "countycode"]).yr.min().rename("kfirst").reset_index()
a = (a.merge(fc, on=["state", "cid"], how="left").merge(fz, on=["state", "zipcode"], how="left")
      .merge(fk, on=["state", "countycode"], how="left"))
a["cov_cty"] = (a.policy_year >= a.kfirst).astype(int)
a["cov_comm"] = (a.policy_year >= a.cfirst).astype(int)
a["cov_zip"] = (a.policy_year >= a.zfirst).astype(int)
a["is_event"] = (a.elevation_retrofit == 1) & (a.policy_year == a.elevation_year)
a["is_builty"] = a.elevation_source == 3
a["builty_event"] = a.is_builty & a.is_event


def summ(mask, name):
    s = a[mask]
    out = []
    for st, g in list(s.groupby("state")) + [("ALL", s)]:
        n_prop = g.property_id.nunique()
        bprop = g.loc[g.is_builty, "property_id"].nunique()
        anyprop = g.loc[g.elevation_retrofit == 1, "property_id"].nunique()
        bev = int(g.builty_event.sum())
        out.append(dict(rule=name, state=st, prop_years=len(g), properties=n_prop, builty_events=bev,
                        builty_per1000=1000 * bev / n_prop, builty_pct_props=100 * bprop / n_prop,
                        any_per1000=1000 * g.is_event.sum() / n_prop, any_pct_props=100 * anyprop / n_prop,
                        matched_pct=100 * g.attom_matched.mean()))
    return pd.DataFrame(out)


m = a.attom_matched == 1
rules = [(a.state.notna(), "all SFHA"),
         (a.cov_cty == 1, f"county from first {args.county_floor}+ yr"),
         (a.cov_comm == 1, "community from first substantive yr"),
         (a.cov_zip == 1, f"ZIP from first {args.zip_floor}+ yr"),
         ((a.cov_comm == 1) & (a.cov_zip == 1), "community AND zip"),
         ((a.cov_cty == 1) & m, "county & matched"),
         ((a.cov_comm == 1) & m, "community & matched"),
         ((a.cov_comm == 1) & (a.cov_zip == 1) & m, "community AND zip & matched"),
         ((a.cov_comm == 1) & (a.cov_zip == 1) & m & (a.policy_year >= 2013), "community AND zip & matched & 2013+")]
res = pd.concat([summ(mm, n) for mm, n in rules])
print("\n" + res.round(3).to_string(index=False))
res.to_csv(work / "prevalence_rules.csv", index=False)
print("\nBuilty events outside community coverage:", int(a[a.builty_event & (a.cov_comm == 0)].shape[0]),
      "outside ZIP coverage:", int(a[a.builty_event & (a.cov_zip == 0)].shape[0]), "of", int(a.builty_event.sum()))
g = a[(a.cov_comm == 1) & m].groupby(["state", "cid"]).agg(props=("property_id", "nunique"), ev=("builty_event", "sum")).reset_index()
g["per1000"] = 1000 * g.ev / g.props
g = g.merge(names, on=["state", "cid"], how="left")
print("\ntop communities by Builty events (community-covered and matched):")
print(g.sort_values("ev", ascending=False).head(20).round(2).to_string(index=False))
print("share of covered matched properties in communities with zero events:",
      round(g.loc[g.ev == 0, "props"].sum() / g.props.sum(), 3))
E.to_parquet(work / "community_year_expected_permits.parquet")
loc.to_parquet(work / "locality_crosswalk.parquet")

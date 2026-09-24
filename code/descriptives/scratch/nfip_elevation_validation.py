"""
Author: Vendela Norman
Date: 2026-09-23
Claude change 09-23: new scratch script (is the NFIP elevation flag a real elevation?).

Two checks on the three elevation sources in analysis.dta (NFIP flag flip, ICC
payment, Builty permit). (1) Year- and state-matched within-property changes in
log premium, log ATTOM value and the any-claim rate, post years 1-3 minus pre
years 3-1, net of the same change among never-retrofit homes. (2) The elevation
certificate fields in the raw policy files (elevationDifference,
lowestFloorElevation, elevationCertificateIndicator), which
clean_nfip_policies.do drops, tracked around each event for the event properties:
a real elevation raises the lowest floor by several feet. The raw property id is
rebuilt as block group | construction date | NB date, as in clean_nfip_policies.do.
See TODO.md 09-23.

Run by hand (about 15 minutes, mostly the policy CSV scans):
    python3 code/descriptives/scratch/nfip_elevation_validation.py --data <Data root> --states "FL LA TX" --work <scratch dir>
"""

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyreadstat

p = argparse.ArgumentParser()
p.add_argument("--data", required=True, help="Data root (from master.do).")
p.add_argument("--states", default="FL LA TX")
p.add_argument("--work", required=True, help="Directory for intermediate parquet files.")
args = p.parse_args()
data = Path(args.data)
work = Path(args.work)
work.mkdir(parents=True, exist_ok=True)
states = args.states.upper().split()
pd.set_option("display.width", 250)

# -----------------------------------------------------------------------------
# Section 1: Year- and state-matched within-property changes
# -----------------------------------------------------------------------------

a, _ = pyreadstat.read_dta(str(data / "analysis" / "analysis.dta"),
                           usecols=["state", "property_id", "policy_year", "elevation_source", "elevation_retrofit",
                                    "elevation_year", "premium", "property_value", "claim", "post_firm",
                                    "construction_year", "risk_rating_2"])
a["lp"] = np.log(a.premium.where(a.premium > 0))
a["lv"] = np.log(a.property_value.where(a.property_value > 0))
a["anyclaim"] = (a.claim > 0).astype(float)
VARS = ["lp", "lv", "anyclaim"]


def change(df, k):
    pre = df[(k >= -3) & (k <= -1)].groupby(["state", "property_id"])[VARS].mean()
    post = df[(k >= 1) & (k <= 3)].groupby(["state", "property_id"])[VARS].mean()
    return (post - pre).dropna(how="all").reset_index()


ctrl = a[a.elevation_retrofit == 0]
rows = []
for y in range(2011, 2023):
    d = change(ctrl, ctrl.policy_year - y)
    for st, g in d.groupby("state"):
        rows.append(dict(state=st, elevation_year=y, c_lp=g.lp.mean(), c_lv=g.lv.mean(), c_ac=g.anyclaim.mean()))
C = pd.DataFrame(rows)
ev = a[a.elevation_retrofit == 1].copy()
ev["k"] = ev.policy_year - ev.elevation_year
pre = ev[(ev.k >= -3) & (ev.k <= -1)].groupby(["elevation_source", "state", "elevation_year", "property_id"])[VARS].mean()
post = ev[(ev.k >= 1) & (ev.k <= 3)].groupby(["elevation_source", "state", "elevation_year", "property_id"])[VARS].mean()
d = (post - pre).dropna(how="all").reset_index().merge(C, on=["state", "elevation_year"], how="left")
d["did_lp"] = d.lp - d.c_lp
d["did_lv"] = d.lv - d.c_lv
d["did_ac"] = d.anyclaim - d.c_ac
se = lambda s: s.std() / np.sqrt(s.count())
print("year- and state-matched within-property change, post(1..3) minus pre(-3..-1), net of never-retrofit homes")
print("(elevation_source 1 = NFIP flag flip, 2 = ICC payment, 3 = Builty permit)")
print(d.groupby("elevation_source").agg(n=("property_id", "size"), did_lp=("did_lp", "mean"), se_lp=("did_lp", se),
                                        did_lv=("did_lv", "mean"), se_lv=("did_lv", se), n_lv=("did_lv", "count"),
                                        did_ac=("did_ac", "mean"), se_ac=("did_ac", se)).round(4).to_string())
print("\nby state:")
print(d.groupby(["elevation_source", "state"]).agg(n=("did_lv", "count"), did_lv=("did_lv", "mean"),
                                                   did_lp=("did_lp", "mean"), did_ac=("did_ac", "mean")).round(3).to_string())
print("\ncomposition in the event year:")
print(ev[ev.k == 0].groupby("elevation_source").agg(n=("property_id", "size"), post_firm=("post_firm", "mean"),
                                                    constr_med=("construction_year", "median"),
                                                    rr2=("risk_rating_2", "mean")).round(3).to_string())

# -----------------------------------------------------------------------------
# Section 2: Elevation certificate fields around each event, from the raw policy files
# -----------------------------------------------------------------------------

panel, _ = pyreadstat.read_dta(str(data / "clean" / "nfip_policies_panel.dta"),
                               usecols=["property_id", "censusblockgroupfips", "originalconstructiondate", "originalnbdate"])
panel = panel.drop_duplicates("property_id")
fmt = lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else ""   # a few dates are outside pandas' range
panel["pid"] = (panel.censusblockgroupfips.astype(str).str.strip() + "|" + panel.originalconstructiondate.map(fmt)
                + "|" + panel.originalnbdate.map(fmt))
events = ev[ev.k == 0][["property_id", "elevation_source", "elevation_year"]].merge(panel[["property_id", "pid"]], on="property_id")
events.to_parquet(work / "event_pids.parquet")

con = duckdb.connect()
con.execute(f"CREATE TABLE ev AS SELECT * FROM '{work / 'event_pids.parquet'}'")
for st in states:
    out = work / f"elevfields_events_{st.lower()}.parquet"
    if out.exists():
        continue
    con.execute(f"""
        CREATE OR REPLACE TABLE py AS
        SELECT censusBlockGroupFips || '|' || substr(originalConstructionDate, 1, 10) || '|' || substr(originalNBDate, 1, 10) AS pid,
               try_cast(substr(policyEffectiveDate, 1, 4) AS INTEGER) AS yr,
               max(try_cast(elevatedBuildingIndicator AS INTEGER)) AS elev,
               median(try_cast(elevationDifference AS DOUBLE)) AS elevdiff,
               median(try_cast(lowestFloorElevation AS DOUBLE)) AS lfe,
               median(try_cast(baseFloodElevation AS DOUBLE)) AS bfe,
               max(try_cast(elevationCertificateIndicator AS INTEGER)) AS ec,
               median(try_cast(numberOfFloorsInInsuredBuilding AS DOUBLE)) AS floors
        FROM read_csv('{data}/clean/nfip_policies_raw/{st.lower()}.csv', header=true, all_varchar=true, ignore_errors=true)
        WHERE occupancyType IN ('1', '11') AND censusBlockGroupFips <> '' AND originalConstructionDate <> '' AND originalNBDate <> ''
        GROUP BY 1, 2""")
    con.execute(f"COPY (SELECT ev.property_id, ev.elevation_source, ev.elevation_year, py.* FROM py JOIN ev USING (pid)) TO '{out}' (FORMAT PARQUET)")
m = pd.concat([pd.read_parquet(work / f"elevfields_events_{st.lower()}.parquet") for st in states])
m["k"] = m.yr - m.elevation_year
print("\nmatched event properties to raw elevation fields:", m.property_id.nunique(), "of", events.property_id.nunique())
print("\nelevation fields by event time (medians; miss = share elevationDifference missing; elev = share flagged elevated):")
print(m[(m.k >= -3) & (m.k <= 3)].groupby(["elevation_source", "k"])
      .agg(n=("property_id", "size"), elevdiff_med=("elevdiff", "median"), elevdiff_mean=("elevdiff", "mean"),
           miss=("elevdiff", lambda s: s.isna().mean()), lfe_med=("lfe", "median"), bfe_med=("bfe", "median"),
           ec=("ec", "mean"), elev=("elev", "mean")).round(2).to_string())
for src in [1, 2, 3]:
    f = m[m.elevation_source == src]
    for var in ["elevdiff", "lfe"]:
        pre = f[(f.k >= -3) & (f.k <= -1)].groupby("property_id")[var].mean()
        post = f[(f.k >= 1) & (f.k <= 3)].groupby("property_id")[var].mean()
        dd = (post - pre).dropna()
        print(f"source {src} {var}: within-property change post(1..3) - pre(-3..-1): n {len(dd)} mean {dd.mean():.2f} "
              f"median {dd.median():.2f} share >= +3 ft {(dd >= 3).mean():.3f} share >= +1 ft {(dd >= 1).mean():.3f} "
              f"unchanged {(dd == 0).mean():.3f} fell {(dd < 0).mean():.3f}")

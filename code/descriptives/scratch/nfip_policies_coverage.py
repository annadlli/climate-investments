"""
Author: Anna Li
Date: 2026-09-21
Claude change 09-21: new scratch script (Setting slide).

NFIP policies and coverage by state for one effective year, all states, from the
raw national OpenFEMA policy file raw/FimaNfipPoliciesV2.csv (31 GB; must be
available offline in Dropbox). Policies are policyCount summed (condo and
multi-unit policies count each unit); coverage is building + contents, nominal
dollars. For the "Setting" slide. Prints the national total and writes
output/tables/nfip_policies_coverage_<year>.xlsx, one row per state.

Run by hand (about ten minutes):
    python3 code/descriptives/scratch/nfip_policies_coverage.py --data <Data root> --year 2025 --output output
"""

import argparse
from pathlib import Path

import duckdb

p = argparse.ArgumentParser()
p.add_argument("--data", required=True, help="Data root (from master.do).")
p.add_argument("--year", type=int, default=2025)
p.add_argument("--output", default="output", help="Output root.")
args = p.parse_args()
raw = Path(args.data) / "raw" / "FimaNfipPoliciesV2.csv"

con = duckdb.connect()
con.execute("SET threads=4; SET memory_limit='6GB';")
df = con.execute(f"""
    SELECT propertyState AS state,
           count(*) AS records,
           sum(COALESCE(policyCount, 1)) AS policies,
           sum(COALESCE(totalBuildingInsuranceCoverage, 0)) AS coverage_building,
           sum(COALESCE(totalBuildingInsuranceCoverage, 0) + COALESCE(totalContentsInsuranceCoverage, 0)) AS coverage
    FROM read_csv('{raw}', ignore_errors=true,
         types={{'policyEffectiveDate': 'VARCHAR', 'policyCount': 'DOUBLE',
                 'totalBuildingInsuranceCoverage': 'DOUBLE', 'totalContentsInsuranceCoverage': 'DOUBLE',
                 'propertyState': 'VARCHAR'}})
    WHERE year(CAST(policyEffectiveDate AS DATE)) = {args.year}
    GROUP BY 1 ORDER BY policies DESC
""").df()

tot = df[["records", "policies", "coverage_building", "coverage"]].sum()
print(f"{args.year}, all states: {tot.records:,.0f} records, {tot.policies:,.0f} policies, "
      f"${tot.coverage/1e12:.2f} trillion coverage (${tot.coverage_building/1e12:.2f} trillion building)")
print(df.head(10).to_string(index=False))
out = Path(args.output) / "tables" / f"nfip_policies_coverage_{args.year}.xlsx"
df.to_excel(out, index=False)
print(f"wrote {out}")

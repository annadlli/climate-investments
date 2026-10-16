"""
Authors: Claude (for Anna Li)
Date: 2026-10-15
Claude change 10-15 (new file; Anna: "downloaded residential and new construction. use it").

One-off attach of ATTOM's classifier labels to the permit files already on Dropbox, plus the
comparison with the text screen. The pipeline path for the classifiers is extract_attom_permits.py
step 2 (cluster: the status tables live only there); this script exists because the classifier
download (RESIDENTIAL, NEW CONSTRUCTION, NEW CONSTRUCTION (BETA) only; 178M rows, 133M permits,
national) arrived on the laptop after the cluster run. Scratch script, run by hand, not in master.do.

What it does, for FL LA TX:
  1. per screened permit (clean/permit_states/permit_elevations_{st}.dta, which carries permit_id):
     cl_residential, cl_new_construction (either NEW CONSTRUCTION label), n_classifiers,
     classifiers (labels joined with "; "), from raw/attom_permits/attom_permit_classifiers_*.parquet.
     A permit with no row gets missing: with only two label families downloaded, "no row" can mean
     commercial, unclassified, or simply neither label.
  2. per address (state x county x street_address, the collapse key of clean_permits.do): max of the
     flags, first non-blank labels; merged onto clean/permit_elevations.dta and
     clean/permit_elevations_zipfilled.dta, replacing the empty columns the cluster run wrote
     (other states stay missing). The originals are backed up under
     build/nfip_attom_pipeline_v2/tmp/permits_1015_preclassifier/ (Dropbox on this Mac purges
     archive/ folders, tmp/ persists).
  3. comparison with the regex flags on the address file, to output/tables/permit_classifiers_vs_regex.xlsx:
     coverage of the labels among elevation addresses; new_construction (regex) x cl_new_construction;
     retrofit addresses ATTOM tags as new construction; residential coverage; by state.
Usage: python permit_classifiers_attach.py --data <Data root> --output <output root> [--dry-run]
"""
import argparse, shutil
from pathlib import Path
import duckdb, pandas as pd

STATES = ["FL", "LA", "TX"]
KEY = ["state", "county", "street_address"]
CL_COLS = ["classifiers", "n_classifiers", "cl_residential", "cl_new_construction"]

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--data", required=True); ap.add_argument("--output", required=True)
    ap.add_argument("--dry-run", action="store_true", help="compute and compare, write nothing to Dropbox")
    a = ap.parse_args(); D = Path(a.data)
    src = str(D / "raw" / "attom_permits" / "attom_permit_classifiers_*.parquet")
    con = duckdb.connect(); con.execute("SET memory_limit='20GB'; SET threads=6")

    # 1. permit level ------------------------------------------------------------------------------
    frames = []
    for st in STATES:
        p = pd.read_stata(D / "clean" / "permit_states" / f"permit_elevations_{st.lower()}.dta",
                          columns=["permit_id", "state", "county", "street_address", "new_construction", "retrofit"])
        p["permit_id"] = p.permit_id.astype(str).str.strip()
        frames.append(p)
    permits = pd.concat(frames, ignore_index=True)
    con.register("permits", permits)
    cl = con.execute(f"""
        WITH c AS (SELECT cast(BUILDINGPERMITID AS varchar) AS permit_id, CLASSIFIER AS cl
                   FROM read_parquet('{src}', union_by_name = true)
                   WHERE cast(BUILDINGPERMITID AS varchar) IN (SELECT DISTINCT permit_id FROM permits))
        SELECT permit_id, string_agg(DISTINCT cl, '; ' ORDER BY cl) AS classifiers, count(DISTINCT cl) AS n_classifiers,
               max(CASE WHEN cl = 'RESIDENTIAL' THEN 1 ELSE 0 END) AS cl_residential,
               max(CASE WHEN cl IN ('NEW CONSTRUCTION', 'NEW CONSTRUCTION (BETA)') THEN 1 ELSE 0 END) AS cl_new_construction
        FROM c GROUP BY 1""").df()
    permits = permits.merge(cl, on="permit_id", how="left")
    cov = permits.groupby("state").agg(permits=("permit_id", "size"), with_classifier=("n_classifiers", lambda x: x.notna().sum()),
                                       residential=("cl_residential", "sum"), new_construction=("cl_new_construction", "sum"))
    print("permit level:\n" + cov.to_string())

    # 2. address level ----------------------------------------------------------------------------
    addr = permits.groupby(KEY, as_index=False).agg(
        classifiers=("classifiers", lambda x: next((v for v in x if isinstance(v, str) and v), "")),
        n_classifiers=("n_classifiers", "max"), cl_residential=("cl_residential", "max"), cl_new_construction=("cl_new_construction", "max"))
    pe = pd.read_stata(D / "clean" / "permit_elevations.dta")
    m = pe[pe.state.isin(STATES)].drop(columns=[c for c in CL_COLS if c in pe.columns]).merge(addr, on=KEY, how="left")
    print(f"addresses FL LA TX: {len(m):,}; with a classifier: {int(m.n_classifiers.notna().sum()):,}; "
          f"unmatched to the permit-level file: {int(m.n_classifiers.isna().sum()):,}")

    # 3. comparison ---------------------------------------------------------------------------------
    m["regex_new_construction"] = m.new_construction.astype(int); m["cl_nc"] = m.cl_new_construction.fillna(-1).astype(int)
    rows = []
    for st, g in list(m.groupby("state")) + [("All", m)]:
        has = g.n_classifiers.notna()
        rows.append(dict(state=st, addresses=len(g), with_classifier=int(has.sum()), pct_with_classifier=round(100 * has.mean(), 1),
                         cl_residential=int((g.cl_residential == 1).sum()), cl_new_construction=int((g.cl_new_construction == 1).sum()),
                         regex_new_construction=int(g.regex_new_construction.sum()),
                         both_new=int(((g.regex_new_construction == 1) & (g.cl_nc == 1)).sum()),
                         regex_only=int(((g.regex_new_construction == 1) & (g.cl_nc == 0)).sum()),
                         regex_only_unclassified=int(((g.regex_new_construction == 1) & (g.cl_nc == -1)).sum()),
                         classifier_only=int(((g.regex_new_construction == 0) & (g.cl_nc == 1)).sum()),
                         retrofit_addresses=int((g.retrofit == 1).sum()),
                         retrofit_tagged_new_by_attom=int(((g.retrofit == 1) & (g.cl_nc == 1)).sum()),
                         retrofit_residential=int(((g.retrofit == 1) & (g.cl_residential == 1)).sum()),
                         retrofit_unclassified=int(((g.retrofit == 1) & (g.cl_nc == -1)).sum())))
    comp = pd.DataFrame(rows); print(comp.to_string(index=False))
    xt = pd.crosstab([m.state, m.regex_new_construction], m.cl_nc.map({-1: "no row", 0: "residential only", 1: "new construction"}), margins=True)
    out = Path(a.output) / "tables" / "permit_classifiers_vs_regex.xlsx"
    with pd.ExcelWriter(out) as xw:
        comp.to_excel(xw, sheet_name="by_state", index=False); xt.to_excel(xw, sheet_name="regex_x_classifier")
        # the disagreements, for a hand read
        m.loc[(m.regex_new_construction == 1) & (m.cl_nc == 0), ["state", "county", "street_address", "year", "permit_subtype", "description", "classifiers"]].head(300).to_excel(xw, sheet_name="regex_new_attom_residential", index=False)
        m.loc[(m.retrofit == 1) & (m.cl_nc == 1), ["state", "county", "street_address", "year", "permit_subtype", "description", "classifiers"]].head(300).to_excel(xw, sheet_name="retrofit_attom_new", index=False)
    print("wrote", out)
    if a.dry_run:
        print("dry run: Dropbox files untouched"); return

    # write back -------------------------------------------------------------------------------------
    bak = D / "build" / "nfip_attom_pipeline_v2" / "tmp" / "permits_1015_preclassifier"; bak.mkdir(parents=True, exist_ok=True)
    for name in ["permit_elevations.dta", "permit_elevations_zipfilled.dta"]:
        f = D / "clean" / name
        shutil.copy2(f, bak / name)
        df = pd.read_stata(f)
        labels = pd.read_stata(f, iterator=True).variable_labels()
        df = df.drop(columns=[c for c in CL_COLS if c in df.columns]).merge(addr, on=KEY, how="left")
        for c in ["n_classifiers", "cl_residential", "cl_new_construction"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["classifiers"] = df.classifiers.fillna("")
        labels.update({"classifiers": "ATTOM work classifiers (RESIDENTIAL / NEW CONSTRUCTION download of 10-15; labels joined with ; )",
                       "n_classifiers": "Distinct ATTOM classifiers (max over the address's permits)",
                       "cl_residential": "ATTOM classifier RESIDENTIAL on some permit (10-15 download)",
                       "cl_new_construction": "ATTOM classifier NEW CONSTRUCTION incl. BETA on some permit (10-15 download)"})
        df.to_stata(f, write_index=False, version=118, variable_labels={k: v[:80] for k, v in labels.items() if k in df.columns})
        print(f"rewrote {f} ({len(df):,} rows); backup in {bak}")

if __name__ == "__main__":
    main()

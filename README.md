# Climate Investments

Empirical analysis of household climate adaptation — primarily **flood-mitigation home
elevations** — and how FEMA mitigation funding is allocated relative to property wealth and
flood risk.

**Scope:** NFIP policies, FEMA FMA, ATTOM and Builty all run over the **20 sample states**
(`local states` in `code/master.do`). The analysis set is a property × policy-year panel of
NFIP-insured single-family homes, restricted to county-years with Builty permit coverage, carrying
claims, multiple-loss status, county FMA spending, and a minimal set of ATTOM/Builty link variables.

## Two roots: code and data

Code lives in **git** (this repo); data lives in **Dropbox** (large, synced, shared, kept out of
git). They're decoupled — `master.do` sets two paths and everything derives from them.

| | Path (Vendela) |
|---|---|
| **code** (this repo) | `…/Documents/Econ_PhD/Projects/climate-investments/code` |
| **data** (Dropbox)   | `…/Library/CloudStorage/Dropbox/Flooding/Empirical/Data` |

To run on another machine, edit only the `local code` / `local data` lines at the top of
`code/master.do` (an `Anna` pair is provided, commented out).

## Pipeline (`code/master.do`)

```
PREP   prepare/extract_nfip_policies.py  -> clean/nfip_policies_raw/{st}.csv
       prepare/extract_builty.py         -> clean/builty_raw/{st}.csv
CLEAN  clean/crosswalks.do               -> clean/crosswalks/county_xwalk.dta
       clean/clean_cpi.do                -> clean/cpi.dta
       clean/clean_fma.do                -> clean/fma_elevation.dta
       clean/clean_nfip_policies.do      -> clean/nfip_policies_state/{st}.dta
       clean/clean_builty.do             -> clean/builty_states/builty_elevations_{st}.dta + clean/builty_elevations.dta
       clean/geocode_builty.py           -> clean/builty_elevations_zipfilled.dta
       clean/clean_builty_coverage.py    -> clean/builty_coverage_{county,zip}.dta
       clean/clean_nfip_claims.do        -> clean/nfip_claims_{panel,property}.dta
       clean/clean_nfip_multiple_loss.do -> clean/nfip_multiple_loss.dta
BUILD  build/prep_fma.do                 -> clean/fma_{zip,county}.dta
       build/prep_nfip_policies.do       -> clean/nfip_policies_panel.dta + clean/nfip_policies_property.dta
       build/merge_nfip_fma.do           -> build/nfip_hma_panel.dta
       slurm/run_property_matching.sh    -> build/nfip_attom_pipeline_v2/...   (cluster: ATTOM geocode, NFHL, Builty, NFIP assignment)
       build/parquet_dta.py              -> build/nfip_attom_property/{st}_nfip_attom_property.dta
       build/complete.do                 -> analysis/analysis.dta (+ analysis/extracts/500M_subsample.dta)
ANALYSIS  analysis/*.do, *.py            run separately
```

`merge_nfip_fma.do` is the core builder: it starts from the NFIP policy-year panel (~10M
single-family properties, ~50M policy-years), attaches RL/SRL status from the multiple-loss file,
claims by property-year, and FMA funding at the county grain (ZIP is finer but only covers grants FEMA
logged at the property level). `complete.do` then restricts to county-years with Builty permit
coverage and merges the ATTOM/Builty property links from the cluster matching run (property value,
Builty elevation flag and year).

**Builty** reaches NFIP through ATTOM: `clean_builty.do` screens permits to true home elevations,
`attom_builty.py` matches them to ATTOM on street address, and `nfip_attom.py` assigns each NFIP
property one ATTOM property inside a Wagner cell. The Gen-1 Builty chain is archived in
`build/archive/`.

## Code organization (`code/`)

| Folder | Stage |
|---|---|
| `code/prepare/` | run-once Python: acquisition (`import_dewey.py`), per-state extraction (`extract_*.py`), ATTOM geocoding |
| `code/clean/` | raw → clean (`clean_*.do`, one per source); dropped sources + `torch_work/` in `clean/archive/` |
| `code/build/` | clean → build/analysis (`prep_*`, `merge_nfip_fma.do`, `complete.do`, ATTOM cells; `build/archive/` holds superseded code incl. the Builty chain) |
| `code/descriptives/` | `summary_table.do` (via `master.do`) and the Builty word cloud; Gen-1 scripts in `descriptives/archive/` |
| `code/analysis/` | regressions, identification, etc. (run separately) — currently all Gen-1, in `analysis/archive/` |
| `output/` | `tables/` and `figures/` — repo-root sibling of `code/` (artifacts, not code) |
| `archive/` | superseded data, outputs, and drafts — repo-root sibling; gitignored |

## Data organization (`Dropbox/Flooding/Empirical/Data/`)

Four stages, each its own folder (`raw → clean → build → analysis`); see the data folder's own
`ReadMe.md`. Current key files: `clean/nfip_policies_state/{st}.dta`, `clean/nfip_policies_{panel,property}.dta`,
`clean/{fma_county, cpi, nfip_multiple_loss, nfip_claims_panel, builty_coverage_county}.dta`,
`build/nfip_hma_panel.dta`, `build/nfip_attom_property/{st}_nfip_attom_property.dta`, `analysis/analysis.dta`.

### Data sources

Built by `master.do`:

| Source | What | Grain |
|---|---|---|
| **FEMA NFIP policies** | flood-insurance policies; the eligible universe + elevation flag + rated flood zone | policy-year → property |
| **FEMA NFIP multiple-loss** | RL/SRL status (FMA prioritization) | property |
| **FEMA HMA** | mitigation grants, restricted to FMA single-family elevations; Mitigated Properties (ZIP) + Projects (dollars, BCR, status) | record → ZIP / county |
| **FEMA NFIP claims** | paid claims, building + contents | property-year |
| **ATTOM** | property records (value, year built, address); all 20 states | property, assigned to NFIP properties by Wagner cell |
| **Builty** | building permits; flood-elevation permits via text screen; all-permit counts as a coverage index | permit → ATTOM property; county-year coverage |

**Removed 2026-05-29:** NRI, NPR buyouts, ClimateRisk. Superseded code moves to the stage's `archive/` folder,
which is gitignored: it stays on the machine that archived it and in git history.

## Requirements

| | |
|---|---|
| **Stata** | 18 (`master.do` sets `version 18`) |
| **Python** | 3.11+ with `duckdb`, `pandas`, `pyarrow`, `deweydatapy` |
| **Access** | FEMA NFIP/HMA files are public; ATTOM and Builty require a Dewey licence |

Stata's GUI `PATH` does not pick up conda — set `local python` in `master.do` to the full interpreter
path. The four shared GitHub shell entry points live in `code/slurm/`; local build and diagnostic wrappers remain beside the code they run.

## Reproducing

1. Edit `local code` and `local data` at the top of `code/master.do` (an `Anna` pair is provided,
   commented out). Nothing else contains a machine-specific path.
2. Set `local states` if you want a subset of the 20 sample states.
3. Set the `0/1` switches in Section 1 for the steps you want, then run `code/master.do` top to bottom.
   Steps are ordered by dependency; each writes to `data/clean/` or `data/analysis/`.

Analysis and descriptives are **not** invoked by `master.do` — it runs data construction only.

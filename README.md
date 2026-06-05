# Climate Investments

Empirical analysis of household climate adaptation — primarily **flood-mitigation home
elevations** — and how FEMA mitigation funding is allocated relative to property wealth and
flood risk (current focus: **TX and VA**).

## Two roots: code and data

Code lives in **git** (this repo); data lives in **Dropbox** (large, synced, shared, kept out of
git). They're decoupled — `master.do` sets two paths and everything derives from them.

| | Path (Vendela) |
|---|---|
| **code** (this repo) | `…/Documents/Econ_PhD/Projects/climate-investments/code` |
| **data** (Dropbox)   | `…/Library/CloudStorage/Dropbox/Flooding/Data` |

To run on another machine, edit only the `global code` / `global data` lines at the top of
`code/master.do` (an `Anna` pair is provided, commented out).

## Pipeline (`code/master.do`)

```
CLEAN  clean/clean_hma.do            -> hma_projects.dta
       clean/clean_nfip_claims.do    -> nfip_claims.dta
       clean/clean_nfip_policies.do  -> nfip_policies_{tx,va}.dta
BUILD  build/filter_builty_strict.py    strict-filter Builty elevation permits      [PENDING Anna's revision]
       build/split_builty_states.py     split filtered permits into TX/VA parquet
       build/attom_onto_permits.py      match ATTOM property records onto permits    [PENDING Anna's revision]
       build/parquetdta.py              matched parquet -> {state}_attom_builty.dta
       build/nfip_build.do              NFIP claims -> county-year   [needs reconciling, see TODO]
       build/build_nfip_hma_panels.do   -> analysis/{state}_{property,county}_nfip_hma.dta
ANALYSIS  analysis/*.do, *.py           run separately
```

`build_nfip_hma_panels.do` is the core builder: it matches NFIP **policies** to permits via tiered
Wagner-style cells (ZIP×construction-year×policy-year → … → county×year) and merges HMA at
county×year.

## Code organization (`code/`)

| Folder | Stage |
|---|---|
| `clean/` | raw → clean — one `.do` per source (HMA, NFIP claims, NFIP policies); dropped sources in `clean/archive/` |
| `build/` | clean → build/analysis (active Gen-2 scripts above; `build/archive/` holds superseded code) |
| `descriptives/` | descriptive scripts (run separately) — currently all Gen-1, in `descriptives/archive/` |
| `analysis/` | regressions, identification, etc. (run separately) — currently all Gen-1, in `analysis/archive/` |
| `output/` | saved `.gph` graphs |
| `torch_work/` | upstream raw acquisition (Dewey API pulls, NYU cluster) |

## Data organization (`Dropbox/Flooding/Data/`)

Four stages, each its own folder (`raw → clean → build → analysis`); see the data folder's own
`ReadMe.md`. Current key files: `clean/{hma_projects, nfip_claims, nfip_policies_tx, nfip_policies_va,
all_builty_elevations}.dta`, `build/{tx,va}_attom_builty.dta`, `analysis/{tx,va}_{property,county}_nfip_hma.dta`.

### Data sources (active)

| Source | What | Grain |
|---|---|---|
| **Builty** | building permits; flood-elevation permits via text detection | permit |
| **ATTOM** | property records (value, year built); matched to permits on address+ZIP (TX, VA) | property |
| **FEMA HMA** | mitigation grants — **restricted to FMA** | project → county |
| **FEMA NFIP claims** | flood-insurance claims | claim → county-year |
| **FEMA NFIP policies** | flood-insurance policies (premiums/coverage) | policy |

**Dropped from the pipeline** (commented out / archived 2026-05-29): NRI, NPR buyouts, ClimateRisk.

## Status

See `code/TODO.md`. In short: `filter_builty_strict.py` and `attom_onto_permits.py` are pending
Anna's revised versions; `nfip_build.do` needs its inputs/outputs reconciled with
`create_clean_files.do`; superseded Gen-1 merge scripts are held in `build/` pending review.

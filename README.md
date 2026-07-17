# Climate Investments

Empirical analysis of household climate adaptation — primarily **flood-mitigation home
elevations** — and how FEMA mitigation funding is allocated relative to property wealth and
flood risk.

**Scope:** NFIP policies and FEMA FMA run over the **20 sample states** (`local states` in
`code/master.do`). ATTOM property values are the binding constraint — only **TX and VA** are
acquired, so anything needing property wealth is still those two.

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
CLEAN  clean/extract_nfip_policies.py    -> clean/nfip_policies_raw/{st}.csv
       clean/extract_builty.py           -> clean/builty_raw/{st}.csv
       clean/crosswalks.do               -> clean/crosswalks/county_xwalk.dta
       clean/clean_cpi.do                -> clean/cpi.dta
       clean/clean_fma.do                -> clean/fma_elevation.dta
       clean/clean_nfip_policies.do      -> clean/nfip_policies_state/{st}.dta
       clean/clean_builty.do             -> clean/builty_permits_{st}.dta
       clean/clean_nfip_multiple_loss.do -> clean/nfip_multiple_loss.dta
BUILD  build/prep_fma.do                 -> clean/fma_{zip,county}.dta
       build/prep_nfip_policies.do       -> clean/nfip_policies_property.dta
       build/compile.do                  -> analysis/analysis.dta
       build/build_attom_value_cells.py  -> build/{state}_attom_value_{zip,county}_{year,decade}.dta
ANALYSIS  analysis/*.do, *.py            run separately
```

`compile.do` is the core builder: it starts from the NFIP-insured property universe (~5.2M
single-family structures), attaches RL/SRL status from the multiple-loss file, and merges FMA funding
at **both** ZIP and county grain. ATTOM value cells are the remaining piece.

The **Builty permit chain** (`build_builty_filter`, `build_split_builty_states`,
`build_attom_onto_permits`, `build_fma_onto_builty_attom`, `parquetdta`, `build_nfip_hma_panels`) is
archived in `build/archive/` and not invoked by `master.do`; elevation status currently comes from the
NFIP policy file's own flag.

## Code organization (`code/`)

| Folder | Stage |
|---|---|
| `code/clean/` | acquisition (`import_dewey.py`), per-state extraction (`extract_*.py`), and raw → clean (`clean_*.do`, one per source); dropped sources + `torch_work/` in `clean/archive/` |
| `code/build/` | clean → build/analysis (`prep_*`, `compile.do`, ATTOM cells; `build/archive/` holds superseded code incl. the Builty chain) |
| `code/descriptives/` | descriptive scripts (run separately) — currently all Gen-1, in `descriptives/archive/` |
| `code/analysis/` | regressions, identification, etc. (run separately) — currently all Gen-1, in `analysis/archive/` |
| `output/` | saved `.gph` graphs — repo-root sibling of `code/` (artifacts, not code) |

## Data organization (`Dropbox/Flooding/Empirical/Data/`)

Four stages, each its own folder (`raw → clean → build → analysis`); see the data folder's own
`ReadMe.md`. Current key files: `clean/{fma_elevation, fma_zip, fma_county, cpi, nfip_multiple_loss,
nfip_policies_property}.dta`, `clean/nfip_policies_state/{st}.dta`, `analysis/analysis.dta`.

### Data sources

Built by `master.do`:

| Source | What | Grain |
|---|---|---|
| **FEMA NFIP policies** | flood-insurance policies; the eligible universe + elevation flag + rated flood zone | policy-year → property |
| **FEMA NFIP multiple-loss** | RL/SRL status (FMA prioritization) | property |
| **FEMA HMA** | mitigation grants, restricted to FMA single-family elevations; Mitigated Properties (ZIP) + Projects (dollars, BCR, status) | record → ZIP / county |
| **ATTOM** | property records (value, year built); TX and VA only | property → ZIP/county cells |

Held in the repo but not invoked by `master.do`:

| Source | What | Grain |
|---|---|---|
| **FEMA NFIP claims** | flood-insurance claims | claim |
| **Builty** | building permits; flood-elevation permits via text detection | permit |

**Removed 2026-05-29:** NRI, NPR buyouts, ClimateRisk. Superseded code is kept rather than deleted —
see `clean/archive/` and `build/archive/`.

## Requirements

| | |
|---|---|
| **Stata** | 18 (`master.do` sets `version 18`) |
| **Python** | 3.11+ with `duckdb`, `pandas`, `pyarrow`, `deweydatapy` |
| **Access** | FEMA NFIP/HMA files are public; ATTOM and Builty require a Dewey licence |

Stata's GUI `PATH` does not pick up conda — set `local python` in `master.do` to the full interpreter
path. `build_attom_value_cells.py` is sized for a cluster (Torch/SLURM wrapper: the matching `.sh`).

## Reproducing

1. Edit `local code` and `local data` at the top of `code/master.do` (an `Anna` pair is provided,
   commented out). Nothing else contains a machine-specific path.
2. Set `local states` if you want a subset of the 20 sample states.
3. Set the `0/1` switches in Section 1 for the steps you want, then run `code/master.do` top to bottom.
   Steps are ordered by dependency; each writes to `data/clean/` or `data/analysis/`.

Analysis and descriptives are **not** invoked by `master.do` — it runs data construction only.

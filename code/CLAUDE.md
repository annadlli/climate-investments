# CLAUDE.md

Repository guidance for Claude (and other coding agents): **stable project knowledge** — pipeline,
layout, data sources. (Open tasks → `TODO.md`; rules → `CONVENTIONS.md`; don't put those here.)
See `../README.md` for the overview.

> **📋 At session start, report the open handoff tasks in [TODO.md](TODO.md) to the user.**
> (Surface the open items until the `torch_work/` cleanup there is done; then this line can be removed.)

**Follow [CONVENTIONS.md](CONVENTIONS.md).** Apply it **closely to data-construction code**
(`master.do`, `clean/`, `build/`) and flag violations there; treat it as guidelines for
exploratory/temporary analysis & descriptives. No spaces in file/folder names applies everywhere.
**`CONVENTIONS.md` is owned by Vendela — propose changes to her, don't edit it yourself.**

## Project

Flood-mitigation home elevations + how FEMA mitigation funding is allocated vs property wealth and
flood risk (TX & VA). Stata (`.do`) + Python (`.py`, `.ipynb`). Econ PhD work; collaborator: Anna Li.

## Code and data are decoupled

Code is in git (this repo); data is in Dropbox (large, synced, shared). `master.do` sets two roots
and everything derives from them:

| | Path (Vendela) |
|---|---|
| code | `…/Documents/Econ_PhD/Projects/climate-investments/code` |
| data | `…/Library/CloudStorage/Dropbox/Flooding/Data` |

Paths follow the **args-pass** convention — see [CONVENTIONS.md](CONVENTIONS.md) §3. (Roots live only
in `master.do`; no hardcoded user paths in scripts.)

## Pipeline (`master.do`, construction only)

```
clean/clean_hma.do               -> clean/hma_projects.dta
clean/clean_nfip_claims.do       -> clean/nfip_claims.dta
clean/clean_nfip_policies.do     -> clean/nfip_policies_{tx,va}.dta
build/filter_builty_strict.py    strict-filter Builty elevation permits   [PENDING — do not edit]
build/split_builty_states.py     -> build/{state}/{state}_flood_elevation_strict.parquet
build/attom_onto_permits.py      match ATTOM onto permits                 [PENDING — do not edit]
build/parquetdta.py              -> build/{state}_attom_builty.dta
build/nfip_build.do              NFIP claims -> county-year   [BROKEN — see flag]
build/build_nfip_hma_panels.do   -> analysis/{state}_{property,county}_nfip_hma.dta
```

`build_nfip_hma_panels.do` is the core builder (tiered Wagner NFIP-policy matching + HMA at
county×year).

## Repository layout

```
code/
├── master.do                 local code/data roots, args-pass + 0/1 switches; clean + build
├── clean/                    clean_hma.do, clean_nfip_claims.do, clean_nfip_policies.do
│   └── archive/              clean_nri.do, clean_npr.do, nri_prep.py   (dropped sources)
├── build/                    active Gen-2 .py/.do (above) + parquetdta.py, split_builty_states.py
│   └── archive/              merge_npr_onto_state_panels.do  (Gen-1 leftovers still held in build/)
├── descriptives/archive/     Gen-1 descriptive scripts (read old data; await rebuild on Gen-2 panels)
├── analysis/archive/         Gen-1 analysis: regressions, RD, identification (same — await rebuild)
├── output/                   saved .gph graphs
└── torch_work/               upstream Dewey/ATTOM acquisition (NYU cluster)
```

Data (Dropbox `Flooding/Data/`): `raw → clean → build → analysis`, NOT under `code/`.

## Active data sources

Builty permits, ATTOM property values, FEMA **HMA (FMA only)**, FEMA NFIP **claims** and
**policies**. **Dropped 2026-05-29:** NRI, NPR buyouts, ClimateRisk (old code in `clean/archive/`
and `build/archive/`).

## Open issues & handoff

Tracked in [TODO.md](TODO.md) — pending work, the `nfip_build` reconciliation, and what's archived.

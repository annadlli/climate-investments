# CLAUDE.md

Repository guidance for Claude (and other coding agents): **stable project knowledge** — pipeline,
layout, data sources. (Open tasks → `TODO.md`; rules → `CONVENTIONS.md`; don't put those here.)
See `../README.md` for the overview.

> **📋 At session start, report the open handoff tasks in [TODO.md](TODO.md) to the user.**
> (Surface the open items until the `torch_work/` cleanup there is done; then this line can be removed.)

**Rules** — naming, paths, banners, Stata, workflow — live in [CONVENTIONS.md](CONVENTIONS.md); follow
them. It's **Vendela-owned**: agents on her machine may edit it on her behalf; others (incl. Anna's
agents) propose changes to her.

## Project

Flood-mitigation home elevations + how FEMA mitigation funding is allocated vs property wealth and
flood risk. Stata (`.do`) + Python (`.py`, `.ipynb`). Econ PhD work; collaborator: Anna Li.

**Scope:** NFIP policies and FEMA FMA now run over the 20 sample states (`local states` in
`master.do`). ATTOM is the binding constraint — only TX & VA are pulled, so anything needing property
values is still those two.

## Code and data are decoupled

Code is in git (this repo); data is in Dropbox (large, synced, shared). `master.do` sets two roots
and everything derives from them:

| | Path (Vendela) |
|---|---|
| code | `…/Documents/Econ_PhD/Projects/climate-investments/code` |
| data | `…/Library/CloudStorage/Dropbox/Flooding/Empirical/Data` |

Paths follow the **args-pass** convention — see [CONVENTIONS.md](CONVENTIONS.md) §3. (Roots live only
in `master.do`; no hardcoded user paths in scripts.)

## Pipeline (`master.do`, construction only)

```
clean/extract_nfip_policies.py    -> clean/nfip_policies_raw/{st}.csv    (split national file per state)
clean/extract_builty.py           -> clean/builty_raw/{st}.csv           (per-state elevation candidates)
clean/crosswalks.do               -> clean/crosswalks/county_xwalk.dta   (state·county -> FIPS)
clean/clean_cpi.do                -> clean/cpi.dta                       (annual, base 2023)
clean/clean_fma.do                -> clean/fma_elevation.dta             (FMA single-family elevations)
clean/clean_builty.do             -> clean/builty_permits_{st}.dta       (permit candidates, basic clean)
clean/clean_nfip_policies.do      -> clean/nfip_policies_state/{st}.dta  (policy-year level, per state)
clean/clean_nfip_multiple_loss.do -> clean/nfip_multiple_loss.dta
build/prep_fma.do                 -> clean/fma_zip.dta + clean/fma_county.dta
build/prep_nfip_policies.do       -> clean/nfip_policies_property.dta    (policy-year -> property)
build/compile.do                  -> analysis/analysis.dta               (property-level analysis set)
build/build_attom_value_cells.py  -> build/{state}_attom_value_{zip,county}_{year,decade}.dta
                                     (.sh = Torch/SLURM wrapper)
```

`clean_fma.do` builds the FMA universe from **two** FEMA files: HMA Mitigated Properties (record level,
carries ZIP) with HMA Projects merged in `m:1` on `projectidentifier` (carries dollars, BCR, status).
The merge doubles as the funding screen — MitProps logs properties for applications that were never
funded, so the Projects status filter is what removes them. `prep_fma.do` then pools grants to ZIP
(primary) and county (fallback); `clean_nfip_claims.do` is parked in `scratch/` pending the ∆D work.

`build_attom_value_cells.py` aggregates raw ATTOM to ZIP/county × construction-year/decade value
cells — NFIP has no street address, so these merge property values onto the NFIP universe by cell.

The Builty-permit chain (`build_builty_filter` / `build_split_builty_states` /
`build_attom_onto_permits` / `build_fma_onto_builty_attom` / `parquetdta` / `build_nfip_hma_panels`)
is **deprioritized and moved to `build/archive/`** — a precision option, not the current path.

## Repository layout

```
code/
├── master.do                 local code/data roots, args-pass + 0/1 switches; clean + build
├── clean/                    import_dewey.py (acquisition) + extract_* (national -> per-state) + clean_* (raw -> clean)
│   └── archive/              dropped sources (clean_nri/npr, nri_prep, clean_fma_projects) + torch_work/ (NYU cluster acquisition)
├── build/                    prep_fma, prep_nfip_policies, compile + build_attom_value_cells.py (+.sh)
│   └── archive/              Gen-1 merge/panel scripts + nfip_build.do + deprioritized Builty chain
├── descriptives/             descriptive scripts (Gen-1 in descriptives/archive/, await rebuild)
└── analysis/                 regressions, RD, identification (Gen-1 in analysis/archive/, await rebuild)

output/                       saved .gph graphs — repo-root sibling of code/ (artifacts, not code)
```

Data (Dropbox `Flooding/Empirical/Data/`): `raw → clean → build → analysis`, NOT under `code/`.

## Active data sources

Builty permits, ATTOM property values, FEMA **HMA (FMA home-elevation projects only)**, FEMA NFIP **claims** and
**policies**. **Dropped 2026-05-29:** NRI, NPR buyouts, ClimateRisk (old code in `clean/archive/`
and `build/archive/`).

## Merge logic & eligible universe

Each source contributes distinct columns: **NFIP policies** = elevation status/measures + insurance &
flood-zone context; **ATTOM** = exact address + property valuation; **FMA** = federal funding, pooled
to **ZIP (primary) and county (fallback)** — its finest geography is ZIP, and grants FEMA never logged
at property level carry no ZIP at all, so they exist only at county; **Builty** = permit-level
elevation events (a precision option, currently
deprioritized). NFIP carries no exact address (lat/long are coarsened to ~1 decimal), so it is joined
by **fuzzy Wagner cells, not 1:1**. The relevant match is Wagner's **property match**
(`4_merge_all_houses.do`): cell = `{zip OR community} × construction-year × flood-zone × policy-year`
(zip primary, community fallback) — this is how NFIP links to ATTOM/permits (and `build_nfip_hma_panels.do`
mirrors it). The redacted policy file is **transaction-level** (~5–6 policy-years per structure);
dropping the policy-year from that cell deduped to *approximate structures*. (Wagner's separate
policy↔claims match adds `org_nb_dt`/`srl_ind`/`count_buy` — not used here.) Eligible universe =
**NFIP-insured single-family structures**; NFIP's own elevation flag + rated flood zone mean
**neither Builty nor NFHL is needed** for the structure-level universe — ATTOM is pulled in (fuzzily
linked) only for property-level valuation.

## Reference: Wagner replication repo

Wagner (2022) — the source of the tiered cell-match method this project borrows — ships a full Stata/R
cleaning + analysis replication package in Dropbox `Flooding/Empirical/Wagner_repository/` (sibling of
`Flooding/Empirical/Data/`; `code/A_cleaning/` is the cleaning pipeline, `README.pdf` documents it). **When
writing or revising NFIP / flood-risk data-cleaning code (claims, policies, flood zones), consult it
for inspiration first** — e.g. the Wagner-cell match keys
(`zipcode · year_built · flood_zone · year · community · org_nb_dt`), the high/low-risk flood-zone
classification (high = A/AE/AH/AO/V/VE), and the single-family / construction-year screens. Our project
differs from hers (we estimate additionality + ∆D, not adverse selection), so **adapt, don't copy**.

## Open issues & handoff

Tracked in [TODO.md](TODO.md) — pending work and what's archived.

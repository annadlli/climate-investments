# CLAUDE.md

Repository guidance for Claude (and other coding agents): **stable project knowledge** — pipeline,
layout, data sources. (Open tasks → `TODO.md`; rules → `CONVENTIONS.md`; don't put those here.)
See `../README.md` for the overview.

> **📋 At session start, report the open handoff tasks in [TODO.md](TODO.md) to the user.**
> (Surface the open items until the `build/` cleanup there is done; then this line can be removed.)

**Rules** — naming, paths, banners, Stata, workflow — live in [CONVENTIONS.md](CONVENTIONS.md); follow
them. It's **Vendela-owned**: agents on her machine may edit it on her behalf; others (incl. Anna's
agents) propose changes to her.

## Project

Flood-mitigation home elevations + how FEMA mitigation funding is allocated vs property wealth and
flood risk. Stata (`.do`) + Python (`.py`, `.ipynb`). Econ PhD work; collaborator: Anna Li.

**Scope:** NFIP policies and FEMA FMA run over the 20 sample states (`local states` in `master.do`).
ATTOM and Builty cover all 20 too (property links for all 20 as of Aug 31). The active pipeline is one
route (settled 2026-09-03; `compile2.do` deleted, `final_data.do` archived): `clean_nfip_policies` →
`prep_nfip_policies` (panel + first-policy-year snapshot) → `merge_nfip_fma` (claims, multiple-loss,
county FMA) → `complete` (Builty coverage restriction + ATTOM/Builty property links).

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
prepare/import_dewey.py           -> raw/dewey/...                       (Dewey ATTOM + Builty pull)
prepare/extract_nfip_policies.py  -> clean/nfip_policies_raw/{st}.csv    (split national file per state)
prepare/extract_builty.py         -> clean/builty_raw/{st}.csv           (per-state elevation candidates)
prepare/extract_attom.py          -> {st}/attom_{st}.parquet             (split Dewey ATTOM batches per state)
prepare/geocode_attom.py          -> build/attom_geocode/{st}_addr/...   (Census geocode of ATTOM addresses, Torch)
clean/crosswalks.do               -> clean/crosswalks/county_xwalk.dta   (state·county -> FIPS)
clean/clean_cpi.do                -> clean/cpi.dta                       (annual, base 2023)
clean/clean_fma.do                -> clean/fma_elevation.dta             (FMA single-family elevations)
clean/clean_builty.do             -> clean/builty_states/builty_elevations_{st}.dta  (per-state, screened to true elevations)
                                     + clean/builty_elevations.dta        (appended, collapsed to property level)
clean/geocode_builty.py           -> clean/builty_elevations_zipfilled.dta  (Census geocoder fills missing ZIPs; permits input to matching)
clean/clean_builty_coverage.py    -> clean/builty_coverage_{county,zip}.dta  (all-permit counts by county/ZIP x year; builty_covered flag)
clean/clean_nfip_policies.do      -> clean/nfip_policies_state/{st}.dta  (policy-year level, per state)
clean/clean_nfip_claims.do        -> clean/nfip_claims_panel.dta + clean/nfip_claims_property.dta  (property-year, property)
clean/clean_nfip_multiple_loss.do -> clean/nfip_multiple_loss.dta
build/prep_fma.do                 -> clean/fma_zip.dta + clean/fma_county.dta
build/prep_nfip_policies.do       -> clean/nfip_policies_panel.dta + clean/nfip_policies_property.dta  (panel; first-policy-year snapshot = only NFIP input to the matcher)
build/merge_nfip_fma.do           -> build/nfip_hma_panel.dta             (NFIP property-year panel + claims, multiple-loss, FMA)
slurm/run_property_matching.sh    -> build/nfip_attom_pipeline_v2/...     (ATTOM geocode/NFHL/Builty/property matching jobs)
build/parquet_dta.py              -> build/nfip_attom_property/{st}_nfip_attom_property.dta
build/complete.do                 -> analysis/analysis.dta                 (property-year analysis set: panel + ATTOM/Builty links)
descriptives/summary_table.do     -> ../output/tables/summary_table.{dta,xlsx}  (summary_stats switch)
build/alternates/attom_value_cells.py -> build/{state}_attom_value_{zip,county}_{year,decade}.dta
                                     (.sh = Torch/SLURM wrapper)
```

`clean_fma.do` builds the FMA universe from **two** FEMA files: HMA Mitigated Properties (record level,
carries ZIP) with HMA Projects merged in `m:1` on `projectidentifier` (carries dollars, BCR, status).
The merge doubles as the funding screen — MitProps logs properties for applications that were never
funded, so the Projects status filter is what removes them. `prep_fma.do` then pools grants to ZIP and
county; `merge_nfip_fma.do` merges the county grain (covers all grants), ZIP stays as a finer option.

`attom_value_cells.py` aggregates raw ATTOM to ZIP/county × construction-year/decade value
cells — NFIP has no street address, so these merge property values onto the NFIP universe by cell.

`clean_builty.do` screens each state to true home elevations (Section 1 → `builty_states/`), then
appends and collapses them to the property level (`clean/builty_elevations.dta`, keyed on
`street_address`). Because Builty carries an exact address, it is joined to **ATTOM 1:1 on
`street_address`** (`attom_builty.py`, cluster), and reaches NFIP from the ATTOM side via the Wagner
cell — not a zip/county pool. Builty's permit feeds are patchy (322 of ~1,100 counties report anything),
so `clean_builty_coverage.py` counts all raw permits by county-year and `complete.do` restricts the
panel to covered county-years: outside them an unmatched property is unobserved, not unelevated. The
Gen-1 Builty chain (`build_builty_filter` / `build_split_builty_states` /
`build_attom_onto_permits` / `build_fma_onto_builty_attom` / `parquetdta` / `build_nfip_hma_panels`)
is **superseded and archived in `build/archive/`** — not the current path.

## Repository layout

```
code/
├── master.do                 local code/data roots, args-pass + 0/1 switches; prepare + clean + build + descriptives
├── prepare/                  run-once Python: import_dewey (acquisition), extract_* (national -> per-state), geocode_attom
├── clean/                    clean_* (raw -> clean)
│   └── archive/              dropped sources (clean_nri/npr, nri_prep, clean_fma_projects) + torch_work/ (NYU cluster acquisition)
├── build/                    active construction scripts called by master.do or the SLURM driver
│   └── archive/              Gen-1 merge/panel scripts + nfip_build.do + deprioritized Builty chain
├── descriptives/             summary_table.do (master.do switch) + Builty word cloud; Gen-1 in descriptives/archive/
├── slurm/                    shared cluster wrappers and reusable shell drivers
└── analysis/                 regressions, RD, identification (Gen-1 in analysis/archive/, await rebuild)

output/                       tables/ + figures/ — repo-root sibling of code/ (artifacts, not code; tracked)
archive/                      superseded data, outputs, drafts — repo-root sibling; not tracked
```

Every `archive/` folder (repo root, `output/archive/`, each code stage's `archive/`) and `data/` is
gitignored: archived files stay on disk and in git history but are not in the repo.

Data (Dropbox `Flooding/Empirical/Data/`): `raw → clean → build → analysis`, NOT under `code/`.

## Active data sources

Builty permits, ATTOM property values, FEMA **HMA (FMA home-elevation projects only)**, FEMA NFIP **claims** and
**policies**. **Dropped 2026-05-29:** NRI, NPR buyouts, ClimateRisk (old code in `clean/archive/`
and `build/archive/`).

## Merge logic & eligible universe

Each source contributes distinct columns: **NFIP policies** = elevation status/measures + insurance &
flood-zone context; **ATTOM** = exact address + property valuation; **FMA** = federal funding, merged at
**county** — its finest geography is ZIP, but grants FEMA never logged at property level carry no ZIP
at all, so only county covers every grant (ZIP kept as a finer option); **Builty** = permit-level
elevation events carrying an exact address, so joined to **ATTOM 1:1 on `street_address`** (Anna),
then to NFIP from the ATTOM side. NFIP carries no exact address (lat/long are coarsened to ~1 decimal), so it is joined
by **fuzzy Wagner cells, not 1:1**. The relevant match is Wagner's **property match**
(`4_merge_all_houses.do`): cell = `{zip OR community} × construction-year × flood-zone × policy-year`
(zip primary, community fallback) — this is how NFIP links to ATTOM/permits (and `build_nfip_hma_panels.do`
mirrors it). The redacted policy file is **transaction-level** (~5–6 policy-years per structure);
dropping the policy-year from that cell deduped to *approximate structures*. (Wagner's separate
policy↔claims match adds `org_nb_dt`/`srl_ind`/`count_buy` — not used here.) Eligible universe =
**NFIP-insured single-family structures**; NFIP's own elevation flag + rated flood zone mean
**neither Builty nor NFHL is needed** for the structure-level universe — ATTOM is pulled in (fuzzily
linked) only for property-level valuation. **Caveat: NFIP miscodes elevations.** The flag
(`elevatedbuildingindicator`, insurer/self-reported) behaves as a stock, not a flow — only 2,998 of
313,156 flagged-elevated properties (~1%) show an observed transition — so elevation *events* and
their timing cannot be trusted from NFIP alone; Builty permits are the independent cross-check and
candidate replacement outcome (see TODO.md).

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

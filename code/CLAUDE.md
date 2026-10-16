# CLAUDE.md

Entry point for every coding agent on this repo (Claude Code, Codex, Cursor, ...). Three files, read
all three at session start:

1. **[CONVENTIONS.md](CONVENTIONS.md)** — the rules: naming, paths, banners, comments, Stata, workflow.
   Follow them. **Vendela-owned**: agents on her machine may edit it on her behalf; everyone else,
   including Anna's agents, proposes changes to her.
2. **[TODO.md](TODO.md)** — open tasks and handoff. **Report the open items to the user at session start.**
3. This file — stable project knowledge: pipeline, layout, data sources. Not a task list, not a
   restatement of the rules. See `../README.md` for the overview.

## Project

Flood-mitigation home elevations + how FEMA mitigation funding is allocated vs property wealth and
flood risk. Stata (`.do`) + Python (`.py`, `.ipynb`). Econ PhD work; collaborator: Anna Li.

**Scope** (Claude change 09-10)**:** the default sample is the three key states FL LA TX (`local states` in `master.do`, settled 2026-09-08); the 20-state list is kept in a comment there and every script takes the states as an argument.
ATTOM property records cover all 20; the ATTOM building-permit extract covers FL LA TX only (Claude change 10-02: ATTOM permits replaced Builty as the permit source; the Builty chain is in each stage's `archive/`). The active pipeline is one
route (settled 2026-09-03; `compile2.do` deleted, `final_data.do` archived): `clean_nfip_policies` →
`prep_nfip_policies` (panel + first-policy-year snapshot) → `merge_nfip_hma` (claims, multiple-loss,
county HMA) → `complete` (ATTOM property / permit links, ATTOM value, coverage flags; keeps SFHA rows).

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
prepare/import_attom_permits.py   -> raw/attom_permits/attom_permits_{name}.parquet + attom_permit_status_{name}.parquet  (Dewey download + combine, Torch; manifest in anna_private/)
prepare/extract_attom_permits.py  -> clean/permits_raw/{st}.csv          (per-state elevation candidates from raw/attom_permits/attom_permits_*.parquet, status-history dates joined on BUILDINGPERMITID)
prepare/extract_attom.py          -> {st}/attom_{st}.parquet             (split Dewey ATTOM batches per state)
prepare/geocode_attom.py          -> build/attom_geocode/{st}_addr/...   (Census geocode of ATTOM addresses, Torch)
clean/crosswalks.do               -> clean/crosswalks/county_xwalk.dta   (state·county -> FIPS)
clean/clean_cpi.do                -> clean/cpi.dta                       (annual, base 2023)
clean/clean_hma.do                -> clean/hma_elevation.dta             (HMA single-family elevation grants, all programs)
clean/clean_permits.do            -> clean/permit_states/permit_elevations_{st}.dta  (per-state, screened to true elevations)
                                     + clean/permit_elevations.dta        (appended, collapsed to one row per address, ATTOMID carried)
clean/geocode_permits.py          -> clean/permit_elevations_zipfilled.dta  (Census geocoder fills missing ZIPs, adds block group + coordinates; permits input to matching)
clean/clean_permit_coverage.py    -> clean/permit_coverage_{county,zip,locality}.dta  (all-permit counts x year; permit_covered + permit_covered_strict flags)
clean/clean_nfip_policies.do      -> clean/nfip_policies_state/{st}.dta  (policy-year level, per state)
clean/clean_nfip_claims.do        -> clean/nfip_claims_panel.dta + clean/nfip_claims_property.dta  (property-year, property)
clean/clean_nfip_multiple_loss.do -> clean/nfip_multiple_loss.dta
build/prep_hma.do                 -> clean/hma_zip.dta + clean/hma_county.dta
build/prep_nfip_policies.do       -> clean/nfip_policies_panel.dta + clean/nfip_policies_property.dta  (panel; first-policy-year snapshot = only NFIP input to the matcher)
build/merge_nfip_hma.do           -> build/nfip_hma_panel.dta             (NFIP property-year panel + claims, multiple-loss, HMA)
slurm/run_property_matching.sh    -> build/nfip_attom_pipeline_v2/...     (ATTOM geocode / NFHL / permit link (attom_permits.py) / NFIP assignment (nfip_attom.py))
build/parquet_dta.py              -> build/nfip_attom_property/{st}_nfip_attom_property.dta
build/attom_value.py              -> build/attom_value/{st}_attom_value.parquet  (property x tax year market value, 2023 $; parquet_dta -> .dta for matched properties)
build/complete.do                 -> analysis/analysis.dta                 (property-year analysis set: panel + ATTOM property / permit links + ATTOM value + permit-based elevation (elevated_attom, elevation_retrofit, elevation_year, cost, funding); nfip_flip / nfip_icc kept, unused)
descriptives/summary_table.do     -> ../output/tables/summary_table.xlsx     (summary_table switch)
descriptives/histograms.do        -> ../output/figures/{claims,elevation_cost}_histogram.png + claims_vs_elevation_cost.png
build/alternates/attom_value_cells.py -> build/{state}_attom_value_{zip,county}_{year,decade}.dta
                                     (.sh = Torch/SLURM wrapper)
```

`clean_hma.do` builds the HMA elevation-grant universe (FMA, SRL, HMGP, PDM, BRIC, RFC) from **two** FEMA files: HMA Mitigated Properties (record level,
carries ZIP) with HMA Projects merged in `m:1` on `projectidentifier` (carries dollars, BCR, status).
The merge doubles as the funding screen — MitProps logs properties for applications that were never
funded, so the Projects status filter is what removes them. `prep_hma.do` then pools grants to ZIP and
county; `merge_nfip_hma.do` merges the county grain (covers all grants), ZIP stays as a finer option.

`attom_value_cells.py` aggregates raw ATTOM to ZIP/county × construction-year/decade value
cells — NFIP has no street address, so these merge property values onto the NFIP universe by cell.

`clean_permits.do` screens each state's ATTOM permits to true home elevations (Section 1 → `permit_states/`), then
appends and collapses them to one row per address (`clean/permit_elevations.dta`, keyed on
`street_address`, ATTOMID carried). The permit is joined to the ATTOM property record on its **ATTOMID**
(83-92% of permits carry one), else **1:1 on `street_address`** (`attom_permits.py`: six exact tiers
plus the loose house-number key and a Jaro-Winkler rung), and reaches NFIP from the ATTOM side via the
Wagner cell — not a zip/county pool; each match carries its tier in `permit_attom_match_tier`.
Permit feeds are by jurisdiction, so `clean_permit_coverage.py` counts all raw permits by county-year
(and ZIP-year, city-year) and `complete.do` flags covered county-years (`permit_covered_strict`): outside
them, or without an ATTOM match, a permit is unobservable and `elevated_attom` is missing, not 0. The
Builty chain (`extract_builty.py`, `clean_builty.do`, `geocode_builty.py`, `clean_builty_coverage.py`,
`attom_builty.py`) was the permit source until 2026-10-02 and sits in each stage's `archive/`; the
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
├── descriptives/             summary_table.do, histograms.do (master.do switches) + scratch/; Gen-1 in descriptives/archive/
├── slurm/                    shared cluster wrappers and reusable shell drivers
└── analysis/                 regressions, RD, identification (Gen-1 in analysis/archive/, await rebuild)

output/                       tables/ + figures/ — repo-root sibling of code/ (artifacts, not code; gitignored, regenerated by the pipeline)
archive/                      superseded data, outputs, drafts — repo-root sibling; not tracked
```

Every `archive/` folder (repo root, each code stage's `archive/`), `data/` and `output/` are
gitignored: archived files stay on disk and in git history but are not in the repo.

Data (Dropbox `Flooding/Empirical/Data/`): `raw → clean → build → analysis`, NOT under `code/`.

## Active data sources

ATTOM building permits (FL LA TX; Builty permits until 2026-10-02), ATTOM property values, FEMA **HMA (home-elevation grants, all programs)**, FEMA NFIP **claims** and
**policies**. **Dropped 2026-05-29:** NRI, NPR buyouts, ClimateRisk (old code in `clean/archive/`
and `build/archive/`).

## Merge logic & eligible universe

Each source contributes distinct columns: **NFIP policies** = elevation status/measures + insurance &
flood-zone context; **ATTOM** = exact address + property valuation; **HMA** = federal funding, merged at
**county** — its finest geography is ZIP, but grants FEMA never logged at property level carry no ZIP
at all, so only county covers every grant (ZIP kept as a finer option); **ATTOM permits** = permit-level
elevation events carrying ATTOM's property ID and an exact address, so joined to the ATTOM property
(ID, else address), then to NFIP from the ATTOM side. NFIP carries no exact address (lat/long are coarsened to ~1 decimal), so it is joined
by **fuzzy Wagner cells, not 1:1**. The relevant match is Wagner's **property match**
(`4_merge_all_houses.do`): cell = `{zip OR community} × construction-year × flood-zone × policy-year`
(zip primary, community fallback) — this is how NFIP links to ATTOM/permits (and `build_nfip_hma_panels.do`
mirrors it). The redacted policy file is **transaction-level** (~5–6 policy-years per structure);
dropping the policy-year from that cell deduped to *approximate structures*. (Wagner's separate
policy↔claims match adds `org_nb_dt`/`srl_ind`/`count_buy` — not used here.) Eligible universe =
**NFIP-insured single-family structures**; NFIP's own elevation flag + rated flood zone mean
**neither the permits nor NFHL are needed** for the structure-level universe — ATTOM is pulled in (fuzzily
linked) only for property-level valuation. **Caveat: NFIP miscodes elevations.** The flag
(`elevatedbuildingindicator`, insurer/self-reported) behaves as a stock, not a flow — only 2,998 of
313,156 flagged-elevated properties (~1%) show an observed transition — so elevation *events* and
their timing cannot be trusted from NFIP alone; the permit is the elevation event since 2026-10-02, and
the NFIP flip and ICC years ride along unused (see TODO.md).

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

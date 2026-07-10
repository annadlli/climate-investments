# TODO — climate-investments

_Last updated: 2026-07-10. Open handoff tasks + project state. Your coding agent surfaces the open
items when you open the project (see CLAUDE.md / AGENTS.md). Follow `CONVENTIONS.md` as you work._

## ⭐ THE MAIN THING: the whole pipeline must be reproducible from `master.do`

Any **data acquisition or cleaning that is part of the pipeline must run from `master.do`** — one
file, top to bottom. If a step only exists as a cluster job, a notebook, or a one-off, it is **not**
reproducible. Wire it in as a `.do`/`.py` step behind a `0/1` switch. This is the bar everything below serves.

## Clean up `torch_work/` (Anna)

A mix of deprecated and active code. Reorganize so nothing pipeline-relevant is stranded:
- **Deprecated files → `torch_work/archive/`** (or delete).
- **Split the rest into `clean/` or `build/` by purpose:**
  - data **acquisition** (Dewey/ATTOM/Builty pulls, raw downloads) → `clean/` (acquisition lives in
    the clean stage; see `import_dewey.ipynb` there).
  - data **construction** (strict filtering, ATTOM matching, panel building) → `build/`.
- Make each moved script reproducible from `master.do` (`.do` → `args data`; `.py` → `--data`; no
  hardcoded `/scratch/...` or `/Users/anna/...` paths in the logic).

**Cluster execution = thin wrapper, not separate logic.** The same scripts `master.do` calls should
run on Torch via a **cluster-specific shell/SLURM wrapper** that just supplies cluster resources/paths
— so each step runs both ways (locally via `master.do`, on Torch via the wrapper) from one source of
truth. Put that wrapper at the top level, **next to `master.do`** (e.g. `master_torch.sh`).

**Streamline + document the pipeline in `master.do`** — every construction step there, in order,
documented, behind a `0/1` switch (including whatever comes out of `torch_work/`).

## Pending / reconcile

- [ ] **Trim `import_dewey.py` (Anna).** It's a faithful byte-for-byte downloader — verified it never
      parses or casts values, so it is **not** the source of the ATTOM ZIP/FIPS leading-zero loss (that
      defect is already in the Dewey-delivered parquet). But it reads AI-generated: cut the 7-key
      `file_name_from_row` probe (deweydatapy returns a known schema) and the verbose docstrings/type
      hints (~30–40 lines). **Keep** the `--manifest` path (it's the real interface — `master.do` /
      `import.sh` invoke it) and the retry / `.part` / `PAR1`-validation logic (justified for the 44 GB
      cluster downloads).
- [x] **`nfip_build.do` archived (2026-06-19).** Its claims→county-year output was consumed only by
      Gen-1 `build/archive/` scripts (not by `build_nfip_hma_panels.do`, which builds from **policies**),
      and it expected claims vars `clean_nfip_claims.do` doesn't produce. Moved to `build/archive/`
      pending the parked ∆D / claims work. SFHA enumeration there is noted in `clean_nfip_policies.do`.
- [x] **`import_dewey.py` source cleanup.** The notebook has been converted to a convention-style
      `.py` script with `--data`. Real Dewey endpoint URLs and API keys are excluded from git; use
      placeholder endpoint values in `clean/import_dewey.py` and fill them only in a secure/local
      run context.

## NFIP data — claims / policies / multiple-loss (in progress, 2026-06-12)

Division of labor (decided this session): **policies** = eligible-homes universe (NFIP-insured
single-family → FMA-eligibility sample restriction); **multiple-loss** = RL/SRL status + mitigation
(FMA prioritization); **claims** = ∆D / avoided damages (parked until later). Mirror NFIP cleaning on
[Wagner's repo](../../Wagner_repository) (Dropbox `Flooding/Wagner_repository`) — see CLAUDE.md.

- [~] **`clean_nfip_claims.do`** — cleaned & **parked**. Single-family + match-key cleaning done
      (codebook-grounded; protects zero-padded keys from `destring`). Resume when building ∆D — needs a
      cell definition + a claims×policies exposure join.
- [x] **`import_nfip_policies.py`** — DONE & run (closes the old reproducibility gap). duckdb extracts
      Wagner's 20 states from the 29GB `FimaNfipPoliciesV2.csv` in one scan → per-state
      `clean/nfip_policies_raw/{st}.csv` (60.7M rows, 22GB; FL 21.9M, TX 11.3M, LA 8.3M). Wired into
      `master.do` (`import_nfip_policies` switch). Needs anaconda python (set `local python` to the full
      conda path — Stata's GUI PATH lacks conda; `pip install duckdb` done).
- [~] **`clean_nfip_policies.do`** — **mostly built (resume here).** Loops per-state raw CSVs →
      single-family screen → `property_id = group(geo · construction_year · originalNBdate)` where
      geo = census block group with ZIP fallback (beats Wagner's ZIP cell — ~0.7% collision on VA);
      `sfha` = first-letter `A`/`V` (verified complete on VA); drops SFHA homes + rows missing
      `property_id`; forces `elevated` monotonic within property; derives `policy_year` +
      `construction_year`. Lean keep set (property_id, geography, construction_year, policy_year,
      ratedfloodzone, elevated). **TODO:** finalize the `keep`/`save` (currently commented for
      interactive testing + a `stop`), then run on TX+VA.
- [~] **`clean_nfip_multiple_loss.do`** — import the MLP CSV
      (`fema.gov/api/open/v1/NfipMultipleLossProperties.csv`, ~240k rows) → `raw/`, **basic cleaning
      only**, save to `clean/`. **No sample restrictions** (it gets merged into policies, which already
      carry them). Use `fmaRl`/`fmaSrl` grant defs (not insurance defs) for RL/SRL status. The
      property/cell match moves to **build**, not here.
- [ ] **SFHA handling.** Considering **dropping SFHA** (isolate full-BCA value-bias; SFHA elevations can
      use flat pre-calc benefits → different rules). Treat as an analysis-stage choice — keep the
      flood-zone variable in the clean files so the sample stays filterable either way. SFHA is derivable
      from the NFIP flood-zone fields (`A/AE/AH/AO/V/VE`); the non-NFIP property universe (ATTOM/Builty)
      needs NFHL.
      **Decided 2026-06-19:** sample = NFIP-insured homes, so flood risk comes from the NFIP rated zone
      (`ratedfloodzone`/`floodzonecurrent`, with the derived `sfha` flag in `clean_nfip_policies.do`) —
      **NFHL not needed** (would only be required to tag the non-insured ATTOM/Builty universe).

## Build → property-level analysis dataset (planned 2026-06-19)

Goal: get to an analysis dataset fast for basic descriptives. Order of operations:

1. **`clean_nfip_multiple_loss.do`** (clean stage, above) — import + basic clean + save; no restrictions.
2. **Build ATTOM value cells** — `build/build_attom_value_cells.py` aggregates raw ATTOM parquet to
   ZIP×construction-year, ZIP×construction-decade, county×construction-year, and
   county×construction-decade value cells. Torch wrapper: `build/build_attom_value_cells.sh`.
3. **Compile property-level file** — **paused: don't edit `build/compile.do` for now.** Current
   `compile.do` remains the VA starter sketch. **TODO:** after ATTOM value-cell files exist, decide
   whether to extend `compile.do` or create a new build file for the NFIP + ATTOM value-cell merge.

Flags to resolve when building (from the merge-logic analysis):
- **OPEN: what's the best merge variable?** No shared `id` across NFIP files (0 overlap, by design).
  Best key = string `geo (block group / ZIP) · construction_year · originalNBDate` (~0.5% collision on
  VA, Wagner-style) — NOT the egen-integer `property_id` (numbered per-dataset, won't match across
  files). Open: is ~0.5% collision OK; tiered fallback for non-matches?; 1492 construction sentinel.
  **Next step: trial MLP↔policies merge, report match rate + 1:1 vs ambiguous.**
- **PREREQ for `compile.do`:** `clean_nfip_policies` must retain `originalnbdate` (currently dropped)
  so the NFIP base can build the string key; and re-run `clean_fma` (the saved `fma_elevation_grants.dta`
  is pre-snake_case / stale).
- **ATTOM is not a true property-key 1:1.** NFIP has no exact address (coarsened geo), so ATTOM links
  only at the **cell level** (zip/tract × construction-year). The "ATTOM 1:1" step is really a
  cell-level value join, not a parcel match. → property wealth ends up cell-level, not structure-level.
- **FMA grain.** `fma_elevation_grants` is project-level; the existing builder aggregates to county×year, and
  FMA has no property key to NFIP — so FMA attaches at **county (or county×year)**, not property. For
  one row per property, decide whether to aggregate FMA to county (1:1) or keep a property×year panel
  (the 1:m).

## Integrate HMA Mitigated Properties (property-level FMA)

We currently use only the **HMA Projects** file (`clean_fma.do` → `fma_elevation_grants.dta`),
which is project-level with **county** as its geographic floor. FEMA also publishes a
**property-level** companion — one row per mitigated structure:
`fema.gov/api/open/v4/HazardMitigationAssistanceMitigatedProperties` (99,255 records national;
5,882 are FMA). Fields: `programArea`, `programFy`, `projectIdentifier`, `propertyAction`
(Elevation vs Acquisition/Demolition/…), `structureType`, `foundationType`, `county`, `city`,
`zip`, `damageCategory`, `actualAmountPaid`, `numberOfProperties`.

- **Finest geography is ZIP** (`county`/`city`/`zip` only — no address/lat-long/block group;
  FEMA redacts sub-ZIP). So this file gets FMA from county → ZIP, but **cannot** reach the
  block-group grain targeted for the NFIP/ATTOM cells. ZIP-vs-county-vs-something-coarser for
  FMA is a design decision, not resolved here. (Note: ZIP was flagged as too coarse for the
  property-value merge; FMA is a funding/treatment-context measure, so its grain tradeoff differs.)
- **Complementary to Projects, not redundant.** This file has property-level elevation flags +
  ZIP + single-family type but **`actualAmountPaid` is only ~2% filled** (no dollars). Projects
  has the dollars/BCR at county. The two are **linkable by `projectIdentifier`** (in both), so
  project $ can be allocated onto the ZIP-located property records if both grain and $ are wanted.
- **Consistency (checked 2026-07-01): linkable but NOT count-consistent.** `projectIdentifier`
  joins ~90% of projects (1027/1071 Projects-elev IDs appear in MitProps; 978/1104 the other way).
  BUT MitProps under-reports: it holds only ~49% of the finalized elevations Projects records
  (national 3,001 vs `numberOfFinalProperties` 6,094; TX 264 vs 806 = 33%; VA 80 vs 128 = 63%).
  Not planned-vs-final (final ≈ planned). Known FEMA gap — not every mitigated structure is
  individually logged. → **Don't use MitProps as the authoritative elevation count.** Best use:
  Projects = authoritative county totals + $; MitProps = within-county ZIP *shares* + property
  attributes; allocate Projects totals to ZIP via those shares (join on `projectIdentifier`),
  caveat that the reported ~half is assumed geographically representative.
- **Scale is sparse:** FMA elevations = 3,001 national, **264 TX (71 zips) / 80 VA (25 zips)** —
  thin at zip×year; may force pooling years or falling back to county for power.

**TODO:** add the acquisition (endpoint → `raw/`) and a `clean_fma_mitigated.do` that filters to
`programArea=='FMA'` + `propertyAction` contains "Elevation" + `structureType=='Single Family'`,
then decides the aggregation grain (open). Wire into `master.do` behind a switch beside `clean_fma`.

## Expand state coverage beyond TX & VA (important)

Current scope is TX + VA. NFIP **policies** (all 20 Wagner states already extracted to
`clean/nfip_policies_raw/`), **FMA**, and **multiple-loss** are national/multi-state already — widening
them is mostly flipping `local states` in `master.do`. **The binding constraint is ATTOM:** only
`raw/attom_{tx,va}.parquet` are acquired, so more states need a **new ATTOM (Dewey) pull** via
`torch_work`/the cluster. Prioritize expanding ATTOM coverage.

## ATTOM geo enrichment is missing from the Dewey extract (Anna)

**The problem.** The ATTOM parquet extracts (`raw/attom/attom_{st}.parquet`) have their
census-geography and coordinate columns **100% empty**. Verified on VA (72.27M rows):
`CENSUSTRACT`, `CENSUSBLOCKGROUP`, `CENSUSBLOCK` (all `DECIMAL`) and `LATITUDE`/`LONGITUDE`
(all `DOUBLE`) have **zero** non-null values; `GEOQUALITYCODE` all blank. Only county FIPS,
ZIP, and street address are populated (address ~87% full, 100% house#+street; ~4.1M unique
`ATTOMID` parcels/state; `TAXMARKETVALUETOTAL` ~69%).

**This is a pull/product gap, NOT an ATTOM limitation.** ATTOM natively provides census
geo + lat/long (the columns exist in the 279-col schema). A clean 100%-empty across five
independent geo fields at once = the geocode/boundary enrichment module was never included
in the delivered feed. `import_dewey.py` downloads Dewey files wholesale (no column
filtering), so we received ATTOM's assessor/tax + address table without the geo enrichment.

**Why it matters.** With no census geo on the ATTOM side, `build_attom_value_cells.py` can
only aggregate to **ZIP/county × year** (median ~720 ATTOM homes per zip×year cell; 29% of
NFIP properties unmatched on VA). NFIP itself *has* block group (99.7% filled, 5,836 distinct
on VA), so if ATTOM carried block group we could merge at **block-group × construction-year**
— far closer to property-level and much less value-skew. The current coarseness is forced by
this gap, not a design choice.

**TODO (Anna) — pick one:**
- [ ] **Re-pull the ATTOM geo/boundary table from Dewey** (cleanest). Check the Dewey ATTOM
      catalog for the geocode / "enhanced GeoID" deliverable and pull it, joining on `ATTOMID`.
      May be a separate Dewey table or subscription tier. Fold into the `torch_work` ATTOM
      acquisition so the re-pull also covers the state-coverage expansion above.
- [ ] **OR geocode locally** (no re-subscription): dedupe ATTOM to unique `ATTOMID`
      (~4.1M/state), run addresses through the Census Bureau batch geocoder (free → returns
      tract + block group), attach back by `ATTOMID`. Then merge to NFIP's block group.

_Confirmed empty on VA + TX (Apr 13 pull). The 19-state Jun 27 batch is the same wholesale
pull / same 279-col schema — spot-check one June state before assuming the whole batch differs._

## Deflate nominal dollars to real (CPI)

All dollar amounts in the data are **nominal** (current-year), and the sources span
~1996–2023, so any cross-year comparison or pooling needs deflation to constant dollars.

**Built (2026-07-01):** `clean/clean_cpi.do` (raw `data/raw/cpi.csv`) → annual `clean/cpi.dta`,
rescaled to **base 2023** (`cpi = 1` in 2023; deflate with `real = nominal / cpi`); wired into
`master.do` (`clean_cpi` switch). `compile.do` deflates `fma_spend` by its window year
(`year_elev_min`).

- [ ] **Switch to the canonical series (BLS CPI-U, FRED `CPIAUCNS`).** Currently reuses the
      meatpacking file (OECD `CPALTT01USM661S` — tracks CPI-U closely but not the standard US
      choice). Swap the raw CSV + the `ren` column name in `clean_cpi.do`. **Do the same in the
      meatpacking project** so both use CPI-U consistently.
- [ ] **Guard incomplete years** in `clean_cpi.do`: the series ends mid-2025, so a 2025 annual
      average is a partial (Jan–Apr) figure — drop years with <12 months.
- [ ] **Deflate ATTOM property values** in `compile.do` once merged, by their value-cell year
      (`TAXYEARASSESSED`/`policy_year`) — same pattern as the FMA block.
- **Robustness option:** a construction-cost/PPI deflator may fit FMA spending better, and
  PCE / CPI-less-shelter avoids housing circularity for property values. General CPI-U is the
  documented default.

## Status / reference (done — for context)

- **Gen-1 build leftovers archived** → `build/archive/` (`merge_states/panel/nfip`, `merge_fema...`,
  `property_panel`, `merge_on_exact`, `property_data` + `nri_prep`, `merge_npr`). **Held:**
  `nfip_clean.do` (see reconcile). `weather_dewey_import.ipynb` (redundant dup) → `clean/archive/`.
- **`analysis/` + `descriptives/` are all Gen-1, archived** (they read data the pipeline no longer
  produces) → their `archive/` subfolders. Need rebuilding on the current `*_nfip_hma` panels. Closest
  to salvageable: `descriptives_all_states_fema.py` (`all_elevation_strict_filtered_fema.dta` →
  `build/all_builtyelevations_fema_npr.dta`).
- **Path convention:** `master.do` sets `local code` + `local data`; passes data to each child via
  `args data` (no fallback); Python steps take `--data`. See CONVENTIONS.md §3.
- **Vendela's cleanup** (folder reorg, args-pass, banners, archiving) is working-tree / branch
  `gen1-cleanup`, **not pushed** — coordinate before pulling so it doesn't clobber your work.

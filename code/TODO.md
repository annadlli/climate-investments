# TODO — climate-investments

_Last updated: 2026-06-19. Open handoff tasks + project state. Your coding agent surfaces the open
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

## Expand state coverage beyond TX & VA (important)

Current scope is TX + VA. NFIP **policies** (all 20 Wagner states already extracted to
`clean/nfip_policies_raw/`), **FMA**, and **multiple-loss** are national/multi-state already — widening
them is mostly flipping `local states` in `master.do`. **The binding constraint is ATTOM:** only
`raw/attom_{tx,va}.parquet` are acquired, so more states need a **new ATTOM (Dewey) pull** via
`torch_work`/the cluster. Prioritize expanding ATTOM coverage.

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

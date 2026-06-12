# TODO — climate-investments

_Last updated: 2026-06-12. Open handoff tasks + project state. Your coding agent surfaces the open
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

- [ ] **`filter_builty_strict.py` / `attom_onto_permits.py`** (Anna's tea-spill files) — push the
      revised versions; they're the two `master.do` steps marked PENDING. Vendela should not edit them locally until then (`git fetch && git checkout origin/main -- build/filter_builty_strict.py build/attom_onto_permits.py`).
- [ ] **`nfip_build.do` ↔ `clean_nfip_claims.do` mismatch.** `nfip_build.do` uses `damage_ratio`,
      `got_icc`, `bldg_damage_amt`, etc. that `clean_nfip_claims.do` doesn't produce (the old
      `nfip_clean.do` computed them). Fold that logic into `clean_nfip_claims.do` (or before
      `nfip_build.do`), or drop `nfip_build.do` (its output isn't consumed by `build_nfip_hma_panels.do`).
      Also confirm the intended NFIP path: panels use **policies**, `nfip_build` uses **claims**.
- [ ] **`import_dewey.ipynb`** — convert the notebook to a `.py` script, and rename off "Dewey" to the
      actual underlying data source.

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
- [~] **`clean_nfip_policies.do`** — **NEXT (resume here).** Rewrite to loop the per-state
      `nfip_policies_raw/{st}.csv` and clean (mirror `clean_nfip_claims.do`: single-family
      `occupancytype` in 1/11, destring protecting zero-padded keys, dates→years, label/order/sort) →
      `clean/nfip_policies_{st}.dta`. Raw header is the 77 policy cols (lowercased/truncated on import).
      NOTE: policies `countyCode` is already **5-digit FIPS** (e.g. 51710), unlike claims' 3-digit.
      FL import ~5 min one-time (measured; accepted). Eligible-universe = distinct insured single-family
      structures, but the dedup (policy-years → structures) belongs at **build**, not in this clean step.
- [~] **`clean_nfip_multiple_loss.do`** — **in progress** (sketch committed). Download the MLP CSV
      (`fema.gov/api/open/v1/NfipMultipleLossProperties.csv`, ~240k rows) → `raw/`; clean to an RL/SRL +
      mitigation roster (use `fmaRl`/`fmaSrl` grant defs for FMA prioritization, not the insurance defs);
      cell-match onto the eligible universe. Wire into `master.do` behind a `0` switch once tested.
- [ ] **SFHA handling.** Considering **dropping SFHA** (isolate full-BCA value-bias; SFHA elevations can
      use flat pre-calc benefits → different rules). Treat as an analysis-stage choice — keep the
      flood-zone variable in the clean files so the sample stays filterable either way. SFHA is derivable
      from the NFIP flood-zone fields (`A/AE/AH/AO/V/VE`); the non-NFIP property universe (ATTOM/Builty)
      needs NFHL.

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

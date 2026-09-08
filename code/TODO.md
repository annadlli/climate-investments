# TODO — climate-investments

_Rewritten 2026-09-02, updated 2026-09-03 and 2026-09-07 (Anna) against the live `master.do`. Pre-September history (torch_work cleanup,
build/ consolidation, NFIP/FMA/CPI build notes) is in git: `git show 10dffe3:code/TODO.md`.
Follow `CONVENTIONS.md` as you work._

## Standing rule: everything runs from `master.do`

Any acquisition or construction step must run from `master.do` behind a `0/1` switch. Cluster
execution is a thin wrapper in `slurm/` around the same script, never separate logic.

## 0. In progress — end of day 2026-09-03

Pipeline state on disk (all rebuilt 2026-09-03 unless noted):
- `clean/nfip_policies_state/{st}.dta` (17:00–17:35), `clean/nfip_policies_{panel,property}.dta` (18:10),
  `clean/fma_{zip,county}.dta`, `build/nfip_hma_panel.dta` (18:27), `analysis/analysis.dta` (19:43).
  These predate the last two cleaner edits below, so the chain needs one more full run.
- **Rerun needed** (in order): `clean_nfip_policies` → `prep_nfip_policies` → `merge_nfip_fma` → `complete`.
  Two edits since the last run: `clean_nfip_policies.do` drops policies with no block group (129,110
  policy-years, 45,508 properties, 0.5%) and builds property_id on block group + the two dates, so the
  claims merge is 1:1; `clean_builty.do` got a narrow tree-permit fix (subtype mentioning trees/pruning
  now killed unconditionally; "tree lifting/leaning", "raise canopy", "prune" added to the description
  kill) — rerun `clean_builty` too (its state files date from Jul 23).
- Don't save `master.do` or a running do-file while a batch job launched from it is running: Stata
  reads do-files incrementally and picks up a shifted byte offset (two runs died this way today).
- `summary_table.do` lists cross-section variables (premium_init etc.) that the property-year
  `analysis/analysis.dta` lacks; revise its variable list before running it.
- `complete.do` extract: the TX/FL/LA 50% draw is 1.5 GB; a cutoff near 0.17 gives ~500 MB. Not seeded.
- ATTOM market value on the panel needs cleaning: ≥5% of matched properties carry exactly 0, max
  1.85e9. Zeros → missing, inspect the top tail, deflate (see item 3).
- [x] Anna, 2026-09-07: reviewed the 09-03 matching changes; matching resubmitted on the cluster for
  FL LA NJ TX from step 3 (`--from 3`, cached geocode + NFHL reused). `master.do` default `states` is
  now FL LA NJ TX (20-state list kept in a comment). New since 09-06: `clean_builty.do` keeps elevated
  new construction with `new_construction` / `retrofit` flags (retrofit counts unchanged);
  `geocode_builty.py` returns block group + coordinates for every permit; `attom_builty.py` backfills
  coordinates, block group and NFHL zone for Builty-elevated houses ATTOM could not place;
  `nfip_attom.py` coalesces the block group and carries `builty_retrofit`, `builty_new_construction`,
  `builty_geo_backfilled`; `complete.do` keeps those plus `match_tier_number`.

## 1. Canonical path — settled 2026-09-03

One route: `merge_nfip_fma.do` (was `compile.do`) → `complete.do` (absorbed `final_data.do`, now in
`build/archive/`). `compile2.do` and the cell-level alternative are gone. Match diagnostics from the
first full run: 86.5% of properties get an ATTOM property (LA 61%, FL/PA/SC ~86%, most others >99%);
31% of assignments are tier 1, 30% are tiers 11–15 (no flood zone), tier 15 alone supplies 19% of
elevated matches. The pairing is arbitrary within a cell, so the analysis is cell-grain in substance.

## 2. Builty screen — validated 2026-09-03, deferred

Read-through of killed candidates in FL/TX/LA (`clean_builty.do`; harness and the 21,606-row
killed-but-likely file were in the session scratchpad, rebuild from `clean/builty_raw/{st}.csv`):
- Recall: LA loses ~100 true elevations (Jefferson Parish "residential elevation / elevate existing
  residential structure" — "residential" not in the N noun list and the act window of 30 chars is too
  short; Lafourche "raised house" filed under a new-construction subtype). TX recall near complete.
- Precision is the bigger problem, in FL: ~2 of 12 random survivors are house elevations; the rest are
  raised slabs/foundations on new construction, utility elevations, boat lifts, elevation-certificate
  submittals.
- The extraction net in `extract_builty.py` is complete (no elevation phrasing outside it; the dropped
  WORK_TYPES/ATTRIBUTES/PROJECTS fields carry nothing). ~1,200 LA permits describe house relocations —
  a separate outcome if ever wanted.
- [x] Retrofit vs elevated-new-construction differentiator -- done 2026-09-06: the new-construction
      kill in `clean_builty.do` is now the `new_construction` flag (FL 633, LA 108, TX 66, NJ 0 new builds
      kept; retrofit counts identical to before). Caveat: FL's raised-slab-on-new-build false positives
      now sit inside `new_construction = 1`, so that bar overstates true elevated new builds until the
      tightening below. `WORK_TYPES` is *not* empty (filled for 76% of survivors) but "New" there means a
      new permit, not a new building (1,521 retrofits carry it), so it cannot define new construction.
      ATTOM year built can: on the Aug links, 78 "retrofit" permits sit on houses built within a year of
      the permit. Layer permit_year <= YEARBUILT + 1 into `builty_new_construction` in `attom_builty.py`.
- [ ] Tighten the screen: N += residential|res|bldg|sfd, act window 30 → 45, "raised house" in strong;
      precision kills for raised slab/foundation on new builds, utilities/meters/condensers, boat lifts,
      EC-only permits; also a street named "Raising Hill Dr" trips the act regex. Hand-label ~300
      survivors and ~300 killed-but-likely in FL for a precision/recall number before and after. Any
      change here forces matching steps 3-4 to rerun. Deferred until the ATTOM permit-data quote is in.

## 3. Analysis-facing construction

- [x] NFIP in two grains — done 2026-09-03: `prep_nfip_policies.do` writes the property × policy-year
      panel (50.4M policy-years, property-year duplicates dropped in the cleaner) and a first-policy-year
      snapshot per property for the matcher. property_id (cross-state) and property_id_state are both kept.
- [x] NFIP premiums — done in `clean_nfip_policies.do`: premiums ≤ 0 set to missing, premium /
      policy_cost / coverage_building deflated to 2023 $ at policy-year level. Still open: policy years
      running to 2027 (date parsing) — check `policy_year` range.
- [ ] Re-do HMA cleaning to keep all elevation programs (HMGP, BRIC, FMA, …) with a program flag;
      restrict to FMA downstream.
- [ ] Keep unfunded/denied/withdrawn applications with a `funded` flag instead of dropping them,
      so self-financed elevations can be measured (Builty elevation with no grant match). Flag
      local recovery programs (e.g. NYC Build It Back, `funding_type == 5` in `clean_builty.do`)
      before calling an elevation self-financed.
- [x] Builty → ATTOM → NFIP frequency loss (Anna; diagnosed 2026-09-03, fixed 2026-09-06). Of 9,853
      Builty elevations, 7,089 matched an ATTOM address and 5,576 reached an NFIP property.
      (a) NYC address normalization: dropped, New York left the sample with the four-state default.
      (b) Done: `geocode_builty.py` geocodes every permit (4,922 of 5,945 get a block group);
          `attom_builty.py` backfills coordinates, block group and NFHL zone (`--nfhl`) for matched
          houses ATTOM could not place; `nfip_attom.py` coalesces the block-group key so they enter
          tiers 1-4. `match_tier_number` and `builty_geo_backfilled` are on the panel. Check the
          step-3 "backfilled" line and step-4 tier counts in the cluster logs when the rerun lands.
- [ ] Tighten the Builty coverage threshold (currently any permit in the county-year). Options, in
      order of effort: (a) DONE 2026-09-07, floor still to choose: `clean_builty_coverage_rate.py` adds
      `attom_n_sf`, `builty_per_100`, `builty_covered_strict` (default floor 1 per 100) to
      `clean/builty_coverage_county.dta`, carried onto the panel by `complete.do` without restricting
      yet. Observed medians 2010-24: FL 17, TX 8, NJ 6, LA 4 per 100; a floor of 1 keeps 1,856 of
      2,407 county-years. (b) add the ZIP-year index
      (`clean/builty_coverage_zip.dta`) as a strict tier: county covered and the property's own ZIP
      shows permits that year (conservative, since a third of permits lack a ZIP); (c) municipal
      matching -- Builty `LOCALITY` to the NFIP rated community number via FEMA's Community Status
      Book, so coverage is flagged at the grain the feeds exist (needs name matching; locality names
      are noisy in FL and VA). `nfipratedcommunitynumber` is dropped in `merge_nfip_fma.do`; keep it
      if (c) goes ahead.
- [ ] Further sample restrictions in `complete.do` Section 3 (Builty coverage is there now): SFHA
      and FMA eligibility, leaving `build/nfip_hma_panel.dta` as the unrestricted universe. Whether to
      also restrict on `attom_matched` is open — keep as a flag unless the analysis is matched-only.
- [x] Deflate ATTOM property values -- done 2026-09-06 as a wide file: `attom_value_wide.py` writes
      `build/attom_value_wide/{st}_attom_value_wide.parquet`, one row per ATTOM property, market value
      by tax year in 2023 $, zeros and values above $100M set to missing; `attom_value_dta` converts the
      NFIP-linked subset to Stata after the matching. Still open: merge it onto the panel in
      `complete.do` and reshape long, and decide what to do with the 23% (LA) of property-years ATTOM
      logs as exactly zero. `attom_market_value_total` on the panel is still nominal until then.

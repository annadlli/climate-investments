# TODO — climate-investments

_Rewritten 2026-09-02, updated 2026-09-03 against the live `master.do`. Pre-September history (torch_work cleanup,
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
- Anna: `nfip_attom.py` now reads only `clean/nfip_policies_property.dta` (first-policy-year snapshot
  from `prep_nfip_policies.do` Section 2). `--state-policies` and the NFHL snapshot mode are removed;
  `run_property_matching.sh` no longer passes the state file; `geocode_builty.py` moved to `clean/` and
  writes `clean/builty_elevations_zipfilled.dta` (its Gen-1 `all_builty_elevations` backfill removed).
  Review, then rerun the matching on the cluster against the rebuilt snapshot. Today's link merge
  matched all 50.4M policy-years on `state property_id_state`, so the Aug 31 links still align.
- Anna: archives and `data/` are untracked (gitignored) and several scripts were renamed/moved — pull
  carefully; copy anything wanted from local archive folders first.

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
- [ ] Add a retrofit vs elevated-new-construction differentiator, then tighten: N += residential|res|bldg|sfd,
      act window 30 → 45, "raised house" in strong; precision kills for raised slab/foundation on new
      builds, utilities/meters/condensers, boat lifts, EC-only permits. Hand-label ~300 survivors and
      ~300 killed-but-likely in FL for a precision/recall number before and after.

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
- [ ] Builty → ATTOM → NFIP frequency loss (Anna; diagnosed 2026-09-03). Of 9,853 Builty
      elevations, 7,089 match an ATTOM address and 5,576 reach an NFIP property. Two fixes, neither
      touching the cell method:
      (a) `attom_builty.py`: New York City addresses never match. Builty writes "42 WEST 12 ROAD
          QUEENS" (borough appended, no ZIP, no county FIPS); ATTOM writes "101 W 12TH ST". Strip
          the borough word, normalize ordinals, and derive county from the borough so the
          no-ZIP county tier can fire. 822 of NY's 1,207 NYC permits are unmatched (30% state match
          rate vs 85-92% elsewhere).
      (b) `geocode_builty.py` / `attom_builty.py`: all 1,513 address-matched elevated houses that
          never reach NFIP have no Census block group on the ATTOM side (geocode failed), hence no
          NFHL flood zone, so they only see the ZIP/county tiers after the NFIP slots are taken.
          Have the Builty geocode return block group + coordinates and backfill the ATTOM record
          for matched houses; they then enter tiers 1-4 where Builty-first ranking protects them.
      Also carry `match_tier_number` onto the panel so analysis can restrict to tiers 1-4.
- [ ] Tighten the Builty coverage threshold (currently any permit in the county-year). Options, in
      order of effort: (a) benchmark against the housing stock -- Builty permits per 100 ATTOM
      residential properties by county-year; a full feed runs roughly 5-15, a partial municipal feed
      1-3, a trickle near 0 -- and set the floor as a rate; (b) add the ZIP-year index
      (`clean/builty_coverage_zip.dta`) as a strict tier: county covered and the property's own ZIP
      shows permits that year (conservative, since a third of permits lack a ZIP); (c) municipal
      matching -- Builty `LOCALITY` to the NFIP rated community number via FEMA's Community Status
      Book, so coverage is flagged at the grain the feeds exist (needs name matching; locality names
      are noisy in FL and VA). `nfipratedcommunitynumber` is dropped in `merge_nfip_fma.do`; keep it
      if (c) goes ahead.
- [ ] Further sample restrictions in `complete.do` Section 3 (Builty coverage is there now): SFHA
      and FMA eligibility, leaving `build/nfip_hma_panel.dta` as the unrestricted universe. Whether to
      also restrict on `attom_matched` is open — keep as a flag unless the analysis is matched-only.
- [ ] Deflate ATTOM property values: `complete.do` carries `attom_value_year` but never merges
      `clean/cpi.dta` (base 2023, `real = nominal / cpi`). Add the merge on `attom_value_year`, same
      pattern as the FMA block in `clean_fma.do`; clean zeros and the top tail first (item 0).

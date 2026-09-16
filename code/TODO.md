# TODO — climate-investments

_Rewritten 2026-09-02, updated 2026-09-03, 2026-09-07, 2026-09-10 (Anna) and 2026-09-15 (Vendela) against the live `master.do`. Pre-September history (torch_work cleanup,
build/ consolidation, NFIP/FMA/CPI build notes) is in git: `git show 10dffe3:code/TODO.md`.
Follow `CONVENTIONS.md` as you work._

## Standing rule: everything runs from `master.do`

Any acquisition or construction step must run from `master.do` behind a `0/1` switch. Cluster
execution is a thin wrapper in `slurm/` around the same script, never separate logic.

## 0. State of the pipeline — 2026-09-15

- Panel is three-state end to end (FL LA TX): `prep_nfip_policies` → `merge_nfip_hma` → `complete`
  rerun 09-15. The 20-state per-state clean files are untouched; widening means rerunning from
  `prep_nfip_policies`, not just `complete` (the state re-filter that used to sit in `complete.do` is gone).
- ATTOM value: `attom_value.py` (was `attom_value_wide.py`) writes one row per NFIP-matched ATTOM
  property per year 2000-2026, `build/attom_value/{st}_attom_value.parquet`, columns `value_own`
  (that tax year), `value` (carried from the nearest tax year, most recent first) and `value_year`;
  `attom_value_dta` converts all states to one `attom_value.dta`. `complete.do` merges `value` as
  `attom_value` (was `attom_value_2023`; the suffix meant 2023 dollars, not the 2023 tax year) in one
  `merge m:1` on ATTOM id x policy year. The 27-column wide file and the year loop are gone.
- `complete.do` (end of day 09-15) merges links, value and coverage, then builds only the harmonized
  elevation: `elevated` (NFIP flag, or 1 from the harmonized year on), `elevation_retrofit` (event
  observed in sample), `elevation_year` (Builty permit, else NFIP flag flip `nfip_flip`, else ICC
  payment `nfip_icc` if the property stays on the panel afterwards, since ICC also pays for
  demolition), `elevation_source`, `elevation_funded` (grant named in the permit text, or an ICC
  payment), `elevation_cost` (was `builty_project_value`; Builty declared value, FL and TX). The
  NFIP flip and ICC years are built in `merge_nfip_hma.do`. `builty_elevated` is set missing where a
  permit is unobservable (no ATTOM match, or no Builty feed in the county-year). No row is dropped
  for coverage; `builty_covered_strict` rides along as a flag and the sample rule lives in the
  analysis script. Unmatched NFIP properties stay (16% overall, 39% of LA). The diagnostics file
  is gone; Anna can rebuild diagnostics from the link files if needed.
- Claims (09-15): `property_id` is block group x construction date x NB date, so several structures
  can share one id and their claims sum. `clean_nfip_claims.do` deflates coverage, keeps `n_records`,
  flags `claim_over_coverage` and caps the property-year payout at building + contents coverage;
  `merge_nfip_hma.do` carries the property-level `claim_collision`; `complete.do` drops those
  properties (5,449 of 7.1M). Collided ids also carry one structure's premiums and coverage for
  several; only a finer id would fix that, and the redacted file has none.
- Builty cost (09-15): the screen moved into `clean_builty.do` and is by permit type, not by dollar
  threshold: the declared value is set missing on new-construction permits (71 of the 80 values
  over $1m were new builds) and below $1k (fee and document lines). No ceiling. Any change there
  means rerunning geocode_builty and matching steps 3-4. `fund_sfha` is gone from the funding
  cues (it matched a FL permit-type prefix, not a funder).
- FMA → HMA rename (09-15): `clean_hma.do`, `prep_hma.do`, `merge_nfip_hma.do` and the `hma_elevation` /
  `hma_zip` / `hma_county` files pool every HMA elevation program (HMGP 13,145 properties, FMA 4,269,
  SRL 1,097, PDM/BRIC/LPDM/RFC 336); `programarea` stays on the clean file. Panel carries
  `hma_n_properties`, `hma_spend`, `hma_year_min`, `hma_year_max`. Federal spend per elevated
  property across the 1,937 funded projects: median $130k, total spend over total properties $176k.
  The old `fma_*.dta` files are still on Dropbox; Anna's scratch scripts read the old names.
- Builty retrofit vs new-construction distinction is not carried into `complete.do` (09-15): every
  Builty permit counts as an elevation (`builty_elevated`); `builty_retrofit` and
  `builty_new_construction` stay in the link files for diagnostics. `es_prices_mitigation.do` still
  reads `builty_retrofit` and needs `builty_elevated` instead.
- [ ] Funding variable (09-15): `elevation_funded` only sees grant language in Builty permit text, and
      the county HMA rate `hma_p` was dropped as uninformative (constant within county, 30-year
      numerator against a 2009+ denominator). Think through one funding measure that combines the
      Builty text cue, an ICC payment (NFIP money for the elevation) and county HMA exposure, and
      say what "self-financed" means against it. Ties to the unfunded-applications item below.
- [ ] LA ATTOM valuation (09-15): the `market_value_total` field is far below market in several
      parishes. 2022 medians: Caddo $39k, Ouachita $32k, Rapides $40k, Calcasieu $61k, each with
      17-29% of properties under $10k nominal, against $123-147k in East Baton Rouge, Jefferson,
      Orleans, St. Tammany. Louisiana assesses residential property at 10% of market, so the field
      may carry an assessed-type figure in those parishes. The $10k floor is not the fix. Check
      `assessed_value_total` x 10 against `market_value_total` by parish and decide which to use
      for LA before any LA value result is shown. Statewide LA real median is $105k vs FL $190k, TX $170k.
- [ ] Rerun in progress at end of day 09-15: clean_builty → geocode_builty → matching steps 3-4 for
      FL LA TX locally (`--from 3`, 8 GB) → `parquet_dta` → `complete` → `summary_table` →
      `histograms`. Everything upstream of the matching (HMA chain, claims, `merge_nfip_hma`, the
      value file) is rebuilt. Check the link-file dates, the value merge rate in the `complete` log,
      and the `elevated` / `elevation_retrofit` / `elevation_cost` rows of the summary table.
      `histograms.do` now also draws `claims_vs_elevation_cost.png` (cost against claims paid
      through the elevation year, 45-degree line); the two histograms keep a $1k display floor.
- [x] SUPERSEDED (merge 09-15, Vendela's version kept): Anna's ATTOM side restructure (Anna: "one ATTOM file and merge"; the
      per-state wide value files and the 27 one-year merges were swapping a 16 GB laptop).
      `build/attom_stata.py` (one `attom_stata` switch in `master.do`, replaces `parquet_dta`,
      `attom_value_wide`, `attom_value_dta`) writes `build/attom_links.dta` (7.1M NFIP properties,
      all sample states: assigned ATTOM ID as an integer, match tier, Builty flags) and
      `build/attom_value.dta` (assigned ATTOM ID x year, 2009-2025, market value in 2023 $,
      ~79M rows / 0.8 GB). `complete.do` merges each once, the value after the sample
      restriction. `complete.do` on the new files reproduces the 09-14 analysis set exactly
      (same 29.2M rows, same matched / Builty counts; the value file equals the old wide
      files on every assigned ID x year). Upstream, `nfip_attom.py` picks the value with a
      filtered join instead of a per-property subquery (LA output identical, 0 rows differ),
      and `run_property_matching.sh` step 3 now matches permits against the geocoded panel
      instead of the raw extract, so the 40 GB FL/TX raw files are never read after step 1.
      Cost, measured on LA against the raw input with today's permits: 2 of 1,549 permit
      matches are lost (218 Plant Rd, Houma, exact; 8312 Lake Park Dr, Denham Springs,
      Jaro-Winkler) because the panel keeps one address spelling per property-year
      (`attom_geocode.py` takes the max) and the spelling that matched sits on another raw
      row; one of the two properties is NFIP-linked, so one Builty flag leaves the LA links
      at the next rerun. `builty_built_at_permit` becomes missing rather than 0 where ATTOM
      has no year built (the flag is not on the panel). Everything else in the step 3
      outputs is identical. Anna's call whether 0.1% is worth the raw dependency; to go
      back, point step 3's `--attom` at `RAW_ATTOM` again. Takes effect on the cluster at
      the next step 3-4 run; the production link parquets are unchanged.
      `attom_value_wide.py` and `parquet_dta.py` are in `build/archive/`; the Dropbox folders
      `build/attom_value_wide/` and `build/nfip_attom_property/*.dta` are no longer read.
      `attom_stata.py` is in `build/archive/`; `parquet_dta.py` stays in `build/`; the `nfip_attom.py`
      filtered join and the step-3 geocoded-panel read in `run_property_matching.sh` were kept.
- Don't save `master.do` or a running do-file while a batch job launched from it is running: Stata
  reads do-files incrementally and picks up a shifted byte offset (killed a run again 09-15).
- `complete.do` extract: the 10% draw to `analysis/extracts/500M_subsample.dta` is not seeded.
- [x] Anna, 2026-09-07: reviewed the 09-03 matching changes; matching resubmitted on the cluster for
  FL LA NJ TX from step 3 (`--from 3`, cached geocode + NFHL reused). `master.do` default `states` is
  now FL LA NJ TX (20-state list kept in a comment). New since 09-06: `clean_builty.do` keeps elevated
  new construction with `new_construction` / `retrofit` flags (retrofit counts unchanged);
  `geocode_builty.py` returns block group + coordinates for every permit; `attom_builty.py` backfills
  coordinates, block group and NFHL zone for Builty-elevated houses ATTOM could not place;
  `nfip_attom.py` coalesces the block group and carries `builty_retrofit`, `builty_new_construction`,
  `builty_geo_backfilled`; `complete.do` keeps those plus `match_tier_number`.
- [x] Claude change 09-10: state sample settled (issue #16, Vendela 09-08): `states` is FL LA TX; NJ dropped for lack of
  Builty coverage. The 09-08 six-state widening (NC NY) is reverted; NC and NY link files on disk
  are the stale Aug 17 versions and are not used.
- [x] Claude change 09-15: Builty cost first cleaned in `complete.do` with a $1k-$1m window; superseded
      the same day by the permit-type screen in `clean_builty.do` (section 0).
      `histograms.do` now draws both panels from `analysis.dta` on one x-axis.
- [x] Claude change 09-10 (done 09-14, redone 09-15 with the loose rungs): rerun for the 09-10 pipeline changes (issues #23-#26), in order:
  `clean_nfip_claims` (adds `claim_icc`; note the `stop` Vendela left before the claims cap) →
  `merge_nfip_fma` (carries `claim_icc`, `hmgp_n_properties`) ← `prep_fma` (HMGP county counts);
  on the cluster, matching from step 3 for FL LA TX (`FROM_STEP=3 sbatch --array=3,5,17
  code/slurm/submit_property_matching.sh`) so `builty_project_value` / `builty_funding_type` reach
  the link files, then copy `nfip_attom_property/*.parquet` back → `parquet_dta` → `attom_value`
  (new $10k floor) → `attom_value_dta` → `complete` (now writes `analysis.dta` and
  `analysis_with_diagnostics.dta`) → `summary_table`, `es_prices_mitigation`. `complete.do` fails on
  the current link files because they lack the two new Builty columns. LA steps 3-4 were run locally
  on 09-10 as a test of the Python changes (scratch output, not copied to Dropbox).

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
- [x] Re-do HMA cleaning to keep all elevation programs (HMGP, BRIC, FMA, …) with a program flag;
      restrict to FMA downstream -- done 09-08 (`clean_fma.do` keeps every program, `programarea` is
      the flag; `prep_fma.do` restricts to FMA/SRL for the ZIP and county files). 09-10: the county
      file also carries `hmgp_n_properties`, merged onto the panel for the HMA probability below.
- [x] Claude change 09-10: harmonized elevation variable (issue #25) -- built in `complete.do` Section 3, 09-10:
      `elevation` = NFIP flag flips 0 -> 1 or an ICC payment (`claim_icc`, new in
      `clean_nfip_claims.do`) or a Builty retrofit permit; `elevation_year`, `elevation_source`
      (0 none / 1 NFIP / 2 Builty / 3 both), `elevation_funded` (grant named in the permit text,
      Builty elevations only); `hma_p_fma`, `hma_p_hmgp` = grant-elevated properties per NFIP-insured
      property in the county (soft HMA signal until the FOIA property lists arrive). On the LA 10%
      extract: 97 Builty-only, 12 NFIP-only, 0 both -- the sources barely overlap, worth a slide.
      Open: the ICC signal is untested until the claims rerun; whether to treat `elevated == 1` at
      first observation (never flips) as a pre-sample elevation.
- [x] Claude change 09-10: Builty elevation cost on the panel (issue #24) -- `builty_project_value` (2023 $) and
      `builty_funding_type` ride from `clean_builty` through `attom_builty.py` → `nfip_attom.py` →
      `complete.do`; land with the step 3-4 rerun. Only FL and TX permits report a value (1,375 and
      282 of the retrofit permits); LA reports none.
- [x] Claude change 09-10: `analysis.dta` declutter (issue #23) -- `complete.do` now saves `analysis_with_diagnostics.dta`
      (everything) and then drops `builty_new_construction`, `match_tier_number`,
      `builty_geo_backfilled`, `builty_per_100`, `builty_funding_type`, `n_nfip_county`,
      `attom_market_value_total`, `attom_value_year` and the source-specific elevation flags before
      saving `analysis.dta`. `builty_covered_strict` stays because `es_prices_mitigation.do` uses it.
- [ ] Claude change 09-10: LA construction year (issue #26): 59% of LA single-family ATTOM properties (783k of 1.33M)
      have no year built (TX 4.5%, FL 0.3%); NFIP properties all have one. The ladder already uses
      the year where both sides have it and falls to tier 15 (block group x zone, no year) only for
      leftovers: LA lands 276k of 704k matches and 1,081 of 1,218 Builty retrofits in tier 15.
      Proposal: move the no-year block-group x zone tier ahead of tiers 11-14 (the no-flood-zone
      tiers), so a same-block-group, same-zone match beats a county x year match where the ATTOM
      year is missing. Changes matches in every state; decide before the next full rerun.
      Claude change 09-13: `build/alternates/nfip_attom_la_noyear.py` runs the parish check and the
      reordered ladder for LA only, to a separate file, and compares it with the baseline. Test
      script, run by hand (see its docstring); not in `master.do` unless it becomes part of the
      final product. Result: the reorder does not help. Jefferson
      Parish (263k NFIP properties, 758 of 1,300 Builty retrofits) has no ATTOM year built at all,
      so nothing there can be displaced; the reorder moves 85k of 1.24M assignments and puts one more
      retrofit on the panel (1,218 -> 1,219). Leave the ladder as is. The LA problem is ATTOM's
      year coverage, not the match order; write-up in Dropbox `Flooding/Notes/la_year_built_2026-09-13.md`.
- [ ] Claude change 09-14: Builty → ATTOM address match, two looser rungs tested in
      `build/alternates/attom_builty_fuzzy.py` (test script, run by hand; review listings in
      `tmp/fuzzy/`). Rung 7, house number + first street word + ZIP, unique in the ZIP: LA 85.4 →
      92.8%, FL 73.4 → 83.8% (Aug 17 permit file), TX 90.9 → 94.5%. Rung 8, Jaro-Winkler on the
      street string blocked on house number + ZIP: at most one point more. Two wrong-street pairs
      in 433 ("1001 ave e" → "avenue k"): when the first street word is a generic (ave, st, hwy,
      highway, county, la, fm) the key should take the next word too. Built into
      `attom_builty.py` as `--loose-tiers` (off by default), with that fix and a direction check;
      `run_property_matching.sh --loose` / `LOOSE=1 sbatch ...` write `_loose` copies of the step
      3-4 files, and `master.do`'s `matching_loose` switch carries the `_loose` suffix through
      `parquet_dta`, `attom_value_dta` and `complete.do` (writes `analysis_loose.dta`), so the
      production files are never overwritten. Cluster run 09-14, all three states: permits matched
      FL 75 → 86%, LA 85 → 92%, TX 90 → 95%; retrofit houses on the NFIP panel 3,361 → 3,679.
      Claude change 09-15: **adopted as production** (Anna). The loose rungs are the default in
      `attom_builty.py` (`--exact-only` runs the old ladder); the `--loose` / `LOOSE=1` options,
      the `matching_loose` switch and the `_loose` suffix are retired; the `_loose` link, value
      and panel files were renamed over the production names. The exact-tier link parquets still
      exist on the cluster if ever needed. Every recovered house carries `loose_key` or
      `jaro_winkler` in `builty_attom_match_tier` (diagnostics file), so exact-only results are
      one filter away.
- [ ] Claude change 09-10: Builty → ATTOM → NFIP loss (issue #26 deck note): where the drop happens and why is written
      up in Dropbox `Flooding/Notes/deck_notes_2026-09-10.md`; add the tab to the deck.
- [ ] Keep unfunded/denied/withdrawn applications with a `funded` flag instead of dropping them,
      so self-financed elevations can be measured (Builty elevation with no grant match). Flag
      local recovery programs (e.g. NYC Build It Back, `funding_type == 5` in `clean_builty.do`)
      before calling an elevation self-financed.
- [x] Builty → ATTOM → NFIP frequency loss (Anna; diagnosed 2026-09-03, fixed 2026-09-06). Of 9,853
      Builty elevations, 7,089 matched an ATTOM address and 5,576 reached an NFIP property.
      (a) NYC address normalization: dropped 09-07 when NY left the sample (NY stays out, 09-10).
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
- [ ] Further sample restrictions (SFHA, FMA eligibility): decided 09-15 that `complete.do` restricts
      nothing; coverage and `attom_matched` are flags and the analysis scripts apply the sample rule.
      Any future restriction goes there too, leaving `analysis.dta` as the flagged universe.
- [x] Deflate ATTOM property values -- done 2026-09-06 (wide file), replaced 09-15 by the long
      `attom_value.py` build described in section 0. Cleaning unchanged: zeros, values below $10,000
      nominal and above $100M set to missing, CPI base 2023. Checked 09-15: no negatives, real medians
      FL $135k (2002) to $284k (2023). Still open: what the 23% (LA) of property-years logged as exactly
      zero mean, and the LA parish-level valuation issue in section 0.

## 4. Descriptives — empirical facts (Claude change 09-15)

- [ ] Claude change 09-15: `descriptives/scratch/empirical_facts_six_questions.do` answers the six
      empirical-facts questions (adoption per 1,000 at-risk homes by prior claims / RL status;
      adoption by within-county x SFHA value quintile; permit cost in dollars and as a share of
      pre-permit value, HMA total cost and federal share kept apart; county FMA dollars vs
      repetitive-loss and loss shares and by county value; permit hazard by years since a major
      claim vs grant obligation timing; premium and claims event studies plus a same-flood
      comparison). Scratch script, run by hand (invocation in its banner); not in `master.do`.
      Writes `output/tables/empirical_facts_six_questions.xlsx` and `output/figures/q1_*` to `q6_*`.
      Event = Builty retrofit permit; denominators = ATTOM-linked homes in strictly covered
      county-years, 2010-2024. Open: prior claims are panel-observed (2009 on) only; the
      same-flood comparison uses a 5% control draw.

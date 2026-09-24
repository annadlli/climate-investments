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
- [x] LA ATTOM valuation (09-15; checked 09-20, `notes/la_valuation_2026-09-20.md`: the market field is
      ten times the assessed field in every large parish, so it is not an assessed-type figure; against
      sale prices it runs 0.85 statewide, 0.69-0.77 in Caddo, Ouachita and Calcasieu, 0.88-0.95 in the
      metro parishes. Keep `market_value_total`; blank the ratio-0.1 rows, mainly St. Helena 22091;
      note the under-valuation in the appendix): the `market_value_total` field is far below market in several
      parishes. 2022 medians: Caddo $39k, Ouachita $32k, Rapides $40k, Calcasieu $61k, each with
      17-29% of properties under $10k nominal, against $123-147k in East Baton Rouge, Jefferson,
      Orleans, St. Tammany. Louisiana assesses residential property at 10% of market, so the field
      may carry an assessed-type figure in those parishes. The $10k floor is not the fix. Check
      `assessed_value_total` x 10 against `market_value_total` by parish and decide which to use
      for LA before any LA value result is shown. Statewide LA real median is $105k vs FL $190k, TX $170k.
- [x] Rerun in progress at end of day 09-15 (done 09-19, see next item): clean_builty → geocode_builty → matching steps 3-4 for
      FL LA TX locally (`--from 3`, 8 GB) → `parquet_dta` → `complete` → `summary_table` →
      `histograms`. Everything upstream of the matching (HMA chain, claims, `merge_nfip_hma`, the
      value file) is rebuilt. Check the link-file dates, the value merge rate in the `complete` log,
      and the `elevated` / `elevation_retrofit` / `elevation_cost` rows of the summary table.
      `histograms.do` now also draws `claims_vs_elevation_cost.png` (cost against claims paid
      through the elevation year, 45-degree line); the two histograms keep a $1k display floor.
- [x] Claude change 09-19: the 09-15 rerun landed. Matching steps 3-4 on the cluster for FL LA TX
      (`FROM_STEP=3`, job 18028231, after `git pull` and copying up the 09-15 `builty_elevations_zipfilled.dta`):
      permits matched FL 2,636 of 3,074 (86%), LA 1,547 of 1,675 (92%), TX 1,090 of 1,152 (95%);
      NFIP properties with a Builty flag FL 1,913, LA 1,369, TX 941 (within 4 of the 09-14 files on
      the same properties; ATTOM ids changed for 50 FL, 7 LA, 0 TX). The permit-type cost screen shows
      in the links: FL valued permits 970 -> 626, median $227k -> $83k; TX 221 -> 195. The 09-14
      link parquets are in `build/nfip_attom_pipeline_v2/tmp/links_0914/`, the cluster logs in
      `tmp/logs/`. Locally `parquet_dta` -> `complete` -> `summary_table` -> `histograms` rerun:
      links merge on every panel row, value on 29.0M of 34.8M rows; `analysis.dta` 16.6M rows
      (unchanged). `summary_table.do` read `attom_value`, which `complete.do` renames to
      `property_value` on 09-15; fixed to `property_value` (that is why the 09-15 21:00 table on
      disk still showed the old names). `claims_vs_elevation_cost.png` drawn for the first time.
      Summary table: `elevation_retrofit` 0.17%, `elevation_cost` N 538, mean $206k (no ceiling now;
      panel medians FL $156k, TX $117k). `elevations_by_state.do`, `es_elevation.do` and
      `empirical_facts_six_questions.do` rerun on the new panel the same evening: per-state rates
      and the event-study coefficients are unchanged from 09-16/17 (FL Builty homes 1,315 -> 1,316,
      events 2,284 -> 2,285; LA and TX identical); the NPV sheet moves only through the permit
      valuation median, $213.9k -> $140.6k (new-construction values screened out), so NPV/cost
      0.16 -> 0.24 and payback 74 -> 49 years in the same-flood version.
- [x] Claude change 09-19 (fixed 09-21, Anna: a flagged home is an elevation from the permit year on and
      none before; the flag is missing only where there was never a Builty feed or no ATTOM match;
      `complete.do` now sets the flag missing on `attom_matched != 1` or no covered county-year for the
      property, instead of row by row; full tail rerun 09-21): `elevation_retrofit`, `elevation_source` and `elevation_year` were not
      constant within a property. `complete.do` sets `builty_elevated` missing in county-years
      without Builty coverage (`builty_merge != 3`), and the harmonized variables are built row by
      row from it, so a permit home is flagged only in its covered policy years: 351 properties
      (LA 332, FL 13, TX 6), all Builty-sourced. Counting a property by its first policy year
      (`elevations_by_state.do`) gives LA 626 Builty homes; counting any flagged year gives 967.
      Decide whether the permit should be carried to every policy year of the property (the permit
      is observed once; coverage belongs in the denominator, not the event) and fix in `complete.do`.
- [x] Claude change 09-20 (closed 09-21: appendix paragraph with sources at the end of the note; the
      continued-coverage rule screens 887 of 2,183 ICC homes on the SFHA sample, 41%, against a national
      demolition share of 30% (Kousky and Lingle 2017); no finer screen possible without an address-level
      NFIP link): ICC screen tested for issue #25 (Vendela 09-15), write-up in
      `notes/icc_2026-09-20.md` (repo root; notes stay local, not in Dropbox). 2,618 ICC properties in FL LA TX; 43% leave the panel in
      the ICC year (the continued-coverage rule drops them as possible demolitions or buyouts); among
      the 57% that stay, the NFIP elevated flag flips after the payment for 0.7%, 48% were flagged
      elevated before the flood and 51% never show as elevated. County-level HMA acquisition
      exposure (LA and TX above 80% either way) cannot sharpen the screen. Recommendation: keep ICC
      as source 2 but do not count it as a verified elevation; replace the coverage rule with a
      property-level buyout check when the GOHSEP and RI lists arrive, or via ATTOM transfers to a
      public grantee. Open: whether `elevated` in the policy file is a current-rating field carried
      across years (then NFIP flag flips are re-ratings, not elevations). Checked 09-20: the
      cleaner forces the flag monotone, so drops are invisible; 2,238 of 5.38M multi-year
      properties ever flip, 711 of them in 2010 (panel start) and almost none after 2021 (Risk
      Rating 2.0; flagged share falls 10% -> 6.5%). Check the raw indicator in the per-state clean
      files before source-1 events are used; consider dropping 2010 flips. Raw files checked
      09-20 (addendum 2 of the ICC note): the raw flag is removed twice as often as added (1 to 0:
      FL 3,176, LA 1,601, TX 893; 0 to 1: 752, 855, 621), 60% of removals in 2022-2025, and the
      two directions hit different homes. Keep the monotone cleaning; drop 2010 flips or require
      a claim before the flip.
- [x] Claude change 09-20: FEMA BCA discount rate confirmed at 7% (OMB reinstated the 1992 A-94 rate
      on 2025-04-08; 3.1% applied Nov 2023-Apr 2025); the 0.93 factor in the q6_npv block is 1/1.07,
      source cited in the script. `es_premium_elevation.do` moved to `descriptives/archive/`;
      `es_prices_mitigation.do` and the six-questions banner say what supersedes them. The Builty
      funnel tab is already in `Deck_Sep 8.xlsx` with the 09-15 loose-rung numbers; the 09-19 run
      changes them by at most 2 per state, so nothing to add.
- [ ] Claude change 09-20: unfunded applications (section 3 item below) are partly available without a
      FOIA: OpenFEMA `HmaSubapplications` v2 (FEMA GO, FY2020+ FMA and BRIC, plus HMGP) carries
      status (approved, not selected, withdrawn) and activity, and `HmaSubapplicationsProjectSiteInventories`
      v1 has the property sites. Pulled 09-20 (12,490 subapplications in FL LA TX, all programs):
      elevation subapplications that were denied, not selected, ineligible or withdrawn number
      about 250 in FL, 130 in LA and 25 in TX, against roughly 470, 450 and 90 approved, obligated
      or closed. The file reaches back to the PDM, RFC and SRL years, so pre-2020 is partly
      covered; property-level sites exist only for FEMA GO (FY2020+). Still needs the state lists
      for older property-level detail.
- [ ] Claude change 09-20 (Anna): ICC buyout screens beyond continued coverage, addendum 3 of
      `notes/icc_2026-09-20.md`. ZIP-level funded acquisitions (OpenFEMA mitigated properties, 5,551
      FL LA TX records) within 3 years of the ICC year: 37% of stayers and 37% of leavers. ATTOM
      record after the ICC year on the assigned property: improvements fall to zero for about 4% in
      both groups, nominal transfers 0-3%. Nothing separates leavers from stayers; the coverage rule
      stays, unvalidated, until a property-level buyout list arrives.
      09-21 (Anna: NPR data is local): `~/Downloads/fema npr.csv`, 5,010 FL LA TX address-level buyouts
      (FY 1992-2017), 62% matched to an ATTOM address (`build/npr_buyouts_attom.parquet`); 1,677 NFIP
      panel properties sit on those addresses but keep their policies a median 12 years past the
      buyout year, so the cell-level ATTOM-NFIP link does not deliver the bought-out house; 2 of
      2,618 ICC properties match. Addendum 4 of the ICC note. The NPR file should move from
      Downloads to `raw/` on Dropbox if it is kept.
- [x] Claude change 09-20 (Anna): elevations by state as shares of the state's properties in
      `analysis.dta` (`sh_builty`, `sh_hma_total`, `sh_hma_closed`, `sh_nfip_any`, `sh_an_events`, FL LA TX
      only) and a closed-projects-only HMA column. Caveat: numerators are statewide counts from the
      Builty, HMA and NFIP files while the base is the SFHA insured sample, so the shares overstate;
      `sh_an_events` is the like-for-like rate. Shares: LA Builty 0.26%, HMA 0.94% (closed 0.44%),
      NFIP 0.28%; TX 0.21 / 0.21 (0.07) / 0.28; FL 0.11 / 0.02 (0.01) / 0.08.
- [x] Claude change 09-20 (Anna): HMA closed (completed) projects only, optional and outside the
      pipeline (Anna, same day: "put that as optional and not incorporate into pipeline"). The
      pipeline files are untouched: `clean_hma.do`, `prep_hma.do`, `merge_nfip_hma.do` and the panel's
      `hma_*` variables keep every funded project. `descriptives/scratch/hma_closed_projects.do`
      (run by hand) takes project status from the raw OpenFEMA projects file, writes
      `clean/hma_county_closed.dta` (county collapse as in `prep_hma.do`, closed projects only) and
      `output/tables/summary_table_hma_closed.xlsx` (closed vs all-funded county counts and spend on
      the analysis sample: mean 89 vs 177 properties, $16.3M vs $34.3M). `empirical_facts_six_questions.do`
      with the third argument `closed` reads that county file (Q4, Q5; outputs `_closed`);
      `elevations_by_state.do` has the closed column; `hma_cost_timeseries.do` draws `_closed`
      curves; all three merge status from the raw file themselves (import with `bindquote(strict)` and
      the project id recast from strL to str100 before it can be a merge key). Closed projects are 2,656 of LA's
      5,658 HMA properties, 379 of TX's 1,090, 302 of FL's 436; Q4 regression signs unchanged (value
      coefficient -0.54 vs -0.52 on dollars per repetitive-loss home). `elevations_by_state.do` also
      now reads `hma_elevation.dta` instead of the old `fma_elevation.dta`.
- [x] Claude change 09-21 (Anna: "do the rest"): three pipeline edits, logged here per CONVENTIONS 9.
      (a) `complete.do`: `nfip_flip == 2010` set missing before the harmonized year is built. Reverted
      09-23 (Vendela): a 2010 flip is an observed 0 -> 1 from 2009, not a panel-boundary effect, and no
      mechanism for the spike was given. Panel check 09-23: flips by year 2010 711, 2011 135, 2016 peak
      324 on a flat ~2.1M policy-years at risk; by entry cohort the first renewal flips at several times
      the later rate for every cohort (2009 cohort 711 in 2010, 2010 cohort 88 in 2011, 2011 cohort 42
      in 2012), so a 2010-only drop is inconsistent with the pattern. Whether NFIP flips are elevation
      events or first-renewal re-ratings stays open (ICC item above). `analysis.dta` on disk carries the
      09-21 blanking until `complete` -> `summary_table` -> `histograms` is rerun. (b) `attom_value.py`:
      for LA, property-years whose market field sits at the assessed level (market / (10 x assessed)
      between 0.09 and 0.11, St. Helena above all; `notes/la_valuation_2026-09-20.md`) are set missing;
      count reported as `la_assessed_level` in the log. Claude change 09-22: (b) reverted before the commit
      (Anna): the blanking was Claude's pick over rescaling, never put to Anna; a sale-price check 09-22 found
      95% of the flagged rows already fall under the $10k floor, so the rule touched 24k of 34.9M LA
      property-years, and in Jefferson, Rapides and St. Charles (5.4k rows) both fields sit at market level and
      the rule blanked good values. `attom_value.py` is back at the committed version; the LA value file and
      `analysis.dta` on disk still carry the 09-21 blanking until the tail is rerun (`attom_value` LA ->
      `attom_value_dta` -> `complete` -> `summary_table` -> `histograms`). (c) the property-level Builty flag (item above).
      Rerun order: `attom_value` (LA) -> `attom_value_dta` -> `complete` -> `summary_table` -> `histograms`
      -> descriptives, done 09-21 01:08-01:41: LA assessed-level rows 477,181 of 40.5M property-years;
      2010 flips 9,783 rows; Builty-flagged rows 25,143 (was 23,677), NFIP flip rows 15,356 (was 25,128).
      State table (SFHA panel, one row per home): events per 1,000 LA 3.01 (1,813; Builty 969, NFIP 844),
      TX 2.25 (1,165; 612, 553), FL 0.93 (2,015; 1,327, 688). Summary `elevation_retrofit` 0.15% (was
      0.17%), `elevation_cost` N 538 mean $206k, `property_value` mean unchanged. `es_elevation`: premium
      t0-t4 +$2 to +$13 (SE $25-47), t+5 -$113; same-flood excess claim +$40.6k before, -$0.7k after.
      NPV sheet: median permit valuation $139k, same-flood NPV/cost 0.23, payback 51 years.
      `build/alternates/hma_subapplications.py` (test script, run by hand) pulls the
      OpenFEMA subapplications for FL LA TX to `raw/hma_subapplications.csv` and tabulates elevation
      subapplications by outcome in `output/tables/hma_subapplications_status.xlsx`: funded / not funded /
      open FL 411 / 289 / 255, LA 358 / 134 / 123, TX 63 / 27 / 61. Not done: removing the Q6 block from
      the six-questions script, because the NPV sheet takes its coefficients from it; moving the NPV onto
      `es_elevation.do` changes a deck number, so that is Anna's call.
- [ ] Claude change 09-21 (Anna: "does the block-group FIPS change matter?"): yes, in the ATTOM match.
      The NFIP field carries mixed Census vintages: on the LA raw file, 35% of 2009 policy records use
      codes that exist only in 2010 geography and 4% only in 2020; by 2025 it is 0% and 45%. The
      property snapshot the matcher uses (first policy year) is 2010-only for FL 25%, LA 29%, TX 35% of
      properties (30-42% of those first insured before 2021, 1-2% from 2023 on). `nfip_attom.py` keys
      the block-group tiers 1-4 and 15 on ATTOM's `censusblockgroupfips` (2020 geography) only, so those
      properties cannot hit a block-group tier and fall to ZIP, community or county tiers. Fix: build the
      key on both vintages, ATTOM `censusblockgroupfips2010` is already on the geocoded panel, and take
      the NFIP code against whichever set it belongs to; expect the tier 1-4 share to rise and some
      Builty-permit homes to move. Rerun of steps 3-4 on the cluster. Within-property splits are small:
      6.5% of multi-year units (zip x construction date x NB date) carry two codes in LA, mostly in
      different tracts and spiking in 2010-11 and 2016-17, so they look like re-geocoding or several
      houses per unit rather than the vintage change; county codes are unaffected. Related: issue #5.
      FEMA's documentation (checked 09-21) names no vintage: the v2 dictionary says only that tracts are
      "updated prior to each decennial census" and that "the NFIP relies on our geocoding service to
      assign" them; the FAQ says the fields are derived from a geocode of the address. Also: the v2
      policies and claims files are deprecated and removed on 2026-10-15; the successor NfipPolicies v3
      (refreshed 2026-09-09) carries the block group as `censusGeoid`. Before the next NFIP pull, check
      whether v3 is uniformly 2020 geography (same test against the ATTOM sets); if so the mix goes away
      at the source, but `property_id` changes for every re-geocoded home, so the panel is rebuilt.
      Claude change 09-21, later the same day (Anna): the two-vintage match is OPTIONAL and not
      adopted. The production scripts are back to their committed versions; the modified copies are
      `build/alternates/{nfip_attom,attom_geocode,geocode_attom}_vintage.py`, run by hand with outputs
      under `build/nfip_attom_pipeline_v2/alternates/vintage/`. The cluster job submitted 09-21 (steps
      1-4, FL LA TX) runs the two-vintage version (Anna: "a final alternate comparison, not pipeline
      commit"); its outputs go under `alternates/vintage/` on Dropbox, the production links stay the
      09-19 parquets, and any panel built from it is `analysis_vintage.dta` via a scratch copy of
      `complete.do`. On the cluster the job overwrites the production geocoded panel and links; the
      panel is a superset (one extra column, harmless to the production matcher), the links are
      restorable from Dropbox. What the alternate does: (a) `geocode_attom` runs a second
      Census pass with vintage `Census2010_Current` over the same cached chunks into `results_2010/`
      and writes `blockgroups_by_address_2010.parquet` (`--vintage-2010 none` skips it); the three
      states already have that file from the July/August runs, so no geocoder rerun is needed.
      (b) `build/attom_geocode.py` merges it onto the panel as `censusblockgroupfips2010`.
      (c) `build/nfip_attom.py`: every block-group tier (1-4, 11, 15) runs four passes, NFIP homes
      first insured before `--vintage-switch-year` (2021) against ATTOM's 2010 code, later homes
      against the 2020 code, then each group against the other vintage; `blockgroup_vintage_used`
      on the link file says which hit. Missing 2010 column = old behaviour. Local LA step-4 test
      done: matched 704,064 -> 794,948 of 1,244,072 (57% -> 64%); block-group tiers 1-4 126k -> 167k,
      tier 11 98k -> 147k, tier 15 276k -> 367k; ZIP tiers 5-6 64k -> 24k and tier 12 93k -> 44k.
      Vintage that hit: 2010 for 581k matched homes, 2020 for 99k, none (ZIP/community/county tiers)
      114k. Builty-flagged homes unchanged at 1,369 (Builty houses rank first in every cell), though
      1,331 of them now sit on a different NFIP id, as the cells changed; 520k homes get a different
      ATTOM house, 236k are newly matched, 145k lose theirs. Cluster rerun of steps 1-4 for FL LA TX
      done 09-21 (Anna ran the alternate on the cluster): write-up in `notes/vintage_comparison_2026-09-21.md`,
      files local only under `data/alternates/vintage/` and `output/alternates/vintage/` (not Dropbox).
      Match rate FL 85.8 -> 86.0%, LA 56.6 -> 63.9%, TX 91.6 -> 91.7%; block-group tiers 1-4 FL 1.54M -> 2.08M,
      LA 126k -> 167k, TX 716k -> 1.22M; ZIP tiers fall by the same amount. Value-vs-building-coverage
      correlation rises in every state (Spearman FL 0.12 -> 0.18, LA 0.28 -> 0.30, TX 0.28 -> 0.33).
      Builty-flagged homes FL 1,913 -> 1,907, LA 1,369, TX 941 -> 914. Alternate panel (complete.do on
      `data/alternates/vintage/root`) and descriptives in the same folders: ATTOM-matched 73.5 -> 74.8%,
      premium event study still flat, pre-trend p-values 0.12/0.09/0.01 -> 0.71/0.26/0.25, permit homes on
      the SFHA sample FL 1,327 -> 1,377, TX 612 -> 626, LA 969 -> 770. The LA fall is an artifact: the
      vintage preference pairs permit houses with older NFIP ids in the block group (median 4 vs 6 policy
      years), which more often ended before the parish had a Builty feed, so the flag is set missing as
      unobservable (ever-covered flagged ids 1,224 -> 1,001).
- [ ] Claude change 09-21: permit-aware cell ranking. In `nfip_attom.py` (and the alternate) the NFIP side
      of a cell is ranked by a hash; when the ATTOM house is a permit house, rank NFIP ids alive in the
      permit year first. Removes the LA artifact above and would raise permit counts in production too.
      09-22 (Anna: "do that in my alternate pipeline"): built into `build/alternates/nfip_attom_vintage.py`
      only. In a cell with a permit house, NFIP homes insured in the permit year rank first, then homes
      built by the permit year, then the hash; new link column `nfip_alive_at_permit`. Needs
      `--policy-years`, a parquet of (state, property_id_state, policy_year_first, policy_year_last)
      built from `nfip_hma_panel.dta`, kept locally at `data/alternates/vintage/nfip_policy_years.parquet`
      (38 MB; upload to the cluster beside the properties file, `data/clean/`, and the matcher finds it
      without a flag). LA test 09-22: tier counts unchanged; permit homes insured in their permit year
      52% (production) / 29% (two-vintage) / 65% (with ranking); flagged ids ever in a covered
      parish-year 1,224 / 1,001 / 1,351; permit homes on the SFHA sample 969 / 770 / 1,063. Production unchanged.
      Cluster step-4 rerun 09-22 and local alternate panel: permit homes on the SFHA sample FL 1,358 / LA
      1,070 / TX 621 (production 1,327 / 969 / 612), retrofits 5,109 vs 4,993, event-study shape the same
      as production under Anna's revised es_elevation.do (premium -$326 vs -$357 at t+5, flood-year claims
      +$34.8k vs +$46.0k). Final section of `notes/vintage_comparison_2026-09-21.md`.
      Recommendation in `notes/vintage_comparison_2026-09-21.md`: adopt the two-vintage match together with
      this ranking, in one cluster run, before results are final.
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

- [ ] Claude change 09-21 (Anna: "help me revise and fill out" the deck): draft at
      `notes/slides_2026-09-21/slides.tex`, figures it needs copied to `notes/slides_2026-09-21/figures/`
      (the source PNGs stay in `output/figures`). Code edits, logged here per CONVENTIONS 9:
      (a) `analysis/es_elevation.do`: axis titles are now "Years from permit" and "Effect on premium
      (2023 $)" (claims and any-claim likewise); a cost-effectiveness block after the same-flood
      regressions (pre-minus-post excess claim per flood year x share of the permitted homes'
      county-years that are flood years, 30-year present value at FEMA's 7% BCA rate, against the median
      declared permit cost of the permitted homes) writes sheet `npv` in `output/tables/es_elevation.xlsx`;
      the `q6_npv` sheet of the six-questions workbook is superseded by it. (b) new scratch
      `descriptives/scratch/summary_table_slides.do` (run by hand): the slide's layout, medians, All /
      Pre-FIRM / Post-FIRM, on the SFHA analysis sample, to `output/tables/summary_table_slides.xlsx`;
      the pipeline `summary_table.do` is untouched (Vendela's). Claude change 09-22 (Anna: the slide
      table is the desired summary table): it is now `descriptives/summary_table.do`, run from
      `master.do`, writing `output/tables/summary_table.xlsx`; the means table is archived as
      `descriptives/archive/summary_table_means.do`. Same-day check: every number reproduces by an
      independent collapse; `post_firm` is not constant within property (58,127 property-years
      switch), the scripts take the last year, so the Pre/Post columns move by a few thousand
      properties under the first-year rule (Anna to pick); the deck's pre-FIRM loss ratio was
      typed 1.27, table says 1.265, slide corrected to 1.26. (c) new scratch
      `descriptives/scratch/nfip_claims_timeseries.do` (run by hand): NFIP claims paid by year of loss,
      all policies nationwide, 2023 $, from the raw claims CSV, to `output/figures/nfip_claims_by_year.{png,gph}`
      for the motivation section ($126.8bn over 1978-2025; 2005 $27bn, 2017 $13bn, 2012 $12bn).
      Slide numbers (SFHA sample, 3,291,652 properties): median premium $644 (pre-FIRM $888, post $574);
      claims/premiums 1.07 (1.27 / 0.82); median cumulative claims among claimants $82.6k; elevated ex
      ante 450,281; harmonized retrofit events 4,993 (3,543 / 1,450); median declared retrofit cost
      $145.9k. Still open for the deck: motivation figures for annual U.S. flood losses (NOAA) and the
      Wing et al. (2022) projection; whether the histogram frame or the cost-vs-claims scatter frame
      carries the "elevation is often worth it" point; the raw national policy file is online-only in
      Dropbox, so the policy count and coverage on the Setting slide are FEMA's published figures.

- [ ] Claude change 09-22 (Anna: "do the by source split"): `analysis/es_elevation.do` section 4 runs
      the same event study with one elevation source treated at a time against all never-elevated
      homes, writing `output/figures/es_elevation_{premium,claim,any_claim}_{permit,flip,icc}.{png,gph}`
      and a three-panel `_by_source` figure per outcome; the pooled sections 1-3 are unchanged. Treated
      homes in cohorts 2012-2022: permit 1,573, flip 490, ICC 741. Result: the pooled premium drop is the
      flip and ICC homes (flip -$140 to -$210 from t+1, ICC -$260 falling to -$720 by t+5, both with
      a downward pre-trend); permit homes show no premium change (+$3 to +$22, t+5 -$105). The pooled
      +$46k claim spike at t0 is the ICC homes alone (+$125k at t0, the year of loss by construction;
      flip and permit homes show nothing at t0). No source shows claims below the normal pre-years after
      the event. Open: whether to re-date ICC events (year of loss + 1) and drop flip homes from the
      claims event study, in `complete.do`; see the deck comments.

- [ ] Claude change 09-22 (Anna: "restore it under permit definition", then "a new script that just
      does the same-flood numbers"): the same-flood comparison and cost effectiveness now live in
      `analysis/same_flood_claims.do` (master.do switch `same_flood_claims`), permit-only, writing
      `output/figures/same_flood_claims.{png,gph}` and `output/tables/same_flood_claims.xlsx` (sheets
      same_flood, npv, raw_means). `es_elevation.do` keeps sections 1-4 (pooled and by-source event
      studies) and no longer writes an xlsx. Results 09-22, identical to the 09-21 run: pre +$40.6k
      (SE 9.1k), post -$0.7k (SE 2.1k), $41.3k avoided per flood year; flood-year share 7.1%; $2.9k a
      year; NPV $36.4k at 7% over 30 years against a $146.0k median permit cost (0.25, payback 50 years).
      The deck's premium slide uses the permit panel of the by-source run; the pooled and by-source
      plots are in the appendix.

- [x] Claude change 09-22 (Anna: "see if it replicates last week's results"): scratch
      `descriptives/scratch/es_claims_spec_bridge.do` (run by hand) reproduces the 09-16 six-questions
      claims event study exactly (spec A: pooled TWFE, county x year FE, SE by home, all cohorts, 5%
      controls; t-5 -$23,390 SE 2,184 as in the q6 sheet) and walks to this week's version: B cluster
      by county (same points, SEs x4-5); C county x zone x year FE (no change, within $150); D this
      week's sample (ATTOM-matched, cohorts 2012-2022, counties with a permit, 10% controls; treated at
      t0 1,209 -> 663) halves the level to about -$11k, matching the interaction-weighted permit panel
      within $2k. So last week vs this week is clustering (bars) and the cohort/sample restriction
      (level), not the zone effects or the estimator. Outputs `output/tables/es_claims_spec_bridge.xlsx`,
      `output/figures/es_claims_spec_bridge.{png,gph}`.

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
      year coverage, not the match order; write-up in `notes/la_year_built_2026-09-13.md`.
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
      up in `notes/deck_notes_2026-09-10.md`; add the tab to the deck.
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
      Claude change 09-16: (c) first pass in `build/alternates/builty_coverage_jurisdiction.py`
      (test script, run by hand, Vendela 09-15). Builty LOCALITY -> NFIP community number
      within state by three rules: municipal name (X, CITY/TOWN/VILLAGE OF), county or parish
      name, else the locality's modal county's community. Permit shares matched by the
      municipal rule: FL 85%, TX 98%, LA 58% (41% of LA permits fall to the parish rule,
      Metairie-type places); unmatched < 0.1%. The NFIP community list comes from the raw
      per-state policy files (state, cid, name, county), no FEMA download. Stage 2 compares
      elevation events per 1,000 insured homes under county vs community coverage
      (`output/tables/builty_coverage_crosswalk_compare.xlsx`, events counted in observed
      policy years). County -> community coverage: FL homes 2.17M -> 2.06M, events per 1,000
      0.64 -> 0.59; LA 602k -> 309k, 2.62 -> 3.06 (Builty-only 0.95 -> 1.58); TX 517k -> 182k,
      1.57 -> 1.50 (Builty-only 0.38 -> 0.58). The stricter grain raises the permit rate by
      half in LA and TX and leaves the all-source rate near 0.1-0.3 percent, because NFIP-flag
      and ICC events are spread across every community. If adopted: keep
      `nfipratedcommunitynumber` in `merge_nfip_hma.do` and move the crosswalk into
      `clean_builty_coverage.py`; the weak rule is 3 (mailing city in unincorporated county).
- [ ] Claude change 09-23 (Vendela: "does prevalence improve if complete.do restricts to covered
      counties or localities from the first reporting year on?"): checked read-only, nothing in the
      pipeline changed; scripts in `descriptives/scratch/builty_coverage_community.py` and
      `descriptives/scratch/nfip_elevation_validation.py`.
      (a) Feeds persist: of FL LA TX counties that ever report, 71-77% have no gap year, 87-92%
      still report in 2024+, and only Taylor TX (to 2019), Sumter FL (to 2022) and Hernando FL (to
      1999) stop early. The first loose year is a trickle (strict floor comes 2-7 years later on
      median), so "from the first year on" should open at the first year with 50+ permits or the
      strict floor, not the first row.
      (b) Coverage barely moves prevalence. Three-state SFHA, Builty events per 1,000 properties
      (% of properties): none 0.55 (0.09); county from first 50+ year 0.64 (0.11); community from
      first substantive year 0.65 (0.11); ZIP 0.73 (0.13); community and ZIP and ATTOM matched 0.99
      (0.17); same and 2013+ 1.18 (0.20). All 1,802 events already sit in covered county-years, so
      rules only shrink the denominator; FL (2.1M of 2.9M covered homes, covered since the 1990s)
      stays at 0.33 per 1,000 under every rule. 46% of covered matched homes are in communities
      with zero events; Jefferson Parish 382, Tampa 187, Houston 186, Bellaire 140, EBR 89.
      (c) Crosswalk: name harmonization does not fix LA (59% by name, same as the 09-16 pass),
      because Builty LOCALITY is the mailing city and Baton Rouge, Metairie, Houma, Marrero are
      unincorporated places whose community is the parish. A name-free ZIP-weighted attribution
      (permit -> community by the NFIP policy shares of its ZIP; no-ZIP permits by their locality's
      ZIP mix) assigns 100% of LA and 96% of FL/TX permits and beats the name route where they
      disagree (Orlando, Fort Myers, Pensacola permits are mostly unincorporated county). If a
      community grain is adopted, build it this way in `clean_builty_coverage.py` and keep
      `nfipratedcommunitynumber` in `merge_nfip_hma.do`; county from first 50+ year is within 3%
      of it and needs nothing new.
      (d) Sample rule decision (analysis script, not complete.do): ATTOM matched, community (or
      county) covered from first substantive year, ZIP coverage as robustness. To raise prevalence
      the lever is the state, not the crosswalk: LA reaches 0.71% of properties (4.5 per 1,000,
      661 events); FL never exceeds 0.12%. VA link file (09-01 screen): 215 SFHA events, 1.8 per
      1,000, 97% matched, 44 counties, ZIP missing 46%; NC 63 events, 0.36 per 1,000. VA is worth
      adding, NC is not. Alternative unit: community-year elevation counts.
      (e) NFIP flag validation, raw elevation certificate fields (`elevationDifference`,
      `lowestFloorElevation`, dropped by `clean_nfip_policies.do`), within property post(1..3)
      minus pre(-3..-1): NFIP flip median 0 ft, 9% rise 3 ft or more, 62% unchanged, and the field
      goes from 72% to 36% missing at the flip, so the flip is a certificate entering the rating,
      mostly for an already-elevated house (LA the partial exception, 29% rise 3+ ft). Year- and
      state-matched premium change: flip -0.22 log, ICC -0.32, Builty -0.05; ATTOM value rises
      after none of them. Builty-permitted homes: NFIP flag flat at 22% before and after, elevation
      fields unchanged in 99%, claim rate falls from 18% in the permit year to 2-3% after, so NFIP
      does not re-rate after a retrofit rather than the permit hitting the wrong house.
      Agreed 09-23: keep `elevationdifference` and `lowestfloorelevation` in `clean_nfip_policies.do`
      (rerun from there) so the check runs in Stata, and define a certificate-validated NFIP or ICC
      elevation (flag flip or ICC payment with a rise of 3 ft or more in the same window) as a
      candidate fourth source; treat the plain flip as a rating event, not an elevation.
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
- [x] Claude change 09-16: event studies (Vendela 09-15). Reference period is a `ref_period` local
      (= -1, was -2) in `empirical_facts_six_questions.do` (Q6) and `es_premium_elevation.do`;
      matrices carry all 11 periods with the reference row at zero. Figure notes removed
      (sample and spec go on the design slide). Both scripts remapped to the 09-15 panel names
      (`elevation_retrofit`, `elevation_source` 3 = Builty, `elevation_year`, `elevation_cost`,
      `property_value`); Q4 and Q5 read `hma_spend` / `hma_n_properties` (all programs, no
      FMA-HMGP split) and `clean/hma_elevation.dta`. Premium result at t = -1: pooled
      post-permit change +$8 a year (SE $20), no drop.
- [ ] Claude change 09-16: cost effectiveness (Vendela 09-15) -- Q6 writes sheet `q6_npv`: annual
      claims saving = mean claim coefficient over t-5..t-2 minus t+1..t+5 (flood year t-1 excluded,
      so independent of the reference period), NPV over 30 years at 0.93, against the median permit
      valuation. Confirm the discount rate against FEMA's BCA rate. 09-16 rerun: the event-study
      saving is -$227 a year (post years equal the normal pre years; floods are too rare inside the
      window), NPV -$2.7k. Same-flood version (second block of the sheet): saving per flood year
      $52,140 (pre minus post excess claim), flood-year share of permitted homes' county-years
      5.5%, expected saving $2,881 a year, NPV $33.9k against a median permit valuation of
      $213.9k: NPV/cost 0.16, payback 74 years. Same direction as Hovekamp and Wagner (2023): ex post
      retrofits do not pay on avoided claims alone. Open: the permit valuation median (FL/TX job
      values) may include rebuilds, and the pre-permit excess claim includes the triggering flood.
- [x] Claude change 09-16: elevations per insured home by state (Vendela 09-15, issue #13) -- five
      "Analysis sample" columns in `elevations_by_state.xlsx` from `analysis.dta` (SFHA homes in
      covered county-years): events per 1,000 homes LA 2.9 (1,734 / 601,794), TX 2.4 (1,233 /
      516,758), FL 1.1 (2,284 / 2,173,100). About 0.1-0.3 percent, an order of magnitude above the
      0.01 percent worry from the 09-03 meeting.
- [x] Claude change 09-17: `analysis/es_elevation.do` (Vendela's slide note: a defensible event study
      in the analysis folder). Switch `es_elevation` in `master.do`, off by default. Writes
      `output/figures/es_elevation_{premium,claims,any_claim,same_flood}.{gph,png}`,
      `output/tables/es_elevation.xlsx`, log in `output/logs/`. Design: interaction-weighted (Sun and
      Abraham) built in `reghdfe`, since `eventstudyinteract` and `coefplot` are not installed;
      cohort-specific event-time coefficients averaged with cohort shares, weights treated as fixed
      in the SE. Treated = Builty permit homes, permit years 2012-2022 (the 2025 cohort, the largest,
      has no post period). Controls = homes with no elevation from any source, ATTOM-matched, in
      counties with a permitted home, 10% seeded draw. Effects: home, county x zone (A/V) x year.
      SE clustered by county (62). Reference t = -1, endpoints binned at 5.
      Results: 1,541 permitted homes (663 insured in the permit year), 206,338 control homes,
      1.05M home-years. Premium flat: t0..t4 between +$5 and +$17, SE $23-47; the t+5 bin is -$118
      (SE $56). Two-way FE pre-trend tests: premium p = 0.19, claims 0.39, any claim 0.10. Claims
      relative to the flood year t-1 are -$8k to -$18k in every other period, pre and post alike.
      Same flood (all permit homes): excess claim +$41.5k before the permit (SE $9.2k), -$0.4k after
      (SE $2.0k); any claim +0.19 before, +0.11 after; 784 and 528 home-years.
      Open: no code markers in this script per CONVENTIONS section 9; it supersedes the Q6 block of
      the six-questions scratch script and `es_premium_elevation.do`, which can be archived once the
      deck points at the new figures. `es_prices_mitigation.do` still calls `eventstudyinteract` and
      old variable names, so it does not run on this machine.
- [x] Claude change 09-18: `descriptives/scratch/hma_cost_timeseries.do` (Anna: deck motivation figure).
      Cumulative total project cost of funded HMGP and FMA (incl. SRL, RFC) grants by obligation year
      in 2024 dollars, from the raw OpenFEMA HMA Projects file with the `clean_hma.do` status screen;
      one figure for all projects, one for home-elevation projects (types 202.1 and 202.2). 2026 is
      dropped as a partial year. Scratch, run by hand, not in `master.do`.
      Claude change 09-20: the 09-18 import lacked `bindquote(strict)` (which `clean_hma.do` uses), so
      quoted fields shifted columns and the 09-18 totals (HMGP $38.0bn / FMA $2.3bn; elevation $0.70bn /
      $0.97bn) were wrong. Corrected, through 2025 in 2024 dollars: all projects HMGP $48.3bn, FMA $2.7bn;
      elevation projects HMGP $2.52bn, FMA $1.25bn (consistent with 13,145 HMGP and 4,269 FMA elevated
      properties at roughly $190k and $290k each). Closed projects only (`_closed` figures): all HMGP
      $28.2bn, FMA $1.76bn; elevation HMGP $1.95bn, FMA $0.71bn. Redraw the deck figure from the new PNGs.
- [x] Claude change 09-22 (Anna): archived the scripts the 09-22 deck no longer uses:
      `analysis/es_prices_mitigation.do` -> `analysis/archive/` (switch and call removed from
      `master.do`); `descriptives/scratch/{empirical_facts_six_questions, empirical_facts_candidates,
      es_premium_change_elevations}.do` -> `descriptives/scratch/archive/`. Their figures
      (`q1_*` to `q6_*`, `fact11_*`, `es_premium_change_*`) stay in `output/figures` until the
      folder is next cleaned. `analysis/es_elevation.do` carries the event studies and the
      same-flood comparison; the NPV sheet `q6_npv` lived in the six-questions workbook, so the
      cost-effectiveness numbers now come from `es_elevation.do`.
- [x] Claude change 09-22 (Anna): `descriptives/scratch/{hma_cost_timeseries, nfip_claims_timeseries}.do`
      -> `descriptives/scratch/archive/` (no motivation figure in the 09-22 deck).
      `nfip_policies_coverage.py` stays: it supplies the Setting slide's policy count and coverage.
- [x] Claude change 09-22 (Anna): `descriptives/scratch/{builty_coverage_table, empirical_facts_figures,
      empirical_facts_from_workbook}.do` -> `descriptives/scratch/archive/` (the Builty Coverage frame and
      the `figure_1`-`figure_5` set are not in the 09-22 deck). `create_builty_elevation_wordcloud.py`
      stays for the appendix word cloud.


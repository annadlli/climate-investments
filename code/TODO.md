# TODO — climate-investments

_Rewritten 2026-09-02 against the live `master.do`. Pre-September history (torch_work cleanup,
build/ consolidation, NFIP/FMA/CPI build notes) is in git: `git show 10dffe3:code/TODO.md`.
Follow `CONVENTIONS.md` as you work._

## Standing rule: everything runs from `master.do`

Any acquisition or construction step must run from `master.do` behind a `0/1` switch. Cluster
execution is a thin wrapper in `slurm/` around the same script, never separate logic.

## 0. In progress — end of day 2026-09-02

- `clean_nfip_policies.do` now carries premium, policy_cost, coverage_building (2023 $), risk_rating_2,
  drops property-year duplicates (highest premium kept), and renames ratedfloodzone → flood_zone,
  postfirm → post_firm. `prep_nfip_policies.do` appends first and saves `clean/nfip_policies_panel.dta`
  before collapsing. Both were rerun this evening; check the timestamps on
  `clean/nfip_policies_{panel,property}.dta` before building on them.
- `compile.do` is mid-edit: it reads the panel now. Claims merge should be `m:1` on the three keys +
  year against `clean/nfip_claims_panel.dta` (renamed from `_property_year`, data file renamed too),
  zero-filled, with a running sum for cumulative claims; multiple-loss stays `m:1` (property-level
  status file, no year).
- `summary_table.do` lists premium_init / premium_last; it reads `analysis_no_diagnostics.dta`, which
  must be rebuilt through `final_data.do` before the table is right.
- Anna's unpushed cleaner/prep must adopt the new names (post_firm, flood_zone, premium); `nfip_attom.py`
  reads post_firm_init / flood_zone_init from the property prep when present.
- Anna: archives and `data/` are untracked (gitignored) and several scripts were renamed/moved — pull
  carefully; copy anything wanted from local archive folders first.

## 1. Settle the canonical property-level path

`master.do` carries two routes to a property-level analysis set:

- `compile.do` → `complete.do` (with `compile2.do` attaching ATTOM/Builty at the block-group ×
  construction-year cell grain via the `temp wagner` links; 57% linked).
- `geocode_attom` → `merge_datasets` → `parquet_dta` → `final_data` (Census-geocoded
  ATTOM, Builty matched on address, NFIP assigned per property; output `build/nfip_attom_property/`).

- [ ] Decide with Anna which route is canonical (the second is the intended one).
- [ ] Archive the other route's scripts to `build/archive/` and drop its switches from `master.do`.
- [ ] Update CLAUDE.md pipeline table and README to describe the surviving route only.

## 2. Leftovers in `build/`

- [x] `parquetdta.py` vs `parquet_to_dta.py` — settled by Anna 2026-09-02: `parquetdta.py` deleted,
      `parquet_to_dta.py` renamed `parquet_dta.py`.
- [ ] `compile2.do` (Vendela's) stays live in `build/` until item 1 is settled; it was archived by
      Anna 2026-09-02 and restored the same day. Her archived copy added a market → assessed value
      coalesce and `output_suffix` / `links_dir` args; those edits are kept.

## 3. Analysis-facing construction

- [ ] NFIP in two grains: the property cross-section (current `prep_nfip_policies.do` →
      `nfip_policies_property.dta`) and a full property × policy-year panel (new
      `prep_nfip_panel.do` → `clean/nfip_policies_panel.dta`). Size is fine: 51.1M policy-years
      across the 20 states, 3.4 GB on disk, ~5 years per property. Design points (2026-09-02):
      build the cross-state `property_id` once in the panel and derive the cross-section from it so
      the keys agree; resolve within-year duplicates explicitly (1–2% of property-years in VT/SC are
      repeated transactions, 3–4% of properties have coverage gaps); keep only time-varying fields in
      the panel and encode strings; deflate premiums there (see the premium item). Waits on Anna's
      unpushed premium cleaner.
- [ ] NFIP premiums (found 2026-09-02 via `summary_table.do`; the premium code is Anna's, not yet
      pushed). In `clean_nfip_policies.do`: premiums go negative (min about -17,600 nominal; refunds
      or cancellations, screen or zero them) and `policy_year_init` runs to 2027 (date parsing).
      Move premium deflation upstream: `summary_table.do` deflates `nfip_premium_init` itself for
      now, and `nfip_premium` (most recent year) cannot be deflated there at all because its policy
      year is not carried. Deflate at policy-year level in `clean_nfip_policies.do` or
      `prep_nfip_policies.do`, so both reach `analysis.dta` in 2023 dollars.
- [ ] Re-do HMA cleaning to keep all elevation programs (HMGP, BRIC, FMA, …) with a program flag;
      restrict to FMA downstream.
- [ ] Keep unfunded/denied/withdrawn applications with a `funded` flag instead of dropping them,
      so self-financed elevations can be measured (Builty elevation with no grant match). Flag
      local recovery programs (e.g. NYC Build It Back, `funding_type == 5` in `clean_builty.do`)
      before calling an elevation self-financed.
- [ ] Sample-restriction do-file after compile: `drop if sfha == 1` and FMA eligibility, leaving
      the compiled set as the unrestricted universe.
- [ ] Deflate ATTOM property values. Neither route does it yet: `compile2.do` labels its cell
      medians nominal, and `final_data.do` carries `attom_value_year` but never
      merges `clean/cpi.dta` (base 2023, `real = nominal / cpi`). Add the merge on
      `attom_value_year` in the surviving route, same pattern as the FMA block in `clean_fma.do`.

# Wagner (2022) cleaning code: what to adapt

_Claude review for Anna, 2026-09-08. Source: Dropbox/Flooding/Empirical/Wagner_repository/code/A_cleaning
(0_importhousingcpi, 2_clean_housing, 4_merge_all_houses, 5_merge_all_claims, 7_finalcleaning) and README.pdf._

## Already mirrored in our pipeline
- Flood-zone classes: high = A, AE, AH, AO, V, VE; low = B, C, D, X, XE; anything else dropped (nfip_attom.py).
- Post-FIRM rule: built in or after max(initial FIRM year, 1975) + 1 (nfip_attom.py build_attom).
- Initial FIRM year from the NFHL community layer, IN_FRM_DAT with IN_NFIP_DT fallback, cleaned to 1968+ (attom_nfhl.py).
- Tiered cell match, tightest first, arbitrary pairing within a cell (ours is finer: block group before ZIP; hash order instead of _n).
- Single-family screen by assessor use code; year built with effective-year fallback.
- Claims cap (we drop > $250k NFIP cap; she drops payout/coverage > 1000).

## Worth adapting, in order of value
1. **Community Status Book fallback for FIRM dates** (0_importhousingcpi.do, 4_merge_all_houses.do l.171-184).
   She fills missing initial-FIRM years from FEMA's community status book (nation.csv, fema.gov/cis) when the
   NFHL community layer lacks them, and cleans 9999/8888. Our postfirm_key is missing wherever
   initial_firm_year is, which knocks a property out of tiers 3, 4, 6, 8, 10. One small download + a merge in
   attom_nfhl.py's attach_firm_dates. Also gives the NFIP community number by name, useful for the locality
   matching item.
2. **Year-built availability diagnostic by county** (2_clean_housing.do l.43-57). She computes the share of
   houses with a year built per FIPS before matching, and picks tiers accordingly. That is exactly the
   Louisiana problem (88% of elevated links in tier 15). A one-line table from the geocoded panel, run as a
   descriptives script, would have flagged it up front.
3. **Premium imputation for non-buyers** (4_merge_all_houses.do l.450-484; 7_finalcleaning.do l.152-169).
   Uninsured houses get a counterfactual premium per $1,000 coverage from the mean of their
   zone x FIRM-status x community x year cell, then a regression on BFE and year built. The inverse-merge
   panel (ATTOM base, NFIP merged in) needs this for every house without a policy; her cell means are the
   natural first version.
4. **Flood severity from claims water depth** (7_finalcleaning.do l.87-123). She builds ZIP-year water-depth
   quintiles from the claims file plus a catastrophe flag, then interacts with decade built. Our realized-loss
   deciles use dollars paid; the claims file's waterDepth field would give a physical measure that does not
   depend on coverage. Check clean_nfip_claims.do keeps waterdepth.
5. **BFE by ZIP x zone from policies** (4_merge_all_houses.do l.418-440). She averages base flood elevation
   over policies and drops high-risk houses with none. We carry static_bfe from NFHL on the ATTOM side but
   do not use it; either source works if elevation-relative-to-BFE ever matters.
6. **Housing CPI instead of all-items CPI** (0_importhousingcpi.do; FRED CPIHOSNS). She deflates coverage
   with the housing component, base 2017. We use all-items, base 2023, for everything. For property values
   the housing index is the defensible choice; for premiums and claims all-items is fine. A second column
   in clean/cpi.dta and one switch in attom_value_wide.py.

## Not worth adapting
- Property x year panel built by appending the housing file once per year (l.194-233): ours comes from the
  NFIP policy years directly.
- High-risk-only sample, year_built < 2000, +/-15-year FIRM window, drop houses built in the FIRM year:
  these are her RD/DID sample choices, analysis-side, and hinge on her design (pre/post-FIRM discontinuity),
  not ours.
- Zillow ZTRAX land-use codes RR101 etc.: ATTOM's use codes 376-386 are the equivalent and already in place.
- Claims match keys with org_nb_dt / srl_ind: our claims merge on construction date + block group + nb date
  + year is already 1:1.

## Two small things her code does that ours should keep doing
- Drop houses observed before they were built (`drop if year < year_built`): implicit in ours since the panel
  starts at the first policy year, but the inverse-merge panel will need it explicitly.
- Treat 9999 / 8888 / pre-1968 FIRM dates as missing: ours does 1968-2027; add 9999/8888 explicitly.

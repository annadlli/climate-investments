# Inverse merge: ATTOM/Builty as the base, NFIP merged in

_Claude draft for Anna, 2026-09-07. One page for Vendela before any code is written._

## Why

Today's route (`merge_nfip_fma.do` → `complete.do`) starts from NFIP policies. A house only
exists in `analysis.dta` if it ever held a policy, so three denominators are missing: houses in
the same block group that never insured, houses that elevated without ever insuring, and houses
that neither insured nor elevated. Additionality questions (did the grant or the premium change
who elevates?) need those denominators. The NFIP-first route also applies the elevation screen
before the merge, so an elevated house that failed to reach NFIP is dropped rather than counted.

## What changes

| | Current route | Inverse route |
|---|---|---|
| Unit of the analysis file | NFIP property × policy year | ATTOM property × year |
| Base file | `clean/nfip_policies_panel.dta` | `build/nfip_attom_pipeline_v2/geocoded/{st}_attom_geocoded.parquet` (property × tax year) |
| Builty joins | via the assigned ATTOM record | directly, `attom_builty.py` output, 1:1 on attomid |
| NFIP joins | is the base | inverted link: `assigned_attomid → property_id` from `nfip_attom_property/{st}.parquet`, then the policy panel on `property_id` × year |
| Elevation filter | applied to Builty before the match | applied last, as a flag; nothing dropped |
| Sample restriction | county-years with a Builty feed | same, plus the strict floor (`builty_covered_strict`) as an option |
| Property value | one ATTOM value per property, nominal | `attom_value_wide` reshaped long: value in 2023 $ every tax year |

Nothing in `merge_nfip_fma.do` or `complete.do` changes. The inverse route is a second build script
(`build/complete_attom_base.do`, working name) writing `analysis/analysis_attom_base.dta`, so the two
files can be compared and the NFIP-first file stays the reference until the inverse one is trusted.

## Steps

1. Base: single-family ATTOM properties (use codes 376, 380, 382, 383, 385, 386) in the four states,
   one row per property with block group, county, flood zone (from `attom_nfhl`), construction year.
2. Builty: `builty_elevated`, `builty_elevation_year`, `builty_retrofit`, `builty_new_construction`
   from the `attom_builty` output. Left join; unmatched houses keep 0.
3. NFIP link: invert `nfip_attom_property` to one row per assigned attomid (it is one-to-one by
   construction, so no duplicates). Carry `property_id`, `match_tier_number`, `builty_geo_backfilled`.
4. NFIP panel: policy years, premium, coverage, claims, RL/SRL from `build/nfip_hma_panel.dta` on
   `property_id`. Houses with no link get no policy rows; an `insured` flag per year comes from the
   join. County FMA spending merges on county as today.
5. Value: `attom_value_wide` reshaped long on attomid × year.
6. Coverage: keep county-years with a Builty feed; carry `builty_per_100` and `builty_covered_strict`.
7. Save. Elevation is a flag, not a filter.

## Decisions for Vendela

- Panel years: ATTOM tax years (2003-2024) or NFIP policy years (2009-2025)? The overlap is 2009-2024.
- Unassigned ATTOM properties: keep as uninsured (my proposal) or drop? Keeping them is the point.
- Tier restriction: none by default; tiers 1-4 or 15 as the "tight" option (tier 15 is block group
  × zone without a construction year and holds 88% of LA's elevated links).
- File size: FL alone is 9.4M single-family properties × ~20 years. Keep the base at property level
  and merge the panel pieces on demand, or write one long file per state.

## What it does not fix

The NFIP-to-ATTOM pairing is still arbitrary within a cell, so property-level insurance variables on
an ATTOM house are still "a house like this one". Block-group-level aggregates are unaffected.

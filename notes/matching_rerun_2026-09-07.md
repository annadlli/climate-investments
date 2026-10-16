# NFIP–ATTOM matching rerun, 2026-09-07 (FL LA NJ TX)

_Claude note for Anna. Steps 3-4 rerun on Torch from the 09-06 code (Builty geocode backfill,
retrofit flags); steps 1-2 (ATTOM geocode, NFHL join) reused from 2026-08-17._

## Inputs
- `clean/builty_elevations_zipfilled.dta`: 5,945 permits (FL 3,074, LA 1,675, TX 1,152, NJ 44),
  4,922 with a Census block group. New construction kept (807 properties).
- `clean/nfip_policies_property.dta`: first-policy-year snapshot, rebuilt 2026-09-03.

## Step 3, Builty → ATTOM (from the logs)
| State | Permits | Matched | Elevated ATTOM props | No ATTOM coords | Backfilled | With zone after backfill |
|---|---|---|---|---|---|---|
| FL | 3,074 | 2,310 (75%) | 2,230 | 129 | 38 | 38 |
| LA | 1,675 | 1,431 (85%) | 1,424 | 93 | 60 | 50 |
| TX | 1,152 | 1,041 (90%) | 1,030 | 31 | 16 | 15 |
| NJ | 44 | 40 (91%) | 40 | 1 | 0 | 0 |

SLURM job 17114915 (array 3, 5, 12, 17), logs in ~/Downloads. FL's 75% address-match rate is the
lowest of the four; the 633 new-construction permits are new to the file and new builds are the
addresses ATTOM is likeliest not to have yet, so the retrofit-only rate is probably higher.

## Step 4, NFIP ← ATTOM
| State | NFIP props | Assigned | % tiers 1-4 | % tiers 11-15 | Elevated links | in 1-4 | in 15 | Backfilled links | of which in 1-4 |
|---|---|---|---|---|---|---|---|---|---|
| FL | 3,704,977 | 3,180,423 | 48.4 | 24.0 | 1,684 | 1,481 | 0 | 21 | 16 |
| TX | 2,161,458 | 1,980,362 | 36.2 | 26.1 | 904 | 765 | 0 | 10 | 5 |
| LA | 1,244,072 | 704,064 | 17.9 | 70.9 | 1,272 | 64 | 1,119 | 52 | 3 |
| NJ | 348,081 | 347,856 | 47.6 | 34.5 | 36 | 32 | 0 | 0 | 0 |

Retrofit / new-construction split of the elevated links: FL 1,274 / 418; LA 1,218 / 55; TX 869 / 36; NJ 36 / 0.

## Reading
- The backfill worked where it applied: 21 of the 31 FL+TX houses that ATTOM could not place, and that
  Builty could, now sit in block-group × flood-zone cells (tiers 1-4).
- Louisiana is a construction-year problem, not a geocode problem. NFIP has a construction year for
  every LA property; ATTOM lacks year built for most of the state, so 71% of all LA assignments and
  88% of its elevated links land in tier 15 (block group × zone, no year). That is still a tight
  cell. Any "tight match" restriction should be tiers 1-4 **or** 15, or Louisiana disappears.
- LA's assignment rate is low (57%) for the same reason; the other three are 92-100%.

## Files
- `build/nfip_attom_pipeline_v2/nfip_attom_property/{st}_nfip_attom_property.parquet` (cluster → Dropbox, 09-07 14:15)
- `build/nfip_attom_property/{st}_nfip_attom_property.dta` (converted locally 09-07)
- `build/attom_value_wide/{st}_attom_value_wide.dta` (NFIP-linked subset, 09-07)
- `analysis/analysis.dta` (complete.do, 09-07) with match_tier_number, builty_retrofit,
  builty_new_construction, builty_geo_backfilled, builty_per_100, builty_covered_strict

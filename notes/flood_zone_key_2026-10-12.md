# Flood-zone keys in the NFIP-ATTOM match

Claude, 2026-10-12, for Anna. Documents the aligned flood-zone key that `build/nfip_attom.py` builds on
both sides of the Wagner-cell match (`zone()` for the NFIP rating code, `ZONE_SQL` for the NFHL map code,
`risk()` for the high/low bucket). Data appendix material; verify the program definitions against the
cited sources before they go in the paper.

## Sources

- NFIP side: `ratedFloodZone` on the OpenFEMA NFIP Redacted Policies file (`flood_zone` after
  `clean_nfip_policies.do`). The code is the zone the policy was rated in, which can be an older map's
  zone (grandfathering) and carries rating suffixes that are not map zones.
- ATTOM side: `FLD_ZONE` (with `ZONE_SUBTY`, `SFHA_TF`) of the NFHL layer `S_FLD_HAZ_AR`, state
  geodatabases downloaded Aug 2018 (`raw/nfhl/nfhl/{fips}/`), joined to the geocoded ATTOM point by
  `build/attom_nfhl.py`.
- References: [1] FEMA, "Flood Zones" glossary, fema.gov/glossary/flood-zones. [2] FEMA, FIRM
  Database Technical Reference (S_FLD_HAZ_AR: FLD_ZONE, ZONE_SUBTY, SFHA_TF), fema.gov/flood-maps/tools-
  resources/risk-map/flood-insurance-rate-map-database. [3] 44 CFR 59.1 (definitions of Zones A99 and AR
  and of the SFHA) and 44 CFR 64.3 (zone list). [4] NFIP Flood Insurance Manual, Rating section
  (AO/AH buildings "with certification of compliance", the source of the AHB/AOB codes) and the
  OpenFEMA NFIP Redacted Policies data dictionary (`ratedFloodZone` value list). [5] Wagner (2022),
  high-risk = A/AE/AH/AO/V/VE, low-risk = B/C/D/X (Dropbox `Flooding/Empirical/Wagner_repository/`).

## Key table

| Raw code | Where it occurs | Meaning | Ref. | Zone key | Risk bucket |
|---|---|---|---|---|---|
| AE | NFIP, NFHL | 1% annual-chance (100-year) zone with a base flood elevation (BFE) on the map. SFHA. | 1, 2 | AE | high |
| A1 to A30, A01 to A30 | NFIP (old maps) | Numbered AE zones on FIRMs before the 1986 format; the number indexed the BFE table. Same hazard as AE. | 1, 3 | AE | high |
| A | NFIP, NFHL | 1% zone studied by approximate methods, no BFE on the map. SFHA. Newer studies turn A into AE. | 1, 2 | AE | high |
| AH | NFIP, NFHL | 1% shallow flooding (ponding), 1 to 3 ft, BFE shown. SFHA. | 1, 2 | AH | high |
| AHB | NFIP only | AH zone, building rated "with certification of compliance": lowest floor certified at or above the BFE. A rating code, not a map zone. | 4 | AH | high |
| AO | NFIP, NFHL | 1% shallow sheet flow on sloping ground, 1 to 3 ft, flood depth (not BFE) shown. SFHA. | 1, 2 | AO | high |
| AOB | NFIP only | AO zone, building rated with certification of compliance (lowest floor at or above the depth number). Rating code. | 4 | AO | high |
| A99 | NFIP, NFHL (rare) | 1% zone to be protected by a federal levee under construction; SFHA by regulation, but rated and mapped without BFE. | 1, 3 | A99 | high |
| AR | NFIP, NFHL (rare) | 1% zone behind a levee being restored to accreditation; temporarily SFHA. | 1, 3 | AR | high |
| VE | NFIP, NFHL | 1% coastal zone with wave action (velocity), BFE shown. SFHA; open-foundation (pile) elevation required. | 1, 2 | VE | high |
| V1 to V30, V01 to V30 | NFIP (old maps) | Numbered VE zones on pre-1986 FIRMs. | 1, 3 | VE | high |
| V | NFIP, NFHL | Coastal velocity zone studied by approximate methods, no BFE. | 1, 2 | VE | high |
| X | NFIP, NFHL | Outside the SFHA. NFHL splits it by `ZONE_SUBTY`: "0.2 pct annual chance flood hazard" (moderate, old shaded X), "area of minimal flood hazard" (old unshaded X), "area with reduced flood risk due to levee" (accredited levee). | 1, 2 | X | low |
| B | NFIP (old maps) | Old-map code for the 0.2% (500-year) zone, today's shaded X. | 1, 3 | X | low |
| C | NFIP (old maps) | Old-map code for minimal hazard, today's unshaded X. | 1, 3 | X | low |
| D | NFIP, NFHL | Possible but undetermined hazard, no study. NFIP rates it outside the SFHA. | 1, 2 | D | low |
| XE | none seen | Kept on the low-risk list from the original code; not a FEMA zone. | | XE | low |
| AREA NOT INCLUDED | NFHL only | Polygon outside the mapped area (unincorporated or unmodernized map). 11% of assigned LA parcels. | 2 | AREANOTINCLUDED | missing |
| missing | both | NFIP: no rated zone. NFHL: ATTOM point not inside any hazard polygon (half of LA ATTOM points). | | missing | missing |

`SFHA_TF` on the NFHL side equals "zone key in {AE, AH, AO, A99, AR, VE}" after the alignment, and
`sfha` on the NFIP side (`clean_nfip_policies.do`: first letter A or V) agrees with it code for code,
AHB and AOB included.

## What changed on 2026-10-12

| | Before | After |
|---|---|---|
| AHB, AOB | key "AHB"/"AOB", risk missing: never in a flood tier (LA 10k homes, TX 10k; 15% of the three-state panel) | AH / AO, high |
| A99, AR | risk missing | own key, high (matches through the risk tiers; NFHL has almost none) |
| A, V (unnumbered) | own key; NFIP "A" vs NFHL "AE" never met at an exact-zone tier | AE / VE |
| B, C | own key; NFHL has only "X" | X |
| A1-A30, V1-V30 | AE / VE (unchanged) | AE / VE, pattern now 1 to 30 so A99 is not caught |

The match tiers are unchanged: exact zone key at tiers 1, 3, 5, 7, 9; risk bucket at 2, 4, 6, 8, 10;
no flood information at 11 to 14; block group x zone without construction year at 15. The change needs
matching step 4 rerun on the cluster for FL LA TX, then `parquet_dta` -> `complete` -> descriptives.

## Effect of the alignment (local rerun of matching step 4, 2026-10-12)

| | LA 10-02 | LA 10-12 | TX 10-02 | TX 10-12 | FL 10-02 | FL 10-12 |
|---|---|---|---|---|---|---|
| Exact-zone tiers (1,3,5,7,9) | 181,932 | 196,930 | 1,386,661 | 1,432,986 | 2,162,254 | 2,356,508 |
| Risk-bucket tiers (2,4,6,8,10) | 22,777 | 7,924 | 74,309 | 32,511 | 257,362 | 75,562 |
| No-flood tiers (11-14) | 223,324 | 223,164 | 512,055 | 507,581 | 753,011 | 740,594 |
| Tier 15 (bg x zone, no year) | 366,928 | 386,844 | 8,519 | 10,801 | 12,483 | 13,197 |
| Unmatched | 449,111 | 429,210 | 179,914 | 177,579 | 519,867 | 519,116 |
| Unmatched among AHB/AOB/A99/B/C/A | 127,628 | 91,233 | 39,469 | 34,934 | 258,493 | 199,774 |
| Permit homes | 4,820 | 4,820 | 1,716 | 1,709 | 4,838 | 4,836 |

Homes matched in both runs that keep the same ATTOM id: LA 62%, TX 83%, FL 62% (same tier 96%, 96%,
89%). The cells were redefined (A with AE, B and C with X), so the within-cell draw changes; the permit
flag, which rides on the permit-ranked candidate, is stable.

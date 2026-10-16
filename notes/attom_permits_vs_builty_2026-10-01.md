# ATTOM building permits (full Dewey product) vs Builty: elevation counts, FL LA TX

Claude change 10-01: new file. Run on Anna's laptop 2026-10-01 from the combined parquet
in `~/Downloads/attom_permits_all.parquet` (Dewey endpoint `prj_hgemyv4u__cdst_jepwthr36ssvhibt`,
1,418 monthly files, 39 columns, 8 GB zstd). Code: `anna_private/attom_permits_combine_screen.ipynb`
(stage A) and the unmodified `code/clean/clean_builty.do` (stage B) on an alternate data root.

## What was run

1. Candidate net `CANDIDATE_PATTERNS` from `prepare/extract_builty.py` over type, subtype and
   description, in DuckDB, FL LA TX only. Survivors written in the Builty raw layout
   ("Type - SubType" on line 1, description on line 2), exactly as `build/alternates/attom_permits_sample.py`.
2. `clean_builty.do` Sections 1 and 2 with `~/dewey/attom_permits/root` as data root and `cpi.dta` copied in.
3. `clean/geocode_builty.py` on the Section 2 output (ZIP fill + Census block groups), same as the pipeline does for Builty.
4. Comparison against the pipeline's Builty files in Dropbox `clean/` (`builty_elevations.dta`,
   `builty_states/`, `builty_raw/`, `builty_coverage_county.dta`).

The whole-table `.dta` was not made: FL alone has 71.1M permits, TX 37.3M, LA 2.0M.

Differences from the Duval sample run: this product has no classifier table (so `PROPERTY_TYPE` is
empty and commercial permits are not dropped up front) and no status table (no submitted/finaled
dates; permit year is the issue year). Both push the ATTOM counts up.

## Funnel (Builty | ATTOM | ratio)

| Stage | FL | LA | TX |
|---|---|---|---|
| All permits 1990-2026 | 29.9M / 69.8M / 2.3 | 0.94M / 1.77M / 1.9 | 16.1M / 35.5M / 2.2 |
| Elevation candidates (net) | 230,564 / 827,650 / 3.6 | 114,718 / 126,297 / 1.1 | 140,480 / 321,063 / 2.3 |
| Survive `clean_builty.do`, permits | 3,405 / 17,871 / 5.2 | 1,793 / 7,557 / 4.2 | 1,887 / 5,702 / 3.0 |
| Addresses (collapse) | 3,074 / 10,005 / 3.3 | 1,675 / 6,362 / 3.8 | 1,152 / 4,580 / 4.0 |
| Retrofit addresses | 2,449 / 8,897 / 3.6 | 1,568 / 5,808 / 3.7 | 1,087 / 4,390 / 4.0 |
| New-construction addresses | 633 / 1,195 / 1.9 | 108 / 580 / 5.4 | 66 / 196 / 3.0 |
| Retrofit addresses 2010+ | 2,043 / 5,647 / 2.8 | 1,568 / 4,440 / 2.8 | 922 / 3,900 / 4.2 |
| ATTOM addresses with an ATTOMID | 8,326 (83%) | 5,788 (91%) | 4,205 (92%) |
| Addresses with a ZIP before fill | 2,213 / 9,858 | 1,660 / 6,319 | 657 / 4,553 |

Full tables, by year and by county, in `~/dewey/attom_permits/attom_vs_builty_elevations.xlsx`.

## Caveats on the ATTOM side

The screen was tuned on Builty text. Two jurisdiction boilerplates it never saw survive here:

- **Harris County TX**: "elevation boundary survey iecc required" is standard permit text. 2,031 of
  the 2,919 Harris addresses carry it; the county's ATTOM count is 2,919 vs Builty 690.
- **Citrus County FL**: HVAC changeouts and service reconnections stamped
  "flood zone AE post firm structure elevation certificate not required". Citrus is 2,161 ATTOM
  addresses vs 147 Builty, mostly this.

Stripping those patterns (and "no elevation", "elevation certificate required") and keeping only
addresses with raise/lift-the-house wording gives a conservative floor:

| | FL | LA | TX |
|---|---|---|---|
| Boilerplate addresses | 1,986 | 223 | 2,180 |
| House-raising wording, not boilerplate | 4,604 | 5,429 | 2,144 |
| of which retrofit | 4,195 | 5,004 | 1,997 |
| Builty retrofit addresses | 2,449 | 1,568 | 1,087 |

So even the floor is 1.7x (FL), 3.2x (LA) and 1.8x (TX) Builty's retrofit addresses. The LA gain is
the cleanest: Orleans 2,877, Calcasieu 759, Jefferson 726 ATTOM addresses, and Builty's LA county
field is empty so no county split is possible there.

Funding source is almost never in the text (FL 13, LA 154, TX 22 addresses tagged FEMA/HMGP/FMA).

## ZIP fill and block groups (`geocode_builty.py`)

20,947 addresses sent to the Census batch geocoder. ZIP: 20,730 already present, 47 filled by
Census, 30 from address text, 1 from a same-address match, 139 unresolved (FL 88, LA 33, TX 18).
Block group attached for 93% (FL 9,165 of 10,005; LA 6,046 of 6,362; TX 4,332 of 4,580).

Quirk: one address ending in a backslash (`4922 3 B ST E BRADENTON\`, Manasota FL) makes the Census
endpoint return HTTP 500 for the whole 10,000-row chunk, on every retry. Found by bisecting the chunk
(scratch script, not in the repo); that row gets a No_Match line. Worth a one-line strip of trailing
backslashes in `clean_street_for_census` before the 20-state run.

## Files

- `~/dewey/attom_permits/root/clean/builty_states/builty_elevations_{fl,la,tx}.dta`: screened permits
- `~/dewey/attom_permits/root/clean/builty_elevations.dta`: one row per address, Section 2 output
- `~/dewey/attom_permits/attom_elevations_fl_la_tx.dta`: final, ZIP-filled, with block groups
- `~/dewey/attom_permits/root/clean/builty_raw/{fl,la,tx}.csv`: candidates in Builty layout
- `~/dewey/attom_permits/attom_elevation_candidates.parquet`: same, one file
- `~/dewey/attom_permits/attom_vs_builty_elevations.xlsx`, `conservative_counts.csv`, `attom_permits_by_state.csv`
- Stata log: `~/dewey/attom_permits/stata_run/run_screen.log`

## Next

- Add the two boilerplate kills to `clean_builty.do` (or an ATTOM-specific pre-screen) before
  using these counts; re-read a sample of Harris and Citrus survivors.
- Decide whether the ATTOM permit file replaces or supplements Builty in `complete.do`. 83-92% of
  addresses already carry an ATTOMID, so the match to the ATTOM property panel is direct.
- Other 17 states: same notebook with `STATES` widened; candidates for FL LA TX took 1 minute, Stata 7 minutes.


## Overall table (added 10-02)

Also in `~/dewey/attom_permits/attom_builty_overall_table.xlsx`. States as rows; permits 1990-2026.

| State | ATTOM permits, 1990-2026 | Builty permits, 1990-2026 | ATTOM elevation candidates | Builty elevation candidates | ATTOM elevation permits (screened) | Builty elevation permits (screened) | ATTOM elevation addresses | Builty elevation addresses | ATTOM retrofit addresses | Builty retrofit addresses |
|---|---|---|---|---|---|---|---|---|---|---|
| FL | 69,762,631 | 29,862,612 | 827,650 | 230,564 | 17,871 | 3,405 | 10,005 | 3,074 | 8,897 | 2,449 |
| LA | 1,765,748 | 943,315 | 126,297 | 114,718 | 7,557 | 1,793 | 6,362 | 1,675 | 5,808 | 1,568 |
| TX | 35,451,078 | 16,101,633 | 321,063 | 140,480 | 5,702 | 1,887 | 4,580 | 1,152 | 4,390 | 1,087 |
| Total | 106,979,457 | 46,907,560 | 1,275,010 | 485,762 | 31,130 | 7,085 | 20,947 | 5,901 | 19,095 | 5,104 |

## Coverage and permit-field granularity (added 10-02)

Scratch script `coverage2.py` (not in repo); outputs `~/dewey/attom_permits/coverage_by_year.csv` and
`coverage_by_county_2015_2024.csv`. Denominator: Census 2020 county population estimates (co-est2024-alldata).
"Floor" = at least 0.4 permits per 100 residents per year, roughly 1 per 100 homes, the Builty strict floor.

Population share in counties with any permit / with permits above the floor, 2020:
FL ATTOM 1.00 / 0.98, Builty 0.96 / 0.94. LA ATTOM 0.93 / 0.65, Builty 0.66 / 0.56. TX ATTOM 0.96 / 0.87, Builty 0.76 / 0.74.
ATTOM is at FL 0.95+ from 1995 and TX 0.84+ from 2010; LA only passes 0.5 (floor) in 2015. Builty's TX floor share is
under 0.5 before 2018, LA under 0.5 before 2015.

2015-2024, population in counties with zero ATTOM permits: FL 0, LA 2.8%, TX 1.7% (largest: Starr, Maverick, Val Verde TX;
Natchitoches, Evangeline LA). Thin ATTOM (below floor): FL 1.5%, LA 36%, TX 10.5%. Zero Builty: FL 2.7%, LA 25%, TX 23%.
Louisiana gaps worth knowing: Ouachita (64 ATTOM permits in ten years), Rapides, St. Landry, Iberia; Livingston and
Tangipahoa are thin in ATTOM (~0.4/100) but dense in Builty (5-6/100). Nacogdoches TX and Hardee FL likewise Builty-only in practice.

Localities 2015-2024 (city-name match, noisy): FL 795 in both, 8,228 Builty-only (2% of Builty permits), 2,755 ATTOM-only.
Biggest Builty-only names are spelling (ST. PETERSBURG, CITY OF TALLAHASSEE, HILLSBOROUGHCOUNTY, SUGARLAND).

Descriptions present for 83% (FL, LA) and 79% (TX) of 2010-2024 permits. Counties with under half: Sarasota 12117 (24%),
St. Johns 12111 (29%), Seminole 12109 (34%), Bell TX 48027 (7%), Comal 48091, Johnson 48251, El Paso 48141, and in LA
Tangipahoa 22105 (9%), Rapides 22079 (1%), Bossier 22015 (1%), Lafourche 22057 (8%). Those counties can only yield
elevations through the type field.

Type/subtype fields: FL has 109,521 distinct TYPE values, TX 53,011, LA 10,467 (jurisdiction-specific free text, no
taxonomy). SUBTYPE empty for 59% FL, 53% TX, 80% LA. Values that explicitly name a house elevation or raise: 663 values,
3,026 permits, 45 counties, nearly all in Harris TX ("Home elevation permit", 547, 2005-2026), Pinellas FL ("Residential
elevated structure", 306, 2019-), Galveston TX ("Raise house"), and the LA parishes Terrebonne, St. Mary, St. Bernard,
St. Charles, Ascension, St. John the Baptist. Only 2% (FL), 12% (LA), 10% (TX) of screened ATTOM elevation addresses
come from such a type; the rest come from description text. Flood-related type values are common (flood-zone
determination, floodplain development, Harvey recovery) but are not elevations.

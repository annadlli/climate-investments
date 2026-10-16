# ATTOM permits replace Builty in the pipeline (2026-10-02)

Claude change 10-02: new file. Anna's to-do list of 10-02, item by item. Code in the repo,
data under the Dropbox root; numbers from the local run of the same day (16 GB laptop).

## 1. Pipeline: Builty -> ATTOM permits

The permit stage is generic now and reads ATTOM building permits. Old scripts are in each
stage's `archive/` (gitignored, in git history), new ones mirror them one for one:

| Stage | Was | Now | Output |
|---|---|---|---|
| extract | `prepare/extract_builty.py` | `prepare/extract_attom_permits.py` | `clean/permits_raw/{st}.csv`, candidate net over type + subtype + description |
| screen | `clean/clean_builty.do` | `clean/clean_permits.do` | `clean/permit_states/permit_elevations_{st}.dta`, `clean/permit_elevations.dta` (one row per address, ATTOMID carried) |
| geocode | `clean/geocode_builty.py` | `clean/geocode_permits.py` | `clean/permit_elevations_zipfilled.dta` (ZIP fill, block group, coordinates; `*_permit` columns) |
| coverage | `clean/clean_builty_coverage.py` | `clean/clean_permit_coverage.py` | `clean/permit_coverage_{county,zip,locality}.dta` (`permit_covered`, `permit_covered_strict`) |
| link | `build/attom_builty.py` | `build/attom_permits.py` | `build/nfip_attom_pipeline_v2/attom_permits/{st}_attom_permits.parquet` (property master) and `{st}_permits_matched.parquet` (permit level) |
| assign | `build/nfip_attom.py` | same script, `builty_*` columns are `permit_*`, `--attom-nfhl-permits` | unchanged |
| panel | `build/complete.do` | permit-only elevation | `analysis/analysis.dta` |

Raw input: `raw/attom_permits/attom_permits_all.parquet` (8 GB, FL 71.1M, LA 2.0M, TX 37.3M
permits; moved from `~/Downloads`). `master.do` switches: `extract_attom_permits`,
`clean_permits`, `geocode_permits`, `clean_permit_coverage`; the matching driver
(`slurm/run_property_matching.sh`) step 3 calls `attom_permits.py`.

What changed in logic, beyond names:

- **Link on ATTOMID first.** 80% (FL), 92% (LA), 93% (TX) of screened retrofit addresses carry
  ATTOM's own property ID; `attom_permits.py` joins those directly (tier `attomid`) and sends the
  rest down the old address ladder (six exact tiers, loose key, Jaro-Winkler).
- **Two stamp kills in the screen** (`clean_permits.do`): the strong pattern "<noun> elevation"
  no longer fires when the next word is certificate / cert / verification / boundary / survey /
  requirement / not / or, and a new kill (gated on `strong == 0`) removes "elevation certificate
  ... not required", "no (flood) elevation certificate", "boundary survey", "iecc", "no elevation"
  and "base flood elevation ... not established". Harris County TX survivors fall from 2,919 to
  879 addresses, Citrus County FL from 2,161 to 713. The same regex applied to Builty would
  also change Builty counts slightly; not rerun.
- **ATTOMID through the collapse.** `clean_permits.do` keeps `attomid` (first non-missing per
  address). The 10-01 alternate-root run had dropped it.
- **Permit year** is the issue date only (ATTOM has no submitted / finaled dates in this product).
- **Trailing backslash** stripped from street text before the Census geocoder (one such
  address made the endpoint return HTTP 500 for a whole 10,000-row chunk on 10-01).
- **County codes outside their state** (81 county-years, 376 permits) are set missing in the
  coverage file so county x year is unique, which `complete.do`'s `merge m:1` requires.

`complete.do` (permit-only elevation; "no harmonized elevation anymore: keep but not use"):

- `elevation_retrofit`, `elevation_year`, `elevation_cost` (ATTOM job value, 2023 $),
  `elevation_funded` come from the permit alone; an address whose only permits are elevated new
  construction is not an event (`permit_retrofit != 1` -> 0).
- `elevated_attom` = 1 from the permit year on, 0 before and for never-permitted observable
  homes, missing where a permit is unobservable (no ATTOM match, or no permit feed in any of the
  home's county-years).
- `elevated` is the raw NFIP status flag again (no longer overwritten from the permit year);
  `nfip_flip` and `nfip_icc` (years, from `merge_nfip_hma.do`) stay on `analysis.dta`, unused.
  The 09-21 "2010 flip set missing" line is gone (Vendela reverted it in the TODO on 09-23).
- `elevation_source` is gone. `es_elevation.do` lost its by-source section 4 (the 09-22
  by-source figures stay on disk under `output/figures/es_elevation_*_{flip,icc,permit,by_source}`);
  `same_flood_claims.do` treats every permitted home as treated and no longer sets flip / ICC
  homes aside. One-line option if wanted: drop homes with `nfip_flip` or `nfip_icc` from the
  controls in both scripts.

## 2. Screen results (FL LA TX)

| | FL | LA | TX |
|---|---|---|---|
| Candidate permits (net) | 827,650 | 126,297 | 321,063 |
| with an ATTOMID | 670,027 | 101,390 | 227,491 |
| Screened permits | 15,354 | 7,364 | 3,379 |
| Addresses | 8,543 | 6,179 | 2,421 |
| Retrofit addresses | 7,416 | 5,647 | 2,227 |
| Elevated new construction | 1,213 | 557 | 197 |
| Retrofit addresses with an ATTOMID | 80% | 92% | 93% |
| Block group from the Census geocoder | 7,731 | 5,886 | 2,263 |

Against the 10-01 run without the stamp kills: FL 10,005 -> 8,543 addresses, LA 6,362 ->
6,179, TX 4,580 -> 2,421 (Harris). Builty retrofit addresses for comparison: FL 2,449, LA
1,568, TX 1,087.

## 3. Do Builty and ATTOM find the same homes? (`build/alternates/attom_vs_builty_homes.py`)

Retrofit addresses, matched on the ATTOMID the Builty address linked to in the 09-19 step-3
run, else on cleaned street + ZIP. `output/tables/attom_vs_builty_homes.xlsx`.

| | FL | LA | TX | All |
|---|---|---|---|---|
| Builty retrofit addresses | 2,449 | 1,568 | 1,087 | 5,104 |
| ATTOM retrofit addresses | 7,416 | 5,647 | 2,227 | 15,290 |
| In both (ATTOMID or address) | 1,324 | 749 | 829 | 2,902 |
| Share of Builty homes found in ATTOM | 56% | 48% | 77% | 58% |
| same, county-years where ATTOM has a feed | 56% | 48% | 77% | 58% |
| Share of ATTOM homes found in Builty | 18% | 13% | 37% | 19% |
| same, county-years where Builty has a feed | 20% | 35% | 45% | 27% |
| Permit year identical (ATTOMID pairs) | 68% | 94% | 76% | |
| Permit year within one year | 91% | 99% | 93% | |

So ATTOM is a superset in volume but not in membership: it misses 23-52% of Builty's homes
even inside county-years where it reports permits. The year agreement of the overlap is tight,
so the two feeds see the same permit when they see the same house. By county (sheet
`by_county`): Jefferson Parish LA has 814 Builty homes and 702 ATTOM homes but only 416 in
both, because ATTOM's Jefferson feed is empty before 2013 and in 2017-2018 (next section);
Livingston LA (171 vs 14), Fort Bend TX (76 vs 27) and Lee FL (334 vs 591, 174 in both) are the
other large one-sided counties.

## 4. Coverage over time and within the county (`descriptives/scratch/permit_coverage_by_year.do`)

Share of insured SFHA homes whose county-year has at least 1 permit per 100 single-family
homes (the strict floor), and whose own ZIP shows a permit that year
(`output/tables/permit_coverage_by_year.xlsx`, `output/figures/permit_coverage_by_year.png`):

| | FL | LA | TX |
|---|---|---|---|
| ATTOM strict, 2009 / 2013 / 2017 / 2021 / 2025 | .99 / .99 / .98 / .99 / .99 | .22 / .75 / .52 / .80 / .83 | .88 / .90 / .92 / .91 / .87 |
| ATTOM own ZIP, same years | .99 / .99 / .99 / .99 / .99 | .36 / .88 / .74 / .90 / .90 | .89 / .89 / .90 / .91 / .89 |
| Builty strict, same years | .90 / .95 / .96 / .97 / .94 | 0 / .49 / .72 / .71 / .61 | .23 / .74 / .74 / .75 / .67 |
| ZIP share within ATTOM-covered county-years, 2009-2025 mean | .99 | .83 | .93 |

Does it get better? FL is complete throughout. TX is flat at about 0.9 from 2009 (Builty only
reaches 0.75 in 2017). LA rises from 0.22 (2009) to 0.8+ from 2013, with a hole in 2017-2018:
Jefferson Parish reports 24-74 permits a year in 2009-2012, 3,600-7,100 in 2013-2016, 65 and
21 in 2017-2018, then 2,800-6,500; East Baton Rouge falls to 2,097 and 452 in 2018-2019;
Orleans drops from 35,000 a year to 5,000-7,000 from 2022. These are feed gaps, not real
construction cycles, and the strict county-year flag handles them (a permit in a gap year
is unobservable, not absent).

Within a covered county-year, the home's own ZIP shows a permit for 99% (FL), 83% (LA), 93%
(TX) of insured homes, so a county feed is close to, but not, the whole county. The
`permit_coverage_locality.dta` file (site-address city x year) is the finer grain if a
city-level rule is wanted.

## 5. How ATTOM describes its permit sourcing (for the "do we have all the info" question)

Nothing in ATTOM's documentation promises completeness within a county:

- ATTOM Cloud help, "Building Permits": "Building Permits are sourced from local government
  authorities such as counties." and "Around 10% of building permits do not match to a known
  ATTOM property (ATTOM ID)." (https://cloud-help.attomdata.com/article/494-building-permits)
- Datarade product listing (ATTOM, "Construction Data"): "358 million records" across "3,140
  counties", "20 years of historical data", weekly delivery, sourced from "local city and county
  building departments"; "Because permit formats and descriptions vary widely by jurisdiction,
  ATTOM normalizes permit data".
- 2020 launch press release: "over 200 million residential and commercial building permits
  nationwide", covering "94 percent of the nation's top cities".
- The coverage reports shipped with the sample (`raw/ATTOM sample permits/reports/Currency/`)
  have sheets for TaxAssessor, Recorder, AVM, Geocode, Foreclosure and Assignment, no permit
  sheet; the Catalog of Elements has no permit codes. The 10-01 note found no classifier or
  status side tables in the Dewey product either.

So the unit of coverage is the jurisdiction (a city or county building department), the
county-year counts are the only coverage measure we have, and the year-to-year holes above show
that a county "being in ATTOM" is a per-year fact. The ZIP and city indexes from
`clean_permit_coverage.py` are the more granular check.

## 6. Pipeline run (local, 10-02)

Matching steps 3-4 ran on the laptop (`run_property_matching.sh --from 3`, DuckDB cap 8 GB,
4 threads): LA 20 minutes, TX 6, FL 15. Two things had to change for that to work:

- `attom_permits.py` pulls the ATTOM rows for the loose house-number + ZIP block as one row per
  property (latest assessment) in a second, aggregating scan; the full property x year rows of
  that block (1.8M properties in TX, 4.4M in FL) swapped the machine, and a window function over
  them ran DuckDB out of memory. Exact-key and ATTOMID hits still come back with every year.
- The disk had 16 GB free, and Dropbox evicts freshly written files under disk pressure
  (`ls -lO` shows `dataless`); the first `complete.do` attempt died reading `attom_value.dta`
  with "not Stata format" while that was happening. Reruns read it fine.

Link files (`nfip_attom_property/{st}_nfip_attom_property.parquet`), NFIP properties at first
policy year:

| | FL | LA | TX |
|---|---|---|---|
| Permits (addresses) | 8,543 | 6,179 | 2,421 |
| linked to an ATTOM property | 7,275 (85%) | 5,775 (93%) | 2,297 (95%) |
| of which on ATTOMID | 6,987 | 5,616 | 2,237 |
| ATTOM properties with an elevation permit | 7,207 | 5,762 | 2,280 |
| in an SFHA per NFHL (retrofits) | see `permit_homes_sfha.csv` | 2,442 of 5,293 | |
| NFIP properties matched to ATTOM | 86.0% | 63.9% | 91.7% |
| NFIP properties carrying a permit | 4,838 | 4,820 | 1,716 |
| of which retrofit | 4,020 | 4,426 | 1,604 |
| of which insured in the permit year | 2,065 | 2,853 | 990 |
| Builty-flagged NFIP properties, 09-22 run | 1,907 | 1,369 | 914 |

The match rates and tier counts are the 09-22 production values (the permit source does not
enter the cell keys; only the permit-aware ranking inside a cell changes which NFIP id gets
the permit house).

`complete.do` ran 16:37-17:47 (the 160M-row value merge is the hour); then `summary_table`,
`histograms`, `elevations_by_state`, `es_elevation`, `same_flood_claims`.

### Analysis sample (SFHA homes, FL LA TX, 3,291,652 properties)

| | FL | LA | TX | All |
|---|---|---|---|---|
| Insured SFHA homes | 2,173,100 | 601,794 | 516,758 | 3,291,652 |
| of which permit observable (ATTOM match and a permit feed) | 1,677,844 | 327,150 | 438,408 | 2,443,402 |
| Permit events (`elevation_retrofit`) | 2,360 | 2,036 | 1,048 | 5,444 |
| per 1,000 insured homes | 1.09 | 3.38 | 2.03 | |
| per 1,000 observable homes | 1.41 | 6.22 | 2.39 | |
| Builty events, 09-22 panel | 1,327 | 969 | 612 | 2,908 |
| `elevated_attom` = 1 in the last policy year | 1,681 | 1,467 | 822 | 3,970 |
| `elevated_attom` = 0 | 1,676,163 | 325,683 | 437,586 | |
| `elevated_attom` missing (unobservable) | 495,256 (23%) | 274,644 (46%) | 78,350 (15%) | 848,250 |

`output/tables/elevations_by_state.xlsx`, `elevations_by_state_shares.xlsx`,
`elevated_attom_by_state.xlsx` ("tab elevated_attom state"), figure `elevations_by_state.png`
(FL LA TX: ATTOM permits, of which in an SFHA, HMGP, FMA, NFIP flips). Statewide the ATTOM
retrofit addresses are 0.94% (LA), 0.43% (TX), 0.34% (FL) of the analysis-sample homes; the
permit homes NFHL places in an SFHA are 2,442 / 1,020 / 3,020 (LA / TX / FL), 0.41 / 0.20 / 0.14%.
Homes with an event but `elevated_attom` = 0 in their last year (5,444 - 3,970 = 1,474) left the
NFIP before the permit year: the cell ranking pairs a permit house with an NFIP id insured in the
permit year when one exists, but not every cell has one.

### Summary table (`summary_table.xlsx`, medians; All / Pre-FIRM / Post-FIRM)

Premium $644 / $888 / $574; claims over premiums 1.07 / 1.27 / 0.82; elevated ex ante 450,245;
`elevation_retrofit` 5,444 / 3,505 / 1,939 (was 4,993 / 3,543 / 1,450 with the harmonized
definition on 09-22); median declared cost $132,387 / $138,233 / $111,870 (was $145,900; LA job
values enter now, Builty had none for LA).

### Event study (`es_elevation.do`, permit cohorts 2012-2022)

2,740 permitted homes (was 1,541), 224,594 control homes, 80 counties. Premium: t0..t5 between
-$31 and +$6 (SE $9-27), flat as before, but the pre-period now trends: t-5 -$146 (SE 56), t-4 -$68,
t-3 -$36, t-2 -$15. Claims: -$1.6k to -$7.7k in every period against t-1, as before. The
by-source panels are gone (no source split without the NFIP events).

### Same flood (`same_flood_claims.do`)

5,444 permitted homes, 224,594 controls; flood county-years: excess claim +$33.9k before the
permit (SE 5.0k), +$1.1k after (SE 3.0k), difference $32.8k (SE 5.2k); any-claim +0.18 before,
+0.04 after; 1,625 and 835 home-years. Cost effectiveness: flood-year share 6.1%, $1,987 a year,
NPV $24.7k at 7% over 30 years against the $132.4k median permit cost: NPV / cost 0.19, payback
67 years (was 0.25 and 50 years with the Builty cost median of $146k).

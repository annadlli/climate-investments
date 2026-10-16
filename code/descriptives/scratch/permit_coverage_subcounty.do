/******************************************************************************
Authors: Anna Li
Date: 2026-10-09

Claude change 10-09: new scratch script, run by hand (not in master.do).

Description: Does an ATTOM permit feed for a county cover the whole county? Two
    views by state, all states, 2010-2024. (1) Cities: ATTOM's site-address city
    is the reporting unit; the share of a county's ever-reporting cities that
    report in a given year (permit-weighted over covered county-years), the share
    of county-years with 90%+ of them reporting, and the permit-weighted share of
    cities whose feed starts after 2012 or stops before 2024. (2) Counties: the
    share of ATTOM single-family homes in county-years with >= 1 permit per 100
    homes, and the median permits per 100 homes. Answers Anna's "if ATTOM has the
    county, do we have all the info?" and supports the NJ and TX-tier decisions.

Inputs:  clean/permit_coverage_locality.dta, clean/permit_coverage_county.dta
Outputs: ../output/tables/permit_coverage_subcounty.xlsx (sheets cities, counties)
Run:     do permit_coverage_subcounty.do "<data root>" "<output folder>"
******************************************************************************/

args data output

* -----------------------------------------------------------------------------
* Cities within covered county-years
* -----------------------------------------------------------------------------

use "`data'/clean/permit_coverage_locality.dta", clear
keep if inrange(year, 2010, 2024)
egen tag_ce = tag(state countycode locality)
bysort state countycode: egen n_cities_ever = total(tag_ce)
egen tag_cy = tag(state countycode year locality)
bysort state countycode year: egen n_cities_yr = total(tag_cy)
bysort state countycode year: egen permits_cy = total(permit_n_permits)
bysort state countycode locality: egen city_permits = total(permit_n_permits)

* City feeds that start late or stop early, permit-weighted
preserve
    keep if tag_ce
    gen partial = year_first > 2012 | year_last < 2024
    gen one = 1
    collapse (sum) cities = one (mean) share_partial_feed = partial [aw = city_permits], by(state)
    tempfile cities
    save `cities'
restore

* Share of the county's cities reporting, by county-year, permit-weighted
bysort state countycode year: keep if _n == 1
gen share_cities = n_cities_yr / n_cities_ever
gen all_cities = share_cities >= 0.9
gen one = 1
collapse (sum) county_years = one (mean) share_cities_reporting = share_cities share_cy_90pct_cities = all_cities ///
    (p25) p25_share_cities = share_cities [aw = permits_cy], by(state)
merge 1:1 state using `cities', nogen
order state cities county_years share_cities_reporting p25_share_cities share_cy_90pct_cities share_partial_feed
label var state                  "State"
label var cities                 "Cities (ATTOM site-address city) reporting 2010-2024"
label var county_years           "Covered county-years 2010-2024"
label var share_cities_reporting "Share of the county's ever-reporting cities that report, permit-weighted"
label var p25_share_cities       "Same, 25th percentile"
label var share_cy_90pct_cities  "Share of county-years with 90%+ of cities reporting, permit-weighted"
label var share_partial_feed     "Share of permits from cities whose feed starts after 2012 or stops before 2024"
gsort -county_years
export excel using "`output'/tables/permit_coverage_subcounty.xlsx", sheet("cities") firstrow(varlabels) replace
format share_* p25_* %5.2f
list, clean noobs

* -----------------------------------------------------------------------------
* Counties: strict coverage of single-family homes
* -----------------------------------------------------------------------------

use "`data'/clean/permit_coverage_county.dta", clear
keep if inrange(year, 2010, 2024)
gen one = 1
collapse (sum) county_years = one (mean) share_cy_strict = permit_covered_strict, by(state)
tempfile cy
save `cy'
use "`data'/clean/permit_coverage_county.dta", clear
keep if inrange(year, 2010, 2024) & !mi(attom_n_sf)
collapse (mean) share_homes_strict = permit_covered_strict (p50) permits_per_100 = permit_per_100 [aw = attom_n_sf], by(state)
merge 1:1 state using `cy', nogen
order state county_years share_cy_strict share_homes_strict permits_per_100
label var state              "State"
label var county_years       "County-years with any permit, 2010-2024"
label var share_cy_strict    "Share of county-years with >= 1 permit per 100 SF homes"
label var share_homes_strict "Share of ATTOM SF homes in strictly covered county-years"
label var permits_per_100    "Median permits per 100 SF homes (home-weighted)"
gsort -share_homes_strict
export excel using "`output'/tables/permit_coverage_subcounty.xlsx", sheet("counties", replace) firstrow(varlabels)
format share_* %5.2f
format permits_per_100 %6.1f
list, clean noobs

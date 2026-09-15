/******************************************************************************
Author: Anna Li
Date: 2026-09-07
Edited: 2026-09-10
Description: Builty permit coverage by state, one row per state, for the deck.
    Counties with a feed, counties clearing the 1-per-100-homes floor, the
    median feed rate, and the share of single-family homes in a covered county.
Revision: states exclude NJ now
******************************************************************************/

args data output

* Set table options
local abbrevs FL LA TX // 09-10: NJ dropped for lack of Builty coverage
local names `" "Florida" "Louisiana" "Texas" "'
local stock_year 2020 // year to measure the covered share of homes, as most coverage then

* -----------------------------------------------------------------------------

* Counties per state, from the crosswalk 
use "`data'/clean/crosswalks/county_xwalk.dta", clear
drop if county == "Statewide"
gen state_abbrev = ""
local i = 1
foreach ab of local abbrevs {
    local nm : word `i' of `names'
    replace state_abbrev = "`ab'" if state == "`nm'"
    local i = `i' + 1
}
keep if state_abbrev != ""
collapse (count) counties_total = countycode, by(state_abbrev)
rename state_abbrev state
tempfile counties
save `counties'

* Import coverage file 
use "`data'/clean/builty_coverage_county.dta", clear
gen byte in_sample = 0
foreach ab of local abbrevs {
    replace in_sample = 1 if state == "`ab'"
}
keep if in_sample

* Single-family homes in covered counties in the stock year, loose and strict
preserve
    keep if year == `stock_year'
    gen sf_covered = attom_n_sf if builty_covered == 1
    gen sf_strict  = attom_n_sf if builty_covered_strict == 1
    collapse (sum) sf_covered sf_strict, by(state)
    tempfile stock_covered
    save `stock_covered'
restore

* Single-family homes statewide (attom_n_sf repeats across years, so one value per county)
preserve
    collapse (max) attom_n_sf, by(state countycode)
    collapse (sum) sf_total = attom_n_sf, by(state)
    tempfile stock
    save `stock'
restore

* Counties with a feed and their median rate; a county counts if it clears the bar in any year (1 permit per 100 single-family homes in county in year)
gen covered_any = builty_covered == 1
gen covered_strict = builty_covered_strict == 1
collapse (max) covered_any covered_strict (min) year_first = year (max) year_last = year ///
    (median) per_100_median = builty_per_100, by(state countycode)
collapse (sum) counties_any_feed = covered_any counties_strict = covered_strict ///
    (median) per_100_median (min) year_first (max) year_last, by(state)

* Put the pieces together
merge 1:1 state using `counties', nogen
merge 1:1 state using `stock', nogen
merge 1:1 state using `stock_covered', nogen
gen pct_counties_any    = 100 * counties_any_feed / counties_total
gen pct_counties_strict = 100 * counties_strict / counties_total
gen pct_sf_covered      = 100 * sf_covered / sf_total
gen pct_sf_strict       = 100 * sf_strict / sf_total

* Label variables
label var state                "State"
label var counties_total       "Counties in state (crosswalk)"
label var counties_any_feed    "Counties with any Builty permit, any year"
label var pct_counties_any     "% of counties with any feed"
label var counties_strict      "Counties clearing 1 permit per 100 homes, any year"
label var pct_counties_strict  "% of counties clearing the floor"
label var per_100_median       "Median county feed rate, permits per 100 homes"
label var year_first           "First year with permits"
label var year_last            "Last year with permits"
label var pct_sf_covered       "% of single-family homes in a county with any feed, `stock_year'"
label var pct_sf_strict        "% of single-family homes in a county clearing the floor, `stock_year'"

* Output table
keep state counties_total counties_any_feed pct_counties_any counties_strict pct_counties_strict ///
    per_100_median year_first year_last pct_sf_covered pct_sf_strict
order state counties_total counties_any_feed pct_counties_any counties_strict pct_counties_strict ///
    per_100_median year_first year_last pct_sf_covered pct_sf_strict
format pct_* per_100_median %9.1f
list, noobs abbreviate(12)
export excel using "`output'/tables/builty_coverage_by_state.xlsx", firstrow(varlabels) replace

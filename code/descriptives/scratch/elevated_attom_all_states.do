/******************************************************************************
Authors: Anna Li
Date: 2026-10-09

Claude change 10-09: new scratch script, run by hand (not in master.do).

Description: Elevated homes by state for every state, not restricted to the
    SFHA or to insured homes: ATTOM retrofit permit addresses, and per 1,000
    ATTOM single-family homes where the property panel exists (20 states).

Inputs:  clean/permit_elevations.dta, clean/permit_coverage_county.dta
Outputs: ../output/tables/elevated_attom_all_states.xlsx
Run:     do elevated_attom_all_states.do "<data root>" "<output folder>"
******************************************************************************/

args data output

* ATTOM single-family homes by state (the county count is the same every year; take 2020)
use state year attom_n_sf using "`data'/clean/permit_coverage_county.dta", clear
keep if year == 2020
collapse (sum) attom_sf_homes = attom_n_sf, by(state)
tempfile stock
save `stock'

* Permit addresses by state
use state retrofit new_construction using "`data'/clean/permit_elevations.dta", clear
collapse (sum) retrofit new_construction, by(state)
merge 1:1 state using `stock', nogen
replace attom_sf_homes = . if attom_sf_homes == 0 // states without an ATTOM property panel
gen retrofit_per_1000 = 1000 * retrofit / attom_sf_homes

label var state             "State"
label var retrofit          "ATTOM elevation permits: retrofit addresses"
label var new_construction  "ATTOM elevation permits: elevated new construction"
label var attom_sf_homes    "ATTOM single-family homes (20 states with a property panel)"
label var retrofit_per_1000 "Retrofit addresses per 1,000 ATTOM single-family homes"
gsort -retrofit
export excel using "`output'/tables/elevated_attom_all_states.xlsx", firstrow(varlabels) replace
format retrofit_per_1000 %5.2f
list, clean noobs

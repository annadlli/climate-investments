/******************************************************************************
Authors: Vendela Norman, Anna Li
Date: 2026-09-03

Description: Prepares the final analysis dataset: restricts the NFIP-FMA panel to 
    county-years with Builty permit coverage and merges in a minimal set of 
    ATTOM/Builty property-link variables from the matching run.

******************************************************************************/

args data states

* -----------------------------------------------------------------------------
* Section 1: Prepare ATTOM/Builty property links
* -----------------------------------------------------------------------------

* Append the state link files
clear
foreach st of local states {
    local stl = strlower("`st'")
    append using "`data'/build/nfip_attom_property/`stl'_nfip_attom_property.dta", ///
        keep(state property_id_state assigned_attomid attom_value_year ///
             attom_market_value_total builty_elevated builty_elevation_year)
}
isid state property_id_state
tempfile links
save `links'

* -----------------------------------------------------------------------------
* Section 2: Merge datasets
* -----------------------------------------------------------------------------

* Import NFIP-FMA panel
use "`data'/build/nfip_hma_panel.dta", clear

* Merge Builty permit coverage 
ren policy_year year
merge m:1 countycode year using "`data'/clean/builty_coverage_county.dta", keep(1 3) ///
    keepusing(year) gen(builty_merge)
ren year policy_year

* Merge ATTOM/Builty property links
merge m:1 state property_id_state using `links', keep(1 3) nogen
gen attom_matched = assigned_attomid != ""

* -----------------------------------------------------------------------------
* Section 3: Apply sample restrictions
* -----------------------------------------------------------------------------

* Restrict to county-years w/ builty coverage 
// Note: This is a generous restriction that needs to be refined. Some localities 
// within county don't report. 
keep if builty_merge == 3

* Drop extraneous variables
drop builty_merge countycode assigned_attomid property_id_state

* Drop years w/ missing data
drop if policy_year > 2025

* -----------------------------------------------------------------------------
* Section 4: Save data
* -----------------------------------------------------------------------------

* Label 
label var attom_matched            "NFIP property has an assigned ATTOM property"
label var attom_market_value_total "ATTOM total market value (nominal, attom_value_year $)"
label var attom_value_year         "ATTOM tax year of the market value"
label var builty_elevated          "Builty elevation permit on assigned ATTOM property"
label var builty_elevation_year    "Earliest Builty elevation-permit year"

* Save 
order builty_elevated, after(elevated)
order attom_matched attom_market_value_total attom_value_year builty_elevation_year, ///
    after(cumulative_claims)
sort state property_id policy_year
compress
sa "`data'/analysis/analysis.dta", replace

* Save extract that can be used for Claude 
keep if inlist(state, "TX", "FL", "LA")
bysort property_id (policy_year): gen _draw = runiform() if _n == 1
bysort property_id (policy_year): replace _draw = _draw[1]
keep if _draw < 0.1
drop _draw
sa "`data'/analysis/extracts/500M_subsample.dta", replace

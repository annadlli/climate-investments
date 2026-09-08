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
             attom_market_value_total builty_elevated builty_elevation_year ///
             match_tier_number builty_retrofit builty_new_construction builty_geo_backfilled)
             // Anna 09-06:  add in new variables: match tier + retrofit/new-construction flags, builty_built_at_permit here after the next matching rerun
}
isid state property_id_state
tempfile links
save `links'

* -----------------------------------------------------------------------------
* Section 2: Merge datasets
* -----------------------------------------------------------------------------

* Import NFIP-FMA panel
use "`data'/build/nfip_hma_panel.dta", clear

* Restrict to the states in master.do
gen byte in_sample = 0
foreach st of local states {
    replace in_sample = 1 if state == "`st'"
}
keep if in_sample
drop in_sample

* Merge Builty permit coverage 
ren policy_year year
merge m:1 countycode year using "`data'/clean/builty_coverage_county.dta", keep(1 3) ///
    keepusing(year builty_per_100 builty_covered_strict) gen(builty_merge)
    // 09-07: added builty_per_100 and the strict flag, to remain for descriptives
ren year policy_year

* Merge ATTOM/Builty property links
merge m:1 state property_id_state using `links', keep(1 3) nogen
gen attom_matched = assigned_attomid != ""

* Merge the ATTOM value for the policy year (2023 $)
// 09-07: now have attom wide file. attom_value_dta keep the NFIP-linked subset. Pick the column matching policy_year rather than reshaping.
preserve
    clear
    foreach st of local states {
        local stl = strlower("`st'")
        append using "`data'/build/attom_value_wide/`stl'_attom_value_wide.dta", keep(attomid value_*)
    }
    isid attomid
    rename attomid assigned_attomid
    tempfile values
    save `values'
restore
merge m:1 assigned_attomid using `values', keep(1 3) nogen
gen attom_value_2023 = .
forvalues y = 2000/2026 {
    replace attom_value_2023 = value_`y' if policy_year == `y'
}
drop value_*

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
label var attom_value_2023         "ATTOM market value in the policy year (2023 $; zeros, >$100M missing)"
label var attom_value_year         "ATTOM tax year of the market value"
label var builty_elevated          "Builty elevation permit on assigned ATTOM property"
label var builty_elevation_year    "Earliest Builty elevation-permit year"
label var match_tier_number        "NFIP-ATTOM match tier (1 = block group x zone x year ... 15)"
label var builty_retrofit          "Builty permit elevates an existing structure"
label var builty_new_construction  "Builty permit is elevated new construction"
label var builty_geo_backfilled    "ATTOM coordinates/block group backfilled from Builty geocode"
label var builty_per_100           "Builty permits per 100 ATTOM single-family properties, county-year"
label var builty_covered_strict    "County-year has >= 1 Builty permit per 100 SF properties"

* Save 
order builty_elevated, after(elevated)
order attom_matched attom_value_2023 attom_market_value_total attom_value_year builty_elevation_year ///
    builty_retrofit builty_new_construction match_tier_number builty_geo_backfilled, ///
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

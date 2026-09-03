/******************************************************************************
Authors: Vendela Norman
Date: 2026-09-02

Description: Appends the state-level FEMA NFIP policies data to a full property-year
    panel, then collapses it to a property level cross-section.

******************************************************************************/

args data states

* -----------------------------------------------------------------------------
* Section 1: Append state files to a property-year panel
* -----------------------------------------------------------------------------

* Append all state files 
clear
foreach st of local states {
    di in red "Appending NFIP policies data for state: `st'"
    local stl = strlower("`st'")
    append using "`data'/clean/nfip_policies_state/`stl'.dta"
}

* Make property_id unique across states
rename property_id property_id_state
egen property_id = group(state property_id_state)
drop property_id_state
order property_id

* Save
sa "`data'/clean/nfip_policies_panel.dta", replace

* -----------------------------------------------------------------------------
* Section 2: Collapse to property level 
* -----------------------------------------------------------------------------

* Create additional analysis variables 
// i) Got elevated 
// Note: These are properties for which we observe a change in elevation status over time
bysort property_id (policy_year): gen got_elevated = elevated[_N] == 1 & elevated[1] == 0
// ii) Elevation year 
bysort property_id (policy_year): egen elevation_year = min(cond(elevated == 1, policy_year, .))
replace elevation_year = . if got_elevated == 0
// iii) First and last policy year
bysort property_id (policy_year): egen policy_year_init = min(policy_year)
bysort property_id (policy_year): egen policy_year_last = max(policy_year)
// iv) Premium and policy cost in the first and last policy year
// Note: Averaged over transactions within the year
foreach v in premium policy_cost {
    bysort property_id: egen `v'_init = mean(cond(policy_year == policy_year_init, `v', .))
    bysort property_id: egen `v'_last = mean(cond(policy_year == policy_year_last, `v', .))
}

* Collapse to property level 
// Note: NFIP community number and zipcode can change for the same structure over time 
// due to administrative reasons. 
// i) Set time-varying attributes to their most-recent value within each property
foreach v of varlist *elevated flood_zone sfha post_firm primary_residence zipcode countycode ///
    coverage_building risk_rating_2 {
    bysort property_id (policy_year): replace `v' = `v'[_N]
}
// ii) Drop time-varying variables 
drop policy_year nfipratedcommunitynumber premium policy_cost
duplicates drop
isid property_id

* Label 
label var got_elevated              "Property got elevated"
label var elevation_year            "Year property was observed elevated"
label var policy_year_init          "Year of first NFIP policy"
label var policy_year_last          "Year of last NFIP policy"
label var premium_init              "Premium in first policy year (2023 $)"
label var premium_last              "Premium in last policy year (2023 $)"
label var policy_cost_init          "Policy cost in first policy year (2023 $)"
label var policy_cost_last          "Policy cost in last policy year (2023 $)"

* Save
sa "`data'/clean/nfip_policies_property.dta", replace

/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-10

Description: Collapses FEMA NFIP policies data from year level to property level

******************************************************************************/

args data states

* -----------------------------------------------------------------------------
* Section 1: Collapse state-specific NFIP files 
* -----------------------------------------------------------------------------

* Loop over states
foreach st of local states {

    * Import data
    local stl = strlower("`st'")
    use "`data'/clean/nfip_policies_state/`stl'.dta", clear

    * Create additional analysis variables 
    // i) Got elevated 
    // Note: These are properties for which we observe a change in elevation status over time
    bysort property_id (policy_year): gen got_elevated = elevated[_N] == 1 & elevated[1] == 0
    // ii) Elevation year 
    bysort property_id (policy_year): egen elevation_year = min(cond(elevated == 1, policy_year, .))
    replace elevation_year = . if got_elevated == 0
    // iii) Original policy year
    bysort property_id (policy_year): egen policy_year_init = min(policy_year)

    * Collapse to property level 
    // Note: NFIP community number and zipcode can change for the same structure over time 
    // due to administrative reasons. 
    // i) Set time-varying attributes to their most-recent value within each property
    foreach v of varlist *elevated ratedfloodzone primary_residence zipcode countycode {
        bysort property_id (policy_year): replace `v' = `v'[_N]
    }
    // ii) Drop time-varying variables 
    drop policy_year nfipratedcommunitynumber 
    duplicates drop
    isid property_id

    * Label 
    label var got_elevated              "Property got elevated"
    label var elevation_year            "Year property was observed elevated"
    label var policy_year_init          "Year of first NFIP policy"

    * Save 
    tempfile nrip_prop_`stl'
    sa "`nrip_prop_`stl''", replace

}

* -----------------------------------------------------------------------------
* Section 2: Append and save
* -----------------------------------------------------------------------------

* Append all state files 
clear
foreach st of local states {
    local stl = strlower("`st'")
    append using "`nrip_prop_`stl''"
}

* Make property_id unique across states
rename property_id property_id_state
egen property_id = group(state property_id_state)
drop property_id_state
order property_id
isid property_id

* Save
sa "`data'/clean/nfip_policies_property.dta", replace


/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-01

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

    * Create elevation year variable 
    bysort property_id (policy_year): egen elevation_year = min(cond(elevated == 1, policy_year, .))
    // Left-censored: already elevated in the first observed policy-year, so the true
    // elevation predates NFIP and elevation_year is only an upper bound for these.
    bysort property_id (policy_year): gen elev_left_censored = elevated[1] == 1

    * Collapse to property level 
    // Note: NFIP community number and zipcode can change for the same structure over time 
    // due to administrative reasons. 
    // i) Set time-varying attributes to their most-recent value within each property
    foreach v of varlist elevated ratedfloodzone primary_residence zipcode countycode {
        bysort property_id (policy_year): replace `v' = `v'[_N]
    }
    // ii) Drop time-varying variables 
    drop policy_year nfipratedcommunitynumber 
    duplicates drop
    isid property_id

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


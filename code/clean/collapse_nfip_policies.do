/******************************************************************************
Authors: Vendela Norman
Date: 2026-06-25

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

    * Collapse to property level 
    // Note: NFIP community number and zipcode can change for the same structure over time 
    // due to administrative reasons. 
    // Set time-varying attributes to their most-recent value within each property
    // (elevated is enforced monotonic upstream, so most-recent == ever-elevated)
    foreach v of varlist elevated ratedfloodzone primary_residence zipcode {
        bysort property_id (policy_year): replace `v' = `v'[_N]
    }
    // iii) Drop time-varying variables 
    drop policy_year nfipratedcommunitynumber countycode
    duplicates drop
    isid property_id

    * Save 
    tempfile nrip_prop_`stl'
    sa "`nrip_prop_`stl''", replace

}

* -----------------------------------------------------------------------------
* Section 2: Append and save
* -----------------------------------------------------------------------------

// TODO: (1) property_id is numbered per-state by egen group() upstream, so it
//       collides across states after the append (isid only checked per-state).
//       Regenerate on the combined file or drop it before saving.
//       (2) The append hardcodes TX as the base; make it order-independent
//       (clear + append over all states) so it doesn't break if TX is absent.

* Append
use "`nrip_prop_tx'", clear
foreach st of local states {
    if "`st'" != "TX" {
        local stl = strlower("`st'")
        append using "`nrip_prop_`stl''"
    }
}

* Save 
sa "`data'/clean/nfip_policies_property.dta", replace


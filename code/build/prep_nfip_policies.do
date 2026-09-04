/******************************************************************************
Authors: Vendela Norman
Date: 2026-09-03

Description: Appends the state-level FEMA NFIP policies data to a full property-year
    panel, then keeps a first-policy-year snapshot per property for the ATTOM match.

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

* Label variables
label var property_id           "Property ID (unique across states)"
label var property_id_state     "Property ID within state file"

* Save
order property_id property_id_state
sa "`data'/clean/nfip_policies_panel.dta", replace

* -----------------------------------------------------------------------------
* Section 2: Collapse to property level (for the ATTOM match)
* -----------------------------------------------------------------------------

* Keep the first policy year 
bysort property_id (policy_year): keep if _n == 1

* Rename
ren policy_year policy_year_init

* Keep match variables 
keep property_id property_id_state state construction_year policy_year_init zipcode ///
    censusblockgroupfips countycode nfipratedcommunitynumber flood_zone post_firm

* Label 
label var policy_year_init "Year of first NFIP policy"

* Save
order property_id property_id_state state policy_year_init construction_year
sa "`data'/clean/nfip_policies_property.dta", replace

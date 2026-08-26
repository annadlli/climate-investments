/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-08-14
Description: Assigns one ATTOM property to each NFIP property through tiered
    Wagner-style cells that exclude policy year. NFIP is the master; unmatched
    NFIP properties remain in the crosswalk. The fixed crosswalk is then merged
    onto the NFIP policy-year panel, so assigned ATTOMID never changes by year.
Notes / Sources: Requires a reduced one-row-per-ATTOMID Stata candidate file
    built from geocoded ATTOM + Wagner NFHL + Builty. Matching attributes on
    NFIP come from the first observed policy. All merges retain master and
    matched observations only, equivalent to keep(1 3).
******************************************************************************/

version 18
args data state

local st = lower("`state'")
local candidates "`data'/build/attom_nfhl_builty/`st'_attom_candidates.dta"
capture mkdir "`data'/build/nfip_attom_property"

* Prepare one row per NFIP property using initial-policy matching attributes.
use "`data'/clean/nfip_policies_property.dta", clear
keep if lower(state) == "`st'"
isid property_id

rename (zipcode_init censusblockgroupfips_init ///
    nfipratedcommunitynumber_init ratedfloodzone_init postfirm_init) ///
    (zip_key blockgroup_key community_key flood_zone_key postfirm_key)

replace flood_zone_key = upper(subinstr(trim(flood_zone_key), " ", "", .))
gen flood_risk_key = ""
replace flood_risk_key = "high_risk" if ///
    inlist(flood_zone_key, "A", "AE", "AH", "AO", "V", "VE")
replace flood_risk_key = "low_risk" if ///
    inlist(flood_zone_key, "B", "C", "D", "X", "XE")
gen construction_5yr = floor(construction_year / 5) * 5
gen construction_decade = floor(construction_year / 10) * 10

gen nfip_attom_merge_status = 1
gen assigned_attomid = ""
gen match_tier = ""
gen match_tier_number = .
tempfile nfip candidates_remaining links
save `nfip'

* ATTOM input is already reduced to one candidate row per ATTOMID.
use "`candidates'", clear
isid attomid
gen assigned = 0
save `candidates_remaining'

clear
set obs 0
gen property_id = .
gen assigned_attomid = ""
gen match_tier = ""
gen match_tier_number = .
save `links', replace

* Sequential one-to-one pairing within increasingly broad cells.
local keys1 "blockgroup_key flood_zone_key construction_year"
local keys2 "blockgroup_key flood_risk_key construction_year"
local keys3 "blockgroup_key flood_zone_key construction_5yr postfirm_key"
local keys4 "blockgroup_key flood_zone_key construction_decade postfirm_key"
local keys5 "zip_key flood_risk_key construction_decade postfirm_key"
local keys6 "community_key flood_zone_key construction_year"
local keys7 "community_key flood_risk_key construction_5yr postfirm_key"
local keys8 "community_key flood_risk_key construction_decade postfirm_key"

local label1 "1_bg_zone_exact_year"
local label2 "2_bg_risk_exact_year"
local label3 "3_bg_zone_5yr_postfirm"
local label4 "4_bg_zone_decade_postfirm"
local label5 "5_zip_risk_decade_postfirm"
local label6 "6_community_zone_exact_year"
local label7 "7_community_risk_5yr_postfirm"
local label8 "8_community_risk_decade_postfirm"

forvalues tier = 1/8 {
    local keys "`keys`tier''"
    local tier_label "`label`tier''"

    use `nfip', clear
    keep if nfip_attom_merge_status == 1
    gen missing_key = 0
    foreach key of local keys {
        replace missing_key = 1 if missing(`key')
    }
    keep if missing_key == 0
    drop missing_key
    sort `keys' property_id
    by `keys': gen cell_rank = _n
    keep property_id `keys' cell_rank
    tempfile nfip_tier
    save `nfip_tier'

    use `candidates_remaining', clear
    keep if assigned == 0
    gen missing_key = 0
    foreach key of local keys {
        replace missing_key = 1 if missing(`key')
    }
    keep if missing_key == 0
    drop missing_key
    sort `keys' attomid
    by `keys': gen cell_rank = _n
    keep attomid `keys' cell_rank
    rename attomid assigned_attomid
    merge 1:1 `keys' cell_rank using `nfip_tier', keep(3) nogen
    keep property_id assigned_attomid
    gen match_tier = "`tier_label'"
    gen match_tier_number = `tier'
    tempfile hits
    save `hits'

    use `links', clear
    append using `hits'
    save `links', replace

    use `nfip', clear
    merge 1:1 property_id using `hits', keep(1 3) update nogen
    replace nfip_attom_merge_status = 3 if !missing(assigned_attomid)
    save `nfip', replace

    use `candidates_remaining', clear
    rename attomid assigned_attomid
    merge 1:1 assigned_attomid using `hits', keep(1 3) nogen
    replace assigned = 1 if !missing(property_id)
    drop property_id match_tier match_tier_number
    rename assigned_attomid attomid
    save `candidates_remaining', replace
}

* Save the fixed property crosswalk, retaining all NFIP master properties.
use `nfip', clear
isid property_id
count if nfip_attom_merge_status == 3
count if nfip_attom_merge_status == 1
keep property_id property_id_state state policy_year_init assigned_attomid ///
    match_tier match_tier_number nfip_attom_merge_status
compress
save "`data'/build/nfip_attom_property/`st'_nfip_attom_crosswalk.dta", replace

* Rebuild the policy-year panel with a time-invariant assigned ATTOMID.
use "`data'/clean/nfip_policies_state/`st'.dta", clear
rename property_id property_id_state
merge m:1 property_id_state using ///
    "`data'/build/nfip_attom_property/`st'_nfip_attom_crosswalk.dta", ///
    keep(1 3) nogen keepusing(assigned_attomid match_tier ///
        match_tier_number nfip_attom_merge_status property_id)

sort property_id policy_year
compress
save "`data'/build/nfip_attom_property/`st'_nfip_attom_policy_panel.dta", replace

/******************************************************************************
Authors: Anna Li
Date: 2026-08-19

Description: Last step of the pipeline. Appends the 20 state link files from
    the matching run and merges them onto analysis.dta.

    analysis.dta stays the master: every NFIP property survives, matched or not,
    and no ATTOM-only or Builty-only rows are added.

******************************************************************************/

args data states

local links_dir "`data'/build/nfip_attom_property"

* -----------------------------------------------------------------------------
* 1. Append the state links
* -----------------------------------------------------------------------------
* one .dta per state, converted by finalize_nfip_attom_property.py
tempfile links
local first : word 1 of `states'
local first_lower = lower("`first'")
use "`links_dir'/`first_lower'_nfip_attom_property.dta", clear
foreach state of local states {
    local st = lower("`state'")
    if "`state'" != "`first'" append using "`links_dir'/`st'_nfip_attom_property.dta"
}

* the merge below is 1:1 on these two, so they had better be unique
isid state property_id_state
qui count
di as txt "linked NFIP properties: " as res %12.0fc r(N)

* One ATTOM house may not serve two NFIP properties. 
*The parquet finalization  audit checks count(assigned_attomid) == count(distinct assigned_attomid) before conversion.

save `links'

* 2. Merge onto the NFIP-base analysis file
* analysis.dta is the master -- load it first, note the row count, and check after merge
use "`data'/analysis/analysis.dta", clear
qui count
local before = r(N)

* property_id is an egen integer whose numbering depends on the source file's state ordering. 
* Use data-valued state-specific key instead and do keep 1 3 -> no ATTOM-only rows join the sample
merge 1:1 state property_id_state using `links', keep(master match) gen(attom_merge)

* unmatched properties get status 1, same code the matching step uses
replace nfip_attom_merge_status = 1 if missing(nfip_attom_merge_status)

* one clean indicator flag for "has an ATTOM property"
generate attom_matched = assigned_attomid != "" & !missing(assigned_attomid)

* States outside the compiled set have no link record at all; keep their Builty
* fields missing rather than letting 0 read as "no elevation".
replace builty_elevated     = . if attom_merge == 1
replace builty_n_properties = . if attom_merge == 1

* -----------------------------------------------------------------------------
* Labels for construction measures retained in the analysis file
* -----------------------------------------------------------------------------
label var attom_matched                   "NFIP property has an assigned ATTOM property"
label var builty_elevated                 "Builty elevation permit on assigned ATTOM property"
label var builty_elevation_year           "Earliest Builty elevation-permit year"
label var builty_n_properties             "Builty properties attached to assigned ATTOM property"
label var attom_flood_zone_original       "NFHL flood zone on assigned ATTOM property"
label var attom_nfhl_flood_matched        "Assigned ATTOM property intersects NFHL flood zone"
label var attom_nfhl_community_matched    "Assigned ATTOM property intersects NFHL community"
label var attom_value_year                "ATTOM tax year used for assigned property"
label var attom_value_lag                 "Reference year minus ATTOM tax year"
label var attom_market_value_total        "ATTOM total market value"
label var attom_market_value_land         "ATTOM land market value"
label var attom_market_value_improvements "ATTOM improvement market value"
label var attom_assessed_value_total      "ATTOM total assessed value"
label var attom_assessed_improvements     "ATTOM improvement assessed value"
label var attom_previous_assessed_value   "ATTOM previous total assessed value"
label var attom_last_sale_price           "ATTOM last recorded sale price"

* the appended links on their own, without analysis.dta attached
preserve
    use `links', clear
    compress
    save "`data'/build/nfip_attom_property_links.dta", replace
    di as result "Saved: `data'/build/nfip_attom_property_links.dta"
restore

* 3. Analysis version
*
* Drops the cell ranks, tier labels and normalized keys 
* Keeps the sample, the ATTOM values, the NFHL indicators and the Builty measures. 
* Keep note when drop happens -> indicate match problem 
foreach variable in attom_merge assigned_attomid nfip_attom_merge_status ///
    match_tier match_tier_number match_cell_id assignment_method ///
    nfip_cell_rank attom_cell_rank nfip_cell_n attom_cell_n ///
    builty_attom_cell_n cell_singleton builty_merge_status ///
    builty_attom_match_tier attom_flood_zone_key attom_flood_risk_key ///
    attom_property_use_std matching_policy_year nfhl_snapshot_year ///
    snapshot_year_gap reference_year value_reference_year ///
    construction_5yr construction_decade zip_key blockgroup_key county_key ///
    community_key flood_zone_key flood_risk_key postfirm_key ///
    nfip_flood_zone_original {
    drop `variable'
}

* this is the file the analysis reads. Diagnostics are moved now to separate file.
label data "NFIP analysis with stable-property ATTOM and Builty measures"
order property_id property_id_state state zipcode censusblockgroupfips ///
      construction_year policy_year_init
sort state property_id_state
compress
save "`data'/analysis/analysis_no_diagnostics.dta", replace
di as result "Saved clean stable-property analysis dataset"

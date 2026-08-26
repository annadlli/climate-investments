/******************************************************************************
Authors: Anna Li
Date: 2026-08-19

Description: Last step of the pipeline. Appends the 20 state link files from
    the matching run and merges them onto analysis.dta.

    analysis.dta stays the master: every NFIP property survives, matched or not,
    and no ATTOM-only or Builty-only rows are added.

    In   {data}/build/nfip_attom_property/{st}_nfip_attom_property.dta  (x20)
         {data}/analysis/analysis.dta

    Out  {data}/analysis/analysis_nfip_attom_property.dta   <- the analysis file
         {data}/analysis/analysis_nfip_attom_property_diagnostics.dta
         {data}/build/nfip_attom_property_links.dta
         {data}/build/nfip_attom_property_diagnostics.dta

******************************************************************************/

version 18
args data states

local links_dir "`data'/build/nfip_attom_property"

* -----------------------------------------------------------------------------
* 1. Append the state links
* -----------------------------------------------------------------------------
* one .dta per state, converted from parquet by parquet_to_dta.py
tempfile links
local first = 1
foreach state of local states {
    local st = lower("`state'")
    capture confirm file "`links_dir'/`st'_nfip_attom_property.dta"
    if _rc {
        di as error "compile: missing `links_dir'/`st'_nfip_attom_property.dta"
        exit 601
    }
    if `first' {
        use "`links_dir'/`st'_nfip_attom_property.dta", clear
        local first = 0
    }
    else append using "`links_dir'/`st'_nfip_attom_property.dta"
}
if `first' {
    di as error "compile: no state link files found in `links_dir'"
    exit 601
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

qui count
if r(N) != `before' {
    di as error "compile: merge changed the analysis row count (`before' -> " r(N) ")"
    exit 459
}

* unmatched properties get status 1, same code the matching step uses
qui replace nfip_attom_merge_status = 1 if missing(nfip_attom_merge_status)

* one clean indicator flag for "has an ATTOM property"
gen byte attom_matched = (assigned_attomid != "" & !missing(assigned_attomid))

* States outside the compiled set have no link record at all; keep their Builty
* fields missing rather than letting 0 read as "no elevation".
qui replace builty_elevated     = . if attom_merge == 1
qui replace builty_n_properties = . if attom_merge == 1

* -----------------------------------------------------------------------------
* Labels. Two groups, and the difference matters:
*   capture label var ...  -> fields inherited from analysis.dta, which may not
*                             all be present, so a miss is fine
*   label var ...          -> fields this build adds, which must exist; a miss
*                             means the matching step changed its output
* Note: for labels, got Claude to do them.
* -----------------------------------------------------------------------------
label data "NFIP analysis with stable-property ATTOM link diagnostics"
capture label var property_id                    "NFIP property identifier across states"
capture label var property_id_state              "NFIP property identifier within state"
capture label var state                          "State postal abbreviation"
capture label var zipcode                        "Five-digit ZIP code"
capture label var censusblockgroupfips           "Census block-group FIPS code"
capture label var construction_year              "Property construction year"
capture label var policy_year_init               "First observed NFIP policy year"
capture label var nfipratedcommunitynumber       "Current NFIP rated community number"
capture label var countycode                     "Current county FIPS code"
capture label var ratedfloodzone                 "NFIP rated flood zone"
capture label var sfha                           "Property is in a Special Flood Hazard Area"
capture label var fma_rl                         "NFIP repetitive-loss property"
capture label var fma_srl                        "NFIP severe repetitive-loss property"
capture label var fma_n_grants_zip               "FMA elevation grants pooled to ZIP"
capture label var fma_n_properties_zip           "FMA elevated properties pooled to ZIP"
capture label var fma_spend_zip                  "Real FMA elevation spending pooled to ZIP"
capture label var fma_bcr_zip                    "Mean FMA benefit-cost ratio in ZIP"
capture label var fma_year_min_zip               "First FMA obligation year in ZIP"
capture label var fma_year_max_zip               "Last FMA obligation year in ZIP"
capture label var fma_n_grants_county            "FMA elevation grants pooled to county"
capture label var fma_n_properties_county        "FMA elevated properties pooled to county"
capture label var fma_spend_county               "Real FMA elevation spending pooled to county"
capture label var fma_bcr_county                 "Mean FMA benefit-cost ratio in county"
capture label var fma_year_min_county            "First FMA obligation year in county"
capture label var fma_year_max_county            "Last FMA obligation year in county"
label var attom_merge                     "NFIP-to-stable-property-link merge result"
label var attom_matched                   "NFIP property has an assigned ATTOM property"
label var assigned_attomid                "ATTOM property assigned to this NFIP property"
label var nfip_attom_merge_status         "NFIP-to-ATTOM assignment status"
label var match_tier                      "Cell tier producing ATTOM assignment"
label var match_tier_number               "Numeric ATTOM assignment tier"
label var match_cell_id                   "Hashed identifier for assignment cell"
label var assignment_method               "Within-cell ATTOM assignment method"
label var nfip_cell_rank                  "NFIP property's rank within assignment cell"
label var attom_cell_rank                 "ATTOM property's rank within assignment cell"
label var nfip_cell_n                     "NFIP observations in assignment cell"
label var attom_cell_n                    "ATTOM candidates in assignment cell"
label var builty_attom_cell_n             "Builty-elevated ATTOM candidates in cell"
label var cell_singleton                  "Cell has one NFIP and one ATTOM property"
label var builty_elevated                 "Builty elevation permit on assigned ATTOM property"
label var builty_elevation_year           "Earliest Builty elevation-permit year"
label var builty_n_properties             "Builty properties attached to assigned ATTOM property"
label var builty_merge_status             "Builty-to-ATTOM merge status"
label var builty_attom_match_tier         "Address tier matching Builty to ATTOM"
label var attom_flood_zone_original       "NFHL flood zone on assigned ATTOM property"
label var attom_flood_zone_key            "Normalized NFHL flood zone used for matching"
label var attom_flood_risk_key            "NFHL high/low-risk category used for matching"
label var attom_nfhl_flood_matched        "Assigned ATTOM property intersects NFHL flood zone"
label var attom_nfhl_community_matched    "Assigned ATTOM property intersects NFHL community"
label var attom_property_use_std          "ATTOM standardized property-use code"
label var attom_value_year                "ATTOM tax year used for assigned property"
label var attom_value_lag                 "Reference year minus ATTOM tax year"
label var attom_market_value_total        "ATTOM total market value"
label var attom_market_value_land         "ATTOM land market value"
label var attom_market_value_improvements "ATTOM improvement market value"
label var attom_assessed_value_total      "ATTOM total assessed value"
label var attom_assessed_improvements     "ATTOM improvement assessed value"
label var attom_previous_assessed_value   "ATTOM previous total assessed value"
label var attom_last_sale_price           "ATTOM last recorded sale price"
label var matching_policy_year            "NFIP policy year supplying matching attributes"
label var nfhl_snapshot_year              "NFHL map snapshot year used for ATTOM enrichment"
label var snapshot_year_gap               "Absolute gap from NFHL snapshot year"
label var reference_year                  "NFIP reference year for property assignment"
label var value_reference_year            "Reference year for ATTOM tax-value selection"
label var construction_5yr                "Five-year construction bin used only for matching"
label var construction_decade             "Construction decade used only for matching"
label var zip_key                         "Normalized ZIP key used only for matching"
label var blockgroup_key                  "Normalized block-group key used only for matching"
label var county_key                      "Normalized county key used only for matching"
label var community_key                   "Normalized NFIP community key used only for matching"
label var flood_zone_key                  "Normalized NFIP flood-zone key used only for matching"
label var flood_risk_key                  "NFIP high/low-risk key used only for matching"
label var postfirm_key                    "Post-FIRM indicator used only for matching"
label var nfip_flood_zone_original        "Original NFIP rated zone carried into matching"

* first of two versions: keeps every match-machinery field, for auditing 
order property_id property_id_state state zipcode censusblockgroupfips ///
      construction_year policy_year_init
sort state property_id_state
compress
save "`data'/analysis/analysis_nfip_attom_property_diagnostics.dta", replace
di as result "Saved diagnostics-rich stable-property analysis dataset"

* the appended links on their own, without analysis.dta attached
preserve
    use `links', clear
    compress
    save "`data'/build/nfip_attom_property_links.dta", replace
    di as result "Saved: `data'/build/nfip_attom_property_links.dta"
restore

* 3. State-level merge diagnostics,
* matched rows only -- an unmatched state would drag every rate down
preserve
    qui keep if attom_merge == 3
    gen byte one = 1
    gen byte has_value = !missing(attom_value_year)
    gen byte is_builty = (builty_elevated == 1)
    collapse (sum) nfip_properties = one (sum) attom_matched = attom_matched ///
             (sum) with_attom_value = has_value (sum) builty_elevated = is_builty, ///
             by(state)
    gen double attom_match_rate = attom_matched / nfip_properties
    label data "Stable-property NFIP--ATTOM merge diagnostics"
    label var nfip_properties "NFIP properties represented in state link file"
    label var attom_matched "Properties assigned to an ATTOM property"
    label var with_attom_value "Assigned properties with an ATTOM tax value"
    label var builty_elevated "Assigned properties carrying a Builty elevation"
    label var attom_match_rate "Share of NFIP properties assigned to ATTOM"
    order state nfip_properties attom_matched attom_match_rate
    sort state
    compress
    save "`data'/build/nfip_attom_property_diagnostics.dta", replace
    di as result "Saved: `data'/build/nfip_attom_property_diagnostics.dta"
restore

* 4. Analysis version
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
    capture drop `variable'
}

* this is the file the analysis reads
label data "NFIP analysis with stable-property ATTOM and Builty measures"
order property_id property_id_state state zipcode censusblockgroupfips ///
      construction_year policy_year_init
sort state property_id_state
compress
save "`data'/analysis/analysis_nfip_attom_property.dta", replace
di as result "Saved clean stable-property analysis dataset"

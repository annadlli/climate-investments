/******************************************************************************
Authors: Anna Li
Date: 2026-09-02

Description: Creates the diagnostic-rich NFIP--ATTOM analysis file and the
    state match summary from the finalized property links. These outputs are
    deliberately separate from the construction pipeline.

******************************************************************************/

args data

* Read the main NFIP analysis and attach the stable-property link file so we can
* see which properties actually got an ATTOM match and which ones stayed unmatched.
use "`data'/analysis/analysis.dta", clear
merge 1:1 state property_id_state using ///
    "`data'/build/nfip_attom_property_links.dta", ///
    keep(master match) gen(attom_merge)

* The link file is the final truth for matched properties; unmatched rows keep the
* baseline status and we clear the Builty fields for the ones with no ATTOM match.
replace nfip_attom_merge_status = 1 if missing(nfip_attom_merge_status)
generate attom_matched = assigned_attomid != "" & !missing(assigned_attomid)
replace builty_elevated = . if attom_merge == 1
replace builty_n_properties = . if attom_merge == 1

label data "NFIP analysis with stable-property ATTOM link diagnostics"
label var attom_merge             "NFIP-to-stable-property-link merge result"
label var attom_matched           "NFIP property has an assigned ATTOM property"
label var assigned_attomid        "ATTOM property assigned to this NFIP property"
label var nfip_attom_merge_status "NFIP-to-ATTOM assignment status"
label var match_tier              "Cell tier producing ATTOM assignment"
label var match_tier_number       "Numeric ATTOM assignment tier"
label var match_cell_id           "Hashed identifier for assignment cell"
label var assignment_method       "Within-cell ATTOM assignment method"
label var nfip_cell_rank          "NFIP property's rank within assignment cell"
label var attom_cell_rank         "ATTOM property's rank within assignment cell"
label var nfip_cell_n             "NFIP observations in assignment cell"
label var attom_cell_n            "ATTOM candidates in assignment cell"
label var builty_attom_cell_n     "Builty-elevated ATTOM candidates in cell"
label var cell_singleton          "Cell has one NFIP and one ATTOM property"
label var builty_merge_status     "Builty-to-ATTOM merge status"
label var builty_attom_match_tier "Address tier matching Builty to ATTOM"
label var attom_flood_zone_key    "Normalized NFHL flood zone used for matching"
label var attom_flood_risk_key    "NFHL high/low-risk category used for matching"
label var attom_property_use_std  "ATTOM standardized property-use code"
label var matching_policy_year    "NFIP policy year supplying matching attributes"
label var nfhl_snapshot_year      "NFHL map snapshot year used for ATTOM enrichment"
label var snapshot_year_gap       "Absolute gap from NFHL snapshot year"
label var reference_year          "NFIP reference year for property assignment"
label var value_reference_year    "Reference year for ATTOM tax-value selection"
label var construction_5yr        "Five-year construction bin used only for matching"
label var construction_decade     "Construction decade used only for matching"
label var zip_key                 "Normalized ZIP key used only for matching"
label var blockgroup_key          "Normalized block-group key used only for matching"
label var county_key              "Normalized county key used only for matching"
label var community_key           "Normalized NFIP community key used only for matching"
label var flood_zone_key          "Normalized NFIP flood-zone key used only for matching"
label var flood_risk_key          "NFIP high/low-risk key used only for matching"
label var postfirm_key            "Post-FIRM indicator used only for matching"
label var nfip_flood_zone_original "Original NFIP rated zone carried into matching"

* Save the diagnostic-rich analysis file, then collapse it by state to get a simple
* summary of how many properties matched and how many had a value or elevation.
order property_id property_id_state state zipcode censusblockgroupfips ///
    construction_year policy_year_init
sort state property_id_state
compress
save "`data'/analysis/analysis_with_diagnostics.dta", replace

keep if attom_merge == 3
generate one = 1
generate has_value = !missing(attom_value_year)
generate is_builty = builty_elevated == 1
collapse (sum) nfip_properties = one (sum) attom_matched = attom_matched ///
    (sum) with_attom_value = has_value (sum) builty_elevated = is_builty, ///
    by(state)
generate attom_match_rate = attom_matched / nfip_properties

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

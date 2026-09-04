/******************************************************************************
Authors: Anna Li
Date: 2026-08-19

Description: Merge the alternate policy-year-consensus NFIP--ATTOM crosswalk
    onto the existing analysis dataset. Save one diagnostics-rich version and
    one analysis-facing version without matching machinery.

    The diagnostics-rich and clean files have identical observations. The
    clean file drops tier, cell, rank, collision, and intermediate matching
    variables, but retains substantive ATTOM values and Builty outcomes.

Input  : {data}/build/nfip_hma_panel.dta
         {data}/build/nfip_attom_property_crosswalk_policy_v2.dta
Output : {data}/analysis/analysis_nfip_attom_policy_consensus_diagnostics.dta
         {data}/analysis/analysis_nfip_attom_policy_consensus.dta
         {data}/build/nfip_attom_policy_consensus_merge_diagnostics.dta
******************************************************************************/

version 18
args data

* -----------------------------------------------------------------------------
* Merge the alternate crosswalk; nfip_hma_panel.dta remains the master.
* -----------------------------------------------------------------------------
use "`data'/build/nfip_hma_panel.dta", clear
qui count
local before = r(N)

merge 1:1 state property_id_state using ///
    "`data'/build/nfip_attom_property_crosswalk_policy_v2.dta", ///
    keep(master match) gen(attom_consensus_merge)

qui count
if r(N) != `before' {
    di as error "policy-consensus merge changed row count (`before' -> " r(N) ")"
    exit 459
}

replace attom_matched = 0 if missing(attom_matched)
replace builty_elevated = . if attom_consensus_merge == 1
replace builty_n_properties = . if attom_consensus_merge == 1

* -----------------------------------------------------------------------------
* Labels. Labels already present in nfip_hma_panel.dta survive because it is master.
* -----------------------------------------------------------------------------
label data "NFIP analysis with policy-year-consensus ATTOM link diagnostics"
capture label var property_id                    "NFIP property identifier across states"
capture label var property_id_state              "NFIP property identifier within state"
capture label var state                          "State postal abbreviation"
capture label var zipcode                        "Five-digit ZIP code"
capture label var censusblockgroupfips           "Census block-group FIPS code"
capture label var construction_year              "Property construction year"
capture label var policy_year_init               "First observed NFIP policy year"
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
label var attom_consensus_merge          "NFIP-to-consensus-crosswalk merge result"
label var assigned_attomid               "ATTOM property selected by policy-year consensus"
label var attom_matched                  "NFIP property has a surviving ATTOM assignment"
label var property_match_best_tier       "Finest tier supporting selected ATTOM assignment"
label var attom_years_matched            "Policy years supporting selected ATTOM assignment"
label var attom_assignment_share         "Share of matched policy years supporting selection"
label var attom_assignment_consistent    "Same ATTOM ID in every matched policy year"
label var attom_distinct_ids_across_years "Distinct ATTOM IDs assigned across policy years"
label var assignment_source_year         "Earliest policy year supporting selected assignment"
label var attom_collision_dropped        "Selected ATTOM ID lost in property collision resolution"
label var any_zone_exact                 "Selection supported by an exact NFHL-zone tier"
label var any_singleton_t1               "Selection supported by singleton tier-1 cell"
label var min_attom_cell_n               "Smallest ATTOM candidate cell supporting selection"
label var min_value_lag                  "Smallest ATTOM value-year lag supporting selection"
label var builty_elevated                "Builty elevation permit on selected ATTOM property"
label var builty_elevation_year          "Earliest Builty elevation-permit year"
label var builty_n_properties            "Builty properties attached to selected ATTOM property"
label var builty_merge_status            "Builty-to-ATTOM merge status"
label var builty_attom_match_tier        "Address tier matching Builty to ATTOM"
label var attom_flood_zone_original      "NFHL flood zone on selected ATTOM property"
label var attom_flood_zone_key           "Normalized NFHL flood zone used for matching"
label var attom_flood_risk_key           "NFHL high/low-risk category used for matching"
label var attom_nfhl_flood_matched       "Selected ATTOM property intersects NFHL flood zone"
label var attom_nfhl_community_matched   "Selected ATTOM property intersects NFHL community"
label var attom_property_use_std         "ATTOM standardized property-use code"
label var attom_value_year               "ATTOM tax year used for selected assignment"
label var attom_value_lag                "Assignment source year minus ATTOM tax year"
label var attom_market_value_total       "ATTOM total market value"
label var attom_market_value_land        "ATTOM land market value"
label var attom_market_value_improvements "ATTOM improvement market value"
label var attom_assessed_value_total     "ATTOM total assessed value"
label var attom_assessed_improvements      "ATTOM improvement assessed value"
label var attom_previous_assessed_value  "ATTOM previous total assessed value"
label var attom_last_sale_price          "ATTOM last recorded sale price"

order property_id property_id_state state zipcode censusblockgroupfips ///
      construction_year policy_year_init
sort state property_id_state
compress
save "`data'/analysis/analysis_nfip_attom_policy_consensus_diagnostics.dta", replace
di as result "Saved diagnostics-rich policy-consensus analysis dataset"

* -----------------------------------------------------------------------------
* State-level merge diagnostics.
* -----------------------------------------------------------------------------
preserve
    gen one = 1
    gen has_value = !missing(attom_value_year)
    gen is_builty = (builty_elevated == 1) if !missing(builty_elevated)
    collapse (sum) nfip_properties = one attom_matched = attom_matched ///
             with_attom_value = has_value builty_elevated = is_builty ///
             (mean) mean_assignment_share = attom_assignment_share ///
             consistent_share = attom_assignment_consistent, by(state)
    gen attom_match_rate = attom_matched / nfip_properties
    label data "Policy-year-consensus NFIP--ATTOM merge diagnostics"
    label var nfip_properties "NFIP properties in analysis master"
    label var attom_matched "Properties with surviving ATTOM assignment"
    label var with_attom_value "Assigned properties with an ATTOM tax value"
    label var builty_elevated "Assigned properties carrying a Builty elevation"
    label var mean_assignment_share "Mean selected-pair support across matched years"
    label var consistent_share "Share drawing one ATTOM ID across matched years"
    label var attom_match_rate "Share of NFIP properties assigned to ATTOM"
    order state nfip_properties attom_matched attom_match_rate
    sort state
    compress
    save "`data'/build/nfip_attom_policy_consensus_merge_diagnostics.dta", replace
restore

* -----------------------------------------------------------------------------
* Clean analysis-facing version: remove all linkage mechanics.
* -----------------------------------------------------------------------------
foreach variable in attom_consensus_merge assigned_attomid ///
    property_match_best_tier attom_years_matched attom_assignment_share ///
    attom_assignment_consistent attom_distinct_ids_across_years ///
    assignment_source_year attom_collision_dropped any_zone_exact ///
    any_singleton_t1 min_attom_cell_n min_value_lag builty_merge_status ///
    builty_attom_match_tier attom_flood_zone_key attom_flood_risk_key ///
    attom_property_use_std {
    capture drop `variable'
}

* Defensive cleanup if an upstream exporter later carries its raw cell keys.
foreach variable in match_tier match_tier_number match_cell_id ///
    assignment_method nfip_cell_rank attom_cell_rank nfip_cell_n ///
    attom_cell_n nfip_property_n builty_attom_cell_n cell_singleton ///
    construction_5yr construction_decade zip_key blockgroup_key county_key ///
    community_key flood_zone_key flood_risk_key postfirm_key reference_year {
    capture drop `variable'
}

label data "NFIP analysis with policy-year-consensus ATTOM and Builty measures"
order property_id property_id_state state zipcode censusblockgroupfips ///
      construction_year policy_year_init
sort state property_id_state
compress
save "`data'/analysis/analysis_nfip_attom_policy_consensus.dta", replace
di as result "Saved clean policy-consensus analysis dataset"

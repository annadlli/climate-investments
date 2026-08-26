/******************************************************************************
Authors: Anna Li
Date: 2026-08-19

Description: Collapse the policy-year NFIP--ATTOM assignments to one stable
    ATTOMID per NFIP property_id.

    assign_attom_to_nfip.py assigns within policy year, so a property can draw
    different ATTOMIDs in different years and an ATTOMID can serve different
    properties in different years. This resolves both: it picks one ATTOMID per
    property from the accumulated across-year evidence, then breaks any case
    where two properties claim the same ATTOM house.

    Selection priority within a property (Anna's spec):
        1. singleton tier-1 evidence
        2. more policy years agreeing on the same ATTOMID
        3. exact detailed-zone match (tiers 1, 3, 4, 6)
        4. smaller ATTOM cell
        5. smaller ATTOM value-year lag
        6. ATTOMID, as a deterministic final tie-break

    Input  : {data}/build/nfip_attom_policy_year_v2/{state}_policy_links.dta
             (written by export_policy_links.py)
    Output : {data}/build/nfip_attom_property_crosswalk_policy_v2.dta
             {data}/build/nfip_attom_property_crosswalk_policy_v2_diagnostics.dta
******************************************************************************/

version 18
args data states

local links_dir "`data'/build/nfip_attom_policy_year_v2"

* -----------------------------------------------------------------------------
* 1. Append the state policy-year link files
* -----------------------------------------------------------------------------
tempfile links
local first = 1
foreach state of local states {
    local st = lower("`state'")
    capture confirm file "`links_dir'/`st'_policy_links.dta"
    if _rc {
        di as txt "  skipping `st' (no policy link file)"
        continue
    }
    if `first' {
        use "`links_dir'/`st'_policy_links.dta", clear
        local first = 0
    }
    else append using "`links_dir'/`st'_policy_links.dta"
}
if `first' {
    di as error "collapse: no state policy link files found in `links_dir'"
    exit 601
}
di as txt "policy-year rows: " as res %12.0fc _N
save `links'

* Preserve the substantive fields at property x ATTOMID x policy-year grain.
* If the raw policy panel contains duplicate rows, they carry the same assigned
* ATTOM record; the deterministic sort leaves one copy for the later merge.
preserve
    keep if nfip_attom_merge_status == 3
    keep state property_id policy_year assigned_attomid ///
         builty_elevated builty_elevation_year builty_n_properties ///
         builty_merge_status builty_attom_match_tier ///
         attom_flood_zone_original attom_flood_zone_key ///
         attom_flood_risk_key attom_nfhl_flood_matched ///
         attom_nfhl_community_matched attom_property_use_std ///
         attom_value_year attom_value_lag attom_market_value_total ///
         attom_market_value_land attom_market_value_improvements ///
         attom_assessed_value_total attom_assessed_improvements ///
         attom_previous_assessed_value attom_last_sale_price
    rename policy_year assignment_source_year
    sort state property_id assigned_attomid assignment_source_year
    by state property_id assigned_attomid assignment_source_year: keep if _n == 1
    tempfile attributes
    save `attributes'
restore

* -----------------------------------------------------------------------------
* 2. The property universe -- every NFIP property, matched or not
* -----------------------------------------------------------------------------
keep state property_id
duplicates drop
tempfile universe
qui count
di as txt "NFIP properties:  " as res %12.0fc r(N)
save `universe'

* -----------------------------------------------------------------------------
* 3. Accumulate evidence per candidate property x ATTOMID
* -----------------------------------------------------------------------------
use `links', clear
keep if nfip_attom_merge_status == 3
drop if missing(assigned_attomid) | assigned_attomid == ""

* Detailed-zone tiers keep the exact NFHL zone; the others fall back to the
* high/low risk category.
gen byte zone_exact    = inlist(match_tier_number, 1, 3, 5, 7, 9, 15)
gen byte singleton_t1  = (match_tier_number == 1 & cell_singleton == 1)
gen long abs_value_lag = abs(attom_value_lag)

* Count distinct policy years, not rows: property_id x policy_year is not
* unique in the NFIP source (roughly 1% of rows).
bysort state property_id assigned_attomid policy_year: gen byte _year_first = (_n == 1)

collapse (sum) years_agree      = _year_first  ///
         (max) any_singleton_t1 = singleton_t1 ///
         (min) best_tier        = match_tier_number ///
         (max) any_zone_exact   = zone_exact  ///
         (min) min_attom_cell_n = attom_cell_n ///
         (min) min_value_lag    = abs_value_lag ///
         (min) source_year      = policy_year, ///
         by(state property_id assigned_attomid)

* -----------------------------------------------------------------------------
* 4. Pick one ATTOMID per property
* -----------------------------------------------------------------------------
bysort state property_id: gen int attom_distinct_ids_across_years = _N
bysort state property_id: egen long total_years_matched = total(years_agree)

gsort state property_id -any_singleton_t1 -years_agree best_tier ///
      -any_zone_exact min_attom_cell_n min_value_lag assigned_attomid
gen long _order = _n
bysort state property_id (_order): gen byte _pick = (_n == 1)
keep if _pick
drop _pick _order

gen double attom_assignment_share = years_agree / total_years_matched
gen byte   attom_assignment_consistent = (attom_distinct_ids_across_years == 1)

* -----------------------------------------------------------------------------
* 5. Resolve two properties claiming the same ATTOM house
*    Assignment is one-to-one within a policy year but not across years, so a
*    collision here would double-count one Builty permit as two elevations.
* -----------------------------------------------------------------------------
gsort assigned_attomid -any_singleton_t1 -years_agree best_tier ///
      -any_zone_exact min_attom_cell_n min_value_lag state property_id
gen long _order = _n
bysort assigned_attomid (_order): gen int _collision_rank = _n
gen byte attom_collision_dropped = (_collision_rank > 1)
qui count if attom_collision_dropped
di as txt "ATTOMID collisions dropped: " as res %12.0fc r(N)
replace assigned_attomid = "" if attom_collision_dropped
drop _order _collision_rank

rename best_tier       property_match_best_tier
rename years_agree     attom_years_matched
rename source_year     assignment_source_year

* Carry the attributes observed in the earliest policy year supporting the
* selected pair. This keeps values internally consistent with their tax year.
merge 1:1 state property_id assigned_attomid assignment_source_year ///
    using `attributes', keep(master match) nogen ///
    keepusing(builty_elevated builty_elevation_year builty_n_properties ///
              builty_merge_status builty_attom_match_tier ///
              attom_flood_zone_original attom_flood_zone_key ///
              attom_flood_risk_key attom_nfhl_flood_matched ///
              attom_nfhl_community_matched attom_property_use_std ///
              attom_value_year attom_value_lag attom_market_value_total ///
              attom_market_value_land attom_market_value_improvements ///
              attom_assessed_value_total attom_assessed_improvements ///
              attom_previous_assessed_value attom_last_sale_price)

* -----------------------------------------------------------------------------
* 6. Restore unmatched properties and save
* -----------------------------------------------------------------------------
merge 1:1 state property_id using `universe', keep(match using) nogen
replace assigned_attomid = "" if missing(assigned_attomid)
gen byte attom_matched = (assigned_attomid != "")

* The policy panels use a state-specific property identifier. Name it
* explicitly so this crosswalk can merge onto analysis.dta by
* state + property_id_state without confusing it with the global ID.
rename property_id property_id_state

label var assigned_attomid               "ATTOM property assigned to this NFIP property"
label var property_id_state              "NFIP property ID within state policy file"
label var attom_matched                  "NFIP property has a surviving ATTOM assignment"
label var property_match_best_tier       "Finest matching tier supporting the assignment"
label var attom_years_matched            "Policy years supporting the assigned ATTOMID"
label var attom_assignment_share         "Share of matched policy years agreeing"
label var attom_assignment_consistent    "One ATTOMID across all matched policy years"
label var attom_distinct_ids_across_years "Distinct ATTOMIDs drawn across policy years"
label var assignment_source_year         "Earliest policy year supporting the assignment"
label var attom_collision_dropped        "Lost the ATTOMID to a better-supported property"
label var any_zone_exact                 "Supported by an exact detailed-zone tier"
label var any_singleton_t1               "Supported by a singleton tier-1 cell"
label var min_attom_cell_n               "Smallest ATTOM cell supporting the assignment"
label var min_value_lag                  "Smallest ATTOM value-year lag supporting it"
label var builty_elevated                "Builty elevation permit on selected ATTOM property"
label var builty_elevation_year          "Earliest Builty elevation-permit year"
label var builty_n_properties            "Builty properties attached to selected ATTOM property"
label var builty_merge_status            "Builty-to-ATTOM merge status"
label var builty_attom_match_tier        "Address tier matching Builty to ATTOM"
label var attom_flood_zone_original      "NFHL flood zone on selected ATTOM property"
label var attom_flood_zone_key           "Normalized NFHL flood zone used for matching"
label var attom_flood_risk_key           "NFHL high/low-risk category used for matching"
label var attom_nfhl_flood_matched       "Selected ATTOM property intersects an NFHL flood zone"
label var attom_nfhl_community_matched   "Selected ATTOM property intersects an NFHL community"
label var attom_property_use_std         "ATTOM standardized property-use code"
label var attom_value_year               "ATTOM tax year used at assignment source year"
label var attom_value_lag                "Assignment source year minus ATTOM tax year"
label var attom_market_value_total       "ATTOM total market value"
label var attom_market_value_land        "ATTOM land market value"
label var attom_market_value_improvements "ATTOM improvement market value"
label var attom_assessed_value_total     "ATTOM total assessed value"
label var attom_assessed_improvements      "ATTOM improvement assessed value"
label var attom_previous_assessed_value  "ATTOM previous total assessed value"
label var attom_last_sale_price          "ATTOM last recorded sale price"

isid state property_id_state
* No ATTOM house may serve two NFIP properties -- step 5 should guarantee this.
qui duplicates report assigned_attomid if assigned_attomid != ""
if r(unique_value) != r(N) {
    di as error "collapse: assigned_attomid is not unique after collision resolution"
    exit 459
}
qui count if assigned_attomid != ""
di as txt "properties with an ATTOM assignment: " as res %12.0fc r(N)
compress
save "`data'/build/nfip_attom_property_crosswalk_policy_v2.dta", replace
di as result "Saved: `data'/build/nfip_attom_property_crosswalk_policy_v2.dta"

* -----------------------------------------------------------------------------
* 7. Diagnostics
* -----------------------------------------------------------------------------
preserve
    gen byte n = 1
    collapse (sum) properties = n ///
             (sum) assigned = attom_matched ///
             (sum) consistent = attom_assignment_consistent ///
             (sum) collisions = attom_collision_dropped ///
             (mean) mean_years_agreeing = attom_years_matched ///
             (mean) mean_assignment_share = attom_assignment_share, by(state)
    gen double assigned_share = assigned / properties
    list, noobs abbreviate(22)
    save "`data'/build/nfip_attom_property_crosswalk_policy_v2_diagnostics.dta", replace
    di as result "Saved: `data'/build/nfip_attom_property_crosswalk_policy_v2_diagnostics.dta"
restore

tab property_match_best_tier attom_assignment_consistent, missing

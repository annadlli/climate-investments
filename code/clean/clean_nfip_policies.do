/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-05-31

Description: Further-cleans the FEMA NFIP redacted policies ->
    clean/nfip_policies_{tx,va}.dta. The initial 30GB+ split and state
    extraction is done upstream in Python; this is the Stata cleanup.

Source: fema.gov/openfema-data-page/fima-nfip-redacted-policies-v2
******************************************************************************/

* Data root passed from master.do as the first argument
args data
local clean "`data'/clean"

// note:initial processing is done in python already,including splitting the 30+GB file and extracting relevant state only. this is cleaning up further
foreach st in tx va {

    use "`clean'/nfip_policies_`st'.dta", clear

    keep id property_state_clean latitude longitude ///
        policy_effective_date policy_termination_date policy_effective_year ///
        original_nb_date original_construction_year ///
        rated_flood_zone flood_zone_current  ///
        policy_count primary_residence single_family_policy is_elevated post_firm ///
        rental_property_ind floodproofed_ind mandatory_purchase_flag ///
        fips_county zip5 total_policy_premium total_building_coverage total_contents_coverage  nfip_rated_community_number
		//base_flood_elevation elevation_difference


foreach v in policy_effective_date policy_termination_date original_nb_date {
    capture confirm string variable `v'
    if !_rc {
        gen double `v'_num = clock(`v', "YMDhms")
        replace `v'_num = clock(substr(`v', 1, 19), "YMDhms") if missing(`v'_num)
        gen `v'_d = dofc(`v'_num)
        format `v'_d %td
        drop `v'_num `v'
        rename `v'_d `v'
    }
}

foreach v in latitude longitude ///
    policy_count total_policy_premium total_building_coverage total_contents_coverage ///
    original_construction_year policy_effective_year ///
    rental_property_ind floodproofed_ind mandatory_purchase_flag nfip_rated_community_number fips_county zip5{

    capture confirm string variable `v'
    if !_rc {
        destring `v', replace ignore(", $") force
    }
}
recast double latitude longitude total_policy_premium total_building_coverage total_contents_coverage, force

recast int policy_effective_year original_construction_year, force
recast byte policy_count primary_residence single_family_policy is_elevated post_firm, force

label variable total_contents_coverage "Total contents coverage amount"

gen double total_coverage = total_building_coverage + total_contents_coverage
label variable total_coverage "Total building plus contents coverage"

gen double premium_per_1000_coverage = total_policy_premium / (total_coverage / 1000) if total_coverage > 0
label variable premium_per_1000_coverage "Premium per $1,000 of total coverage"

gen has_building_policy = total_building_coverage > 0 if !missing(total_building_coverage)
label variable has_building_policy "Policy has building coverage"

gen has_contents_policy = total_contents_coverage > 0 if !missing(total_contents_coverage)
label variable has_contents_policy "Policy has contents coverage"

capture label define yesno 0 "No" 1 "Yes", replace
label values primary_residence yesno
label values single_family_policy yesno
label values is_elevated yesno
label values post_firm yesno
label values has_building_policy yesno
label values has_contents_policy yesno
    label variable id "FEMA NFIP policy record ID"
    label variable property_state_clean "Property state"
    label variable latitude "Property latitude"
    label variable longitude "Property longitude"
    label variable fips_county "County FIPS code"
    label variable zip5 "5-digit ZIP code"

    label variable policy_effective_date "Policy effective date"
    label variable policy_effective_year "Policy effective year"
    label variable policy_termination_date "Policy termination date"
    label variable original_nb_date "Original new business policy date"
    label variable original_construction_year "Original construction year"

    label variable primary_residence "Primary residence policy"
    label variable single_family_policy "Single-family residential policy"
    label variable rental_property_ind "Rental property indicator"
    label variable is_elevated "Elevated building indicator"
    label variable floodproofed_ind "Floodproofed building indicator"
    label variable mandatory_purchase_flag "Mandatory purchase requirement flag"
    label variable post_firm "Post-FIRM construction indicator"

    label variable policy_count "Policy count"
    label variable total_policy_premium "Total policy premium"
    label variable total_building_coverage "Total building coverage amount"
    label variable total_contents_coverage "Total contents coverage amount"

    label variable rated_flood_zone "NFIP rated flood zone"
    label variable flood_zone_current "Current flood zone"

    label variable nfip_rated_community_number "NFIP rated community number"

    capture label define yesno 0 "No" 1 "Yes", replace
    label values primary_residence yesno
    label values single_family_policy yesno
    label values is_elevated yesno
    label values post_firm yesno

    save "`clean'/nfip_policies_`st'.dta", replace
}

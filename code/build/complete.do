/******************************************************************************
Authors: Vendela Norman
Date: 2026-08-27

Description: Prepares the final analysis dataset.

******************************************************************************/

args data

* Import compiled analysis data
use "`data'/analysis/analysis.dta", clear

* Keep relevant variables 
keep state zipcode censusblockgroupfips property_id_state post_firm flood_zone ///
    sfha *rl* *srl* claim_cb fma*county policy_year_* premium_* policy_cost_* ///
    coverage_building risk_rating_2

* Save 
order property_id_state state zipcode censusblockgroupfips flood_zone sfha
sa "`data'/analysis/analysis_property.dta", replace

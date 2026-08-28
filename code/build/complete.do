/******************************************************************************
Authors: Vendela Norman
Date: 2026-08-27

Description: Prepares the final analysis dataset.

******************************************************************************/

args data

* Import compiled analysis data
use "`data'/analysis/analysis.dta", clear

* Keep relevant variables 
keep state zipcode censusblockgroupfips property_id_state postfirm ratedfloodzone ///
    sfha *rl* *srl* claim_cb fma*county

* Rename 
ren ratedfloodzone floodzone

* Save 
order property_id_state state zipcode censusblockgroupfips floodzone sfha
sa "`data'/analysis/analysis_property.dta", replace

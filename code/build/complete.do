/******************************************************************************
Authors: Vendela Norman, Anna Li
Date: 2026-09-15

Description: Prepares the final analysis dataset.

******************************************************************************/

args data states

* -----------------------------------------------------------------------------
* Section 1: Prepare ATTOM/Builty property links
* -----------------------------------------------------------------------------

* Append the state link files
clear
foreach st of local states {
    local stl = strlower("`st'")
    append using "`data'/build/nfip_attom_property/`stl'_nfip_attom_property.dta", ///
        keep(state property_id_state assigned_attomid builty_elevated builty_elevation_year ///
             builty_project_value builty_funding_type)
}
isid state property_id_state
tempfile links
save `links'

* -----------------------------------------------------------------------------
* Section 2: Merge datasets
* -----------------------------------------------------------------------------

* Import NFIP-HMA panel
use "`data'/build/nfip_hma_panel.dta", clear

* Merge Builty permit coverage
// TODO: Some localities within county don't report. Refine crosswalk. 
ren policy_year year
merge m:1 countycode year using "`data'/clean/builty_coverage_county.dta", keep(1 3) ///
    keepusing(year builty_covered_strict) gen(builty_merge)
ren year policy_year

* Merge ATTOM/Builty property links
merge m:1 state property_id_state using `links', keep(1 3) nogen
gen attom_matched = assigned_attomid != ""

* Merge in ATTOM market values 
rename (assigned_attomid policy_year) (attomid year)
merge m:1 attomid year using "`data'/build/attom_value/attom_value.dta", keep(1 3) nogen
rename (attomid year value) (assigned_attomid policy_year attom_value)

* -----------------------------------------------------------------------------
* Section 3: Create new analysis variables
* -----------------------------------------------------------------------------

* Set Builty elevations to missing where unobservable
replace builty_elevated = . if attom_matched != 1 | builty_merge != 3

* Harmonize the sources into one elevation year: Builty permit, else NFIP flag flip, else ICC payment
// Note: an ICC payment counts only if the property stays on the panel afterwards, since ICC also pays for demolition
bysort property_id (policy_year): egen _last_year = max(policy_year)
gen elevation_year = builty_elevation_year if builty_elevated == 1
replace elevation_year = nfip_flip if mi(elevation_year)
replace elevation_year = nfip_icc if mi(elevation_year) & nfip_icc < _last_year

* Elevation status
replace elevated = 1 if policy_year >= elevation_year
gen elevation_retrofit = !mi(elevation_year)

* Create elevation source variable 
gen elevation_source = cond(builty_elevated == 1, 3, cond(!mi(nfip_flip), 1, cond(elevation_retrofit, 2, 0)))
label define elevation_source_lbl 0 "None" 1 "NFIP elev. change" 2 "NFIP ICC" 3 "Builty permit"
label values elevation_source elevation_source_lbl

* Create funding variable 
// Note: Need to figure out how to incorporate HMA funding 
gen elevation_funded = inlist(builty_funding_type, 1, 2, 3, 5) | !mi(nfip_icc) if elevation_retrofit

* Rename 
rename (builty_project_value attom_value) (elevation_cost property_value)

* -----------------------------------------------------------------------------
* Section 4: Apply sample restrictions
* -----------------------------------------------------------------------------

* Drop properties whose id covers several structures 
drop if claim_collision == 1 

* Drop years w/ missing data
drop if policy_year > 2025

* Restrict to SFHAs
// Note: This is where the premium subsidies have the most bite
keep if sfha == 1 

* Drop extraneous variables
drop builty_merge countycode assigned_attomid property_id_state claim_collision ///
    builty_elevated builty_elevation_year builty_funding_type nfip_flip nfip_icc _last_year ///
    value_own value_year

* -----------------------------------------------------------------------------
* Section 5: Label and save
* -----------------------------------------------------------------------------

* Label
label var elevated                 "Home elevation status"
label var elevation_retrofit       "Elevation event observed in sample (Builty permit, NFIP flip or ICC)"
label var elevation_year           "Elevation year (retrofit)"
label var elevation_source         "Source of the elevation event"
label var elevation_funded         "Elevation was funded"
label var attom_matched            "NFIP property has an assigned ATTOM property"
label var property_value           "ATTOM market value in nearest tax year (2023 $)"
label var elevation_cost           "Elevation cost: Builty declared project value (2023 $), FL and TX permits"
label var builty_covered_strict    "County-year has >= 1 Builty permit per 100 properties"

* Save analysis dataset
order state property_id policy_year construction_year property_value post_firm ///
    sfha primary_residence elevated elevation_retrofit elevation_year elevation_source ///
    elevation_funded elevation_cost risk_rating_2 premium policy_cost coverage_building ///
    claim cumulative_claims claim_icc rl srl 
sort state property_id policy_year
compress
sa "`data'/analysis/analysis.dta", replace

* Save extract that can be used for Claude
bysort property_id (policy_year): gen _draw = runiform() if _n == 1
bysort property_id (policy_year): replace _draw = _draw[1]
keep if _draw < 0.5
drop _draw
sa "`data'/analysis/extracts/500M_subsample.dta", replace

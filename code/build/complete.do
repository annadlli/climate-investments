/******************************************************************************
Authors: Vendela Norman, Anna Li
Date: 2026-10-02

Description: Prepares the final analysis dataset.

******************************************************************************/

* 10-02: the elevation event is the ATTOM permit alone. The other elevation variables are kept in dataset.

args data states

* -----------------------------------------------------------------------------
* Section 1: Prepare ATTOM property / permit links
* -----------------------------------------------------------------------------

* Append the state link files
clear
foreach st of local states {
    local stl = strlower("`st'")
    append using "`data'/build/nfip_attom_property/`stl'_nfip_attom_property.dta", ///
        keep(state property_id_state assigned_attomid permit_elevated permit_elevation_year ///
             permit_retrofit permit_project_value permit_funding_type ///
             attom_flood_risk_key permit_completed permit_year_issued permit_status) // 
}
isid state property_id_state
tempfile links
save `links'

* -----------------------------------------------------------------------------
* Section 2: Merge datasets
* -----------------------------------------------------------------------------

* Import NFIP-HMA panel
use "`data'/build/nfip_hma_panel.dta", clear

* Merge permit coverage
// TODO: Some localities within county don't report. Refine crosswalk. 
ren policy_year year
merge m:1 countycode year using "`data'/clean/permit_coverage_county.dta", keep(1 3) ///
    keepusing(year permit_covered_strict) gen(permit_merge)
ren year policy_year

* Merge ATTOM property / permit links
merge m:1 state property_id_state using `links', keep(1 3) nogen
gen attom_matched = assigned_attomid != ""

* Merge in ATTOM market values 
rename (assigned_attomid policy_year) (attomid year)
merge m:1 attomid year using "`data'/build/attom_value/attom_value.dta", keep(1 3) nogen
rename (attomid year value) (assigned_attomid policy_year attom_value)

* -----------------------------------------------------------------------------
* Section 3: Create new analysis variables
* -----------------------------------------------------------------------------

* Permit flag at the property level: missing where a permit is unobservable (no ATTOM
* match, or the county never had a permit feed in the property's policy years);
* an address whose only permits are elevated new construction is not a retrofit
bysort property_id: egen _ever_covered = max(permit_merge == 3)
replace permit_elevated = . if attom_matched != 1 | _ever_covered == 0
replace permit_elevated = 0 if permit_elevated == 1 & permit_retrofit != 1
drop _ever_covered

* Elevation event: the permit's event year, carried to every policy year of the property
// 10-15 revision: permit_elevation_year is the event year from clean_permits.do (was the issue year)
gen elevation_year = permit_elevation_year if permit_elevated == 1
gen elevation_retrofit = !mi(elevation_year)
gen elevated_attom = policy_year >= elevation_year if !mi(permit_elevated)

* Permit completion flag (no restriction here; the analysis scripts apply the rule)
// 10-15 revision: permit_done = a FINAL or COMPLETE row in the status history, missing without a permit
gen permit_done = permit_completed == 1 if elevation_retrofit
quietly count if elevation_retrofit & permit_done
di "permitted homes with a completed permit: " r(N) " property-years"

* Funding named in the permit text
gen elevation_funded = inlist(permit_funding_type, 1, 2, 3, 5) if elevation_retrofit

* Rename 
rename (permit_project_value attom_value) (elevation_cost property_value)

* -----------------------------------------------------------------------------
* Section 4: Apply sample restrictions
* -----------------------------------------------------------------------------

* Drop properties whose id covers several structures 
drop if claim_collision == 1 

* Drop years w/ missing data
drop if policy_year > 2025

* Restrict to SFHAs
// Note: This is where the premium subsidies have the most bite
// 10-15 revision: where the NFIP rated zone is missing, take the NFHL zone of the matched house
quietly count if mi(sfha) & !mi(attom_flood_risk_key)
di "SFHA from the NFHL zone of the matched house (NFIP zone missing): " r(N) " property-years"
replace sfha = (attom_flood_risk_key == "high_risk") if mi(sfha) & !mi(attom_flood_risk_key)
drop attom_flood_risk_key
keep if sfha == 1 

* Drop extraneous variables
drop permit_merge countycode assigned_attomid property_id_state claim_collision ///
    permit_elevated permit_elevation_year permit_retrofit permit_funding_type ///
    value_own value_year permit_completed // 10-15 revision

* -----------------------------------------------------------------------------
* Section 5: Label and save
* -----------------------------------------------------------------------------

* Label
label var elevated                 "NFIP elevated-building flag (status, not used for events)"
label var elevated_attom           "Elevated per ATTOM permit: 1 from the permit year on; missing where a permit is unobservable"
label var elevation_retrofit       "Elevation permit observed (ATTOM building permit, retrofit)"
label var elevation_year           "Elevation year (permit finaled year, else last status year, else issue year)" // 
label var elevation_funded         "Grant named in the permit text"
label var attom_matched            "NFIP property has an assigned ATTOM property"
label var property_value           "ATTOM market value in nearest tax year (2023 $)"
label var elevation_cost           "Elevation cost: declared job value on the permit (2023 $)"
label var permit_covered_strict    "County-year has >= 1 ATTOM permit per 100 properties"
label var nfip_flip                "Year the NFIP elevated flag flips 0 to 1 (kept, not used)"
label var nfip_icc                 "Year of an ICC claim payment (kept, not used)"
label var permit_done              "Permit has a FINAL or COMPLETE status row (ATTOM status history); missing without a permit" //
label var permit_status            "Raw last-known status label on the permit record" // 
label var permit_year_issued       "Permit issue year (elevation_year is the event year)" // 

* Save analysis dataset
order state property_id policy_year construction_year property_value post_firm ///
    sfha primary_residence elevated elevated_attom elevation_retrofit elevation_year ///
    elevation_funded elevation_cost nfip_flip nfip_icc risk_rating_2 premium policy_cost ///
    coverage_building claim cumulative_claims claim_icc rl srl 
sort state property_id policy_year
compress
sa "`data'/analysis/analysis.dta", replace

* Save extract that can be used for Claude
bysort property_id (policy_year): gen _draw = runiform() if _n == 1
bysort property_id (policy_year): replace _draw = _draw[1]
keep if _draw < 0.5
drop _draw
cap mkdir "`data'/analysis/extracts" // 
sa "`data'/analysis/extracts/500M_subsample.dta", replace

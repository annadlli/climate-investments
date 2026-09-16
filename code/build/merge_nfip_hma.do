/******************************************************************************
Authors: Vendela Norman
Date: 2026-09-15

Description: Merges NFIP claims, multiple-loss status and FMA grants onto the
    NFIP policies panel.

******************************************************************************/

args data

* -----------------------------------------------------------------------------
* Section 1: Merge datasets
* -----------------------------------------------------------------------------

* Import NFIP policy data 
use "`data'/clean/nfip_policies_panel.dta", clear

* Merge NFIP multiple-loss data 
// Note: I believe many properties go unmatched (1) because the MLP dataset is a claims 
// subset of the NFIP policies data. Unmatched (2) due to sample restrictions in NFIP
// like restricting to single-family homes (the SFHA restriction is now deferred downstream).
merge m:1 originalconstructiondate censusblockgroupfips originalnbdate ///
    using "`data'/clean/nfip_multiple_loss.dta", keep(1 3) keepusing(nfip_rl nfip_srl) nogen 

* Merge NFIP claims data 
ren policy_year year_loss
merge 1:1 originalconstructiondate censusblockgroupfips originalnbdate year_loss ///
    using "`data'/clean/nfip_claims_panel.dta", keep(1 3) keepusing(claim_cb claim_icc claim_over_coverage) nogen
ren year_loss policy_year

* Merge in HMA grant data
// Note: Merged at the county level (available for all grants). The finer ZIP grain
// (clean/hma_zip.dta) only covers grants FEMA logged at the property level, so it is
// left for later as a more granular option where available. Property-level HMA
// data would require the FOIA requests to come through.
merge m:1 countycode using "`data'/clean/hma_county.dta", keep(1 3) ///
    keepusing(hma_n_properties hma_spend hma_year_min hma_year_max) nogen

* -----------------------------------------------------------------------------
* Section 2: Clean and create new variables
* -----------------------------------------------------------------------------

* Set missings to 0   
foreach var in claim_cb claim_icc claim_over_coverage nfip_rl nfip_srl hma_n_properties hma_spend { 
    replace `var' = 0 if mi(`var')
}

* Rename
ren (nfip_rl nfip_srl claim_cb) (rl srl claim)

* Create additional variables 
// i) Cumulative claims paid (building + contents)
bysort property_id (policy_year): gen cumulative_claims = sum(claim)
// ii) NFIP elevation 
by property_id: gen _flip = elevated == 1 & elevated[_n-1] == 0 if _n > 1
by property_id: egen nfip_flip = min(cond(_flip == 1, policy_year, .))
by property_id: egen nfip_icc  = min(cond(claim_icc > 0, policy_year, .))
// iii) Property id shared by several structures: a payout above coverage in any year
by property_id: egen claim_collision = max(claim_over_coverage)

* Drop extraneous variables
drop originalconstructiondate originalnbdate censustract nfipratedcommunitynumber ///
    _flip claim_over_coverage // countycode kept for the Builty coverage merge in complete.do

* -----------------------------------------------------------------------------
* Section 3: Save
* -----------------------------------------------------------------------------

* Label variables
label var cumulative_claims  "Cumulative claims paid, building + contents (2023 $)"
label var rl                 "Repetitive-loss property"
label var srl                "Severe-repetitive-loss property"    
label var claim_icc          "Increased Cost of Compliance paid (2023 $)" 
label var nfip_flip          "Year of change in NFIP elevation status"
label var nfip_icc           "Year of first ICC payment"
label var claim_collision    "Property id shared by several structures (payout exceeded coverage)"

* Save
sort state property_id policy_year
order state property_id policy_year construction_year post_firm sfha primary_residence ///
    elevated risk_rating_2 rl srl premium policy_cost coverage_building claim ///
    cumulative_claims claim_icc nfip_flip nfip_icc claim_collision
order zipcode censusblockgroupfips property_id_state, last
compress 
save "`data'/build/nfip_hma_panel.dta", replace
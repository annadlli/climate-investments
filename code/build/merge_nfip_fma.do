/******************************************************************************
Authors: Vendela Norman
Date: 2026-09-03

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
    using "`data'/clean/nfip_claims_panel.dta", keep(1 3) keepusing(claim_cb) nogen
ren year_loss policy_year

* Merge in FMA grant data
// Note: Merged at the county level (available for all grants). The finer ZIP grain
// (clean/fma_zip.dta) only covers grants FEMA logged at the property level, so it is
// left for later as a more granular option where available. Property-level FMA
// data would require the FOIA requests to come through.
merge m:1 countycode using "`data'/clean/fma_county.dta", keep(1 3) keepusing(fma_*) nogen

* -----------------------------------------------------------------------------
* Section 2: Clean and save
* -----------------------------------------------------------------------------

* Set missings to 0   
foreach var in claim_cb nfip_rl nfip_srl fma_n_properties fma_spend {
    replace `var' = 0 if mi(`var')
}

* Rename
ren (nfip_rl nfip_srl claim_cb) (rl srl claim)

* Create additional variables 
bysort property_id (policy_year): gen cumulative_claims = sum(claim)

* Drop extraneous variables
drop originalconstructiondate originalnbdate censustract nfipratedcommunitynumber ///
    fma_n_grants // countycode kept for the Builty coverage merge in complete.do

* Label variables
label var cumulative_claims  "Cumulative claims paid, building + contents"
label var rl                 "Repetitive-loss property"
label var srl                "Severe-repetitive-loss property"    

* Save
sort state property_id policy_year
order state property_id policy_year construction_year post_firm sfha primary_residence ///
    elevated risk_rating_2 rl srl premium policy_cost coverage_building claim ///
    cumulative_claims 
order zipcode censusblockgroupfips property_id_state, last
compress 
save "`data'/build/nfip_hma_panel.dta", replace
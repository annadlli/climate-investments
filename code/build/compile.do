/******************************************************************************
Authors: Vendela Norman
Date: 2026-09-02

Description: Compiles the property-level analysis dataset, starting from NFIP-
    insured homes. 

******************************************************************************/

args data

* Import NFIP policy data 
use "`data'/clean/nfip_policies_panel.dta", clear

* Merge NFIP multiple-loss data 
// Note: I believe many properties go unmatched (1) because the MLP dataset is a claims 
// subset of the NFIP policies data. Unmatched (2) due to sample restrictions in NFIP
// like restricting to single-family homes (the SFHA restriction is now deferred downstream).
merge m:1 originalconstructiondate censusblockgroupfips originalnbdate ///
    using "`data'/clean/nfip_multiple_loss.dta", keep(1 3) ///
    keepusing(totallosses mitigatedindicator *rl* *srl*) nogen 

* Merge NFIP claims data 
ren policy_year year_loss
merge 1:1 originalconstructiondate censusblockgroupfips originalnbdate year_loss ///
    using "`data'/clean/nfip_claims_panel.dta", keep(1 3) keepusing(claim_cb) nogen
ren year_loss policy_year

* Create additioonal variables 
bysort property_id (policy_year): gen total_claims = sum(claim_cb)

stop 

* Merge in FMA grant data
// Note: Until the FOIA requests come through this will have to be at the zip/county level
// i) ZIP (primary grain)
merge m:1 zipcode using "`data'/clean/fma_zip.dta", keep(1 3) ///
    keepusing(n_grants n_properties fma_spend bcr year_min year_max) nogen
ren (n_grants n_properties fma_spend bcr year_min year_max) ///
    (fma_n_grants_zip fma_n_properties_zip fma_spend_zip fma_bcr_zip ///
     fma_year_min_zip fma_year_max_zip)
// ii) County (fallback grain)
merge m:1 countycode using "`data'/clean/fma_county.dta", keep(1 3) ///
    keepusing(n_grants n_properties fma_spend bcr year_min year_max) nogen
ren (n_grants n_properties fma_spend bcr year_min year_max) ///
    (fma_n_grants_county fma_n_properties_county fma_spend_county fma_bcr_county ///
     fma_year_min_county fma_year_max_county)

* Set missings to 0   
foreach var in claim_cb fma_rl fma_srl {
    replace `var' = 0 if mi(`var')
}
foreach grain in zip county {
    foreach var in fma_n_grants fma_n_properties fma_spend {
        replace `var'_`grain' = 0 if mi(`var'_`grain')
    }
}

* Drop extraneous variables
drop originalconstructiondate originalnbdate countycode censustract
drop got_elevated elevation_year elevated // not reliably recorded in NFIP data 

* Note: Need to merge in ATTOM and Builty data. And create a new elevation variable (replacing got_elevated)

* Save analysis dataset
sort state zipcode censusblockgroupfips
order property_id state zipcode censusblockgroupfips construction_year ///
    policy_year_init 
order flood_zone sfha, last
compress
save "`data'/analysis/analysis.dta", replace
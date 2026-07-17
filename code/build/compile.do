/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-16

Description: Compiles the property-level analysis dataset, starting from NFIP-
    insured homes. 

******************************************************************************/

args data

* Import NFIP policy data 
use "`data'/clean/nfip_policies_property.dta", clear

* Merge NFIP multiple-loss data 
// Note: I believe many properties go unmatched (1) because the MLP dataset is a claims 
// subset of the NFIP policies data. Unmatched (2) due to sample restrictions in NFIP
// like excluding SFHAs and restricting to single-family homes. 
merge m:1 originalconstructiondate censusblockgroupfips originalnbdate ///
    using "`data'/clean/nfip_multiple_loss.dta", keep(1 3) keepusing(fma_rl fma_srl) nogen 

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

* Set missing RL/SRL to 0
// Note: Properties must have filed a claim to be classified as RL or SRL    
replace fma_rl = 0 if missing(fma_rl)
replace fma_srl = 0 if missing(fma_srl)

* Set missing FMA counts/spend to 0
foreach grain in zip county {
    foreach var in fma_n_grants fma_n_properties fma_spend {
        replace `var'_`grain' = 0 if mi(`var'_`grain')
    }
}

* Drop extraneous variables 
drop originalconstructiondate originalnbdate countycode censustract

* Save analysis dataset
sort state zipcode censusblockgroupfips
order property_id state zipcode censusblockgroupfips construction_year ///
    policy_year_init elevated got_elevated elevation_year
order ratedfloodzone, last 
compress
save "`data'/analysis/analysis.dta", replace
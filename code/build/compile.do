/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-10

Description: Compiles the property-level analysis dataset, starting from NFIP-
    insured homes. 

******************************************************************************/

args data

* Import NFIP policy data 
use "`data'/clean/nfip_policies_property.dta", clear

* Merge NFIP multiple-loss data 
// Note: I believe many properties go unmatched because the MLP dataset is a claims 
// subset of the NFIP policies data?
// Note: Should check that unmatched ML properties are SFHAs. Probably also state coverage issue. 
merge m:1 originalconstructiondate censusblockgroupfips originalnbdate ///
    using "`data'/clean/nfip_multiple_loss.dta", keep(1 3) keepusing(fma_rl fma_srl) nogen 

* Set missing RL/SRL to 0
// Note: Properties must have filed a claim to be classified as RL or SRL    
replace fma_rl = 0 if missing(fma_rl)
replace fma_srl = 0 if missing(fma_srl)

* Merge in FMA grant data 
// Note: Until the FOIA requests come through this will have to be at the county level 
// (though we should check what's in the property-level version)
merge m:1 countycode elevated using "`data'/clean/fma_county.dta", keep(1 3) ///
    keepusing(n_grants n_properties fma_spend bcr *year*) 

/* * Deflate by CPI 
gen year = year_elev_min
merge m:1 year using "`data'/clean/cpi.dta", keep(master match) keepusing(cpi) nogen
replace fma_spend = fma_spend / cpi if !mi(fma_spend) & !mi(cpi)
drop year cpi */

* Drop extraneous variables 
drop originalconstructiondate originalnbdate countycode censustract

* Save analysis dataset
sort state zipcode censusblockgroupfips
order property_id state zipcode censusblockgroupfips construction_year ///
    policy_year_init elevated got_elevated elevation_year
compress
save "`data'/analysis/analysis.dta", replace
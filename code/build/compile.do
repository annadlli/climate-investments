/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-01

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

* Save analysis dataset
sort state zipcode censusblockgroupfips
save "`data'/analysis/analysis.dta", replace

stop 



* -----------------------------------------------------------------------------
* 3. Merge FMA grants (county level: many properties -> one county)
* -----------------------------------------------------------------------------
preserve
    use "`data'/clean/fma_elevation_grants.dta", clear
    keep if state_code == 51   // VA
    * FMA county_code is 3-digit; build the 5-digit FIPS to match NFIP countycode
    gen countycode = string(state_code, "%02.0f") + string(county_code, "%03.0f")
    collapse (count) fma_n_grants     = project_identifier ///
             (sum)   fma_n_properties = number_of_properties ///
             (sum)   fma_total_amount = project_amount, by(countycode)
    tempfile fma
    save `fma'
restore
merge m:1 countycode using `fma', keep(master match) nogen
foreach v in fma_n_grants fma_n_properties fma_total_amount {
    replace `v' = 0 if missing(`v')
}

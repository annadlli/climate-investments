/******************************************************************************
Authors: Vendela Norman
Date: 2026-06-25

Description: Compiles the property-level analysis dataset, starting from NFIP-
    insured homes. 

******************************************************************************/

args data

* Import NFIP policy data 
// Note: Just Texas for now
use "`data'/clean/nfip_policies_state/tx.dta", clear

* Merge NFIP multiple-loss data 
// Note: Many properties go unmatched because the MLP dataset is a claims subset
// of the NFIP policies data?
merge m:1 originalconstructiondate censusblockgroupfips originalnbdate ///
    using "`data'/clean/nfip_multiple_loss.dta", keep(1 3) keepusing(fma_rl fma_srl)

* Set missing RL/SRL to 0    
replace fma_rl = 0 if missing(fma_rl)
replace fma_srl = 0 if missing(fma_srl)

stop 






stop 









* -----------------------------------------------------------------------------
* 1. NFIP base: collapse policy-years -> one row per property (VA)
* -----------------------------------------------------------------------------
use "`data'/clean/nfip_policies_state/va.dta", clear

* Cross-dataset string match key (see header prereq: needs originalnbdate)
gen geo_key  = cond(missing(censusblockgroupfips) | censusblockgroupfips == "", ///
    "z" + zipcode, "b" + censusblockgroupfips)
gen prop_key = geo_key + "|" + string(construction_year) + "|" + originalnbdate

* Collapse to property level. TODO: finalize per-variable aggregation rules.
collapse (firstnm) state countycode zipcode censustract censusblockgroupfips ///
                   nfipratedcommunitynumber construction_year ratedfloodzone ///
         (max)     elevated primary_residence ///
         (min)     first_policy_year = policy_year ///
         (max)     last_policy_year  = policy_year ///
         (count)   n_policy_years    = policy_year, by(prop_key)

* -----------------------------------------------------------------------------
* 2. Merge NFIP multiple-loss (RL/SRL status), at property level
* -----------------------------------------------------------------------------
preserve
    use "`data'/clean/nfip_multiple_loss.dta", clear
    keep if state == "VA"
    gen geo_key  = cond(missing(censusblockgroupfips) | censusblockgroupfips == "", ///
        "z" + zipcode, "b" + censusblockgroupfips)
    gen prop_key = geo_key + "|" + string(construction_year) + "|" + originalnbdate
    * collapse to one row per property (a few prop_keys may carry >1 MLP record)
    collapse (max) fmarl fmasrl nfiprl nfipsrl mitigatedindicator ///
             (sum) totallosses, by(prop_key)
    tempfile mlp
    save `mlp'
restore
merge 1:1 prop_key using `mlp', keep(master match) nogen
* non-multiple-loss properties are, by definition, not RL/SRL
foreach v in fmarl fmasrl nfiprl nfipsrl mitigatedindicator totallosses {
    replace `v' = 0 if missing(`v')
}

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

* -----------------------------------------------------------------------------
* 4. Save
* -----------------------------------------------------------------------------
compress
save "`data'/build/compile_va.dta", replace

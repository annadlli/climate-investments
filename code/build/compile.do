/******************************************************************************
Authors: Vendela Norman
Date: 2026-06-19

CLAUDE SKETCH TO BE AUDITED

Description: Compiles the property-level analysis dataset. Base = NFIP policies
    (eligible-homes universe) collapsed to one row per property; then merges in
    NFIP multiple-loss (RL/SRL status) and FMA grants (county level).

    STARTER -- VA only for now.
    TODO: generalize to all states (loop `states`); confirm the NFIP base is
    property-level (collapse policy-years -> property, below).

Inputs:  clean/nfip_policies_state/{st}.dta   (policy-year level)
         clean/nfip_multiple_loss.dta          (RL/SRL roster, national)
         clean/fma_elevation_grants.dta        (FMA grants, national, project level)
Output:  build/compile_{st}.dta                (property level)

PREREQUISITE -- cross-dataset match key:
    The merges join on a STRING key  prop_key = geo | construction_year | originalNBDate
    (geo = census block group, ZIP fallback), built identically in each file.
    NOT on `property_id` -- that is an egen-group integer numbered per-dataset and will
    NOT match across files.
    clean_nfip_policies must retain `originalnbdate` (it currently drops it) so the NFIP
    base can form prop_key. MLP + FMA already carry their components.
******************************************************************************/

args data

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

/******************************************************************************
Author: Anna Li
Date: 2026-09-20

Description: HMA home-elevation grants restricted to closed (completed) projects.
    (1) County file clean/hma_county_closed.dta, built like prep_hma.do section 2 but
        keeping only projects whose status in the raw OpenFEMA projects file is "Closed";
        read by empirical_facts_six_questions.do when called with the argument "closed".
    (2) Summary-table rows for the closed-only county counts and spend on the analysis
        sample, next to the all-funded rows the panel carries.

Inputs:  clean/hma_elevation.dta, raw/HazardMitigationAssistanceProjects.csv, analysis/analysis.dta
Outputs: clean/hma_county_closed.dta, ../output/tables/summary_table_hma_closed.xlsx
Run:     do hma_closed_projects.do "<data root>" "<output folder>"
******************************************************************************/

args data output

* -----------------------------------------------------------------------------
* Section 1: county file, closed projects only
* -----------------------------------------------------------------------------

import delimited "`data'/raw/HazardMitigationAssistanceProjects.csv", varnames(1) case(lower) stringcols(_all) bindquote(strict) clear
keep projectidentifier status
gen str100 project_identifier = projectidentifier // import gives a strL, which cannot be a merge key
drop projectidentifier
duplicates drop project_identifier, force
tempfile hma_status
save `hma_status'

use "`data'/clean/hma_elevation.dta", clear
merge m:1 project_identifier using `hma_status', keep(1 3) nogen keepusing(status)
keep if status == "Closed"
drop if mi(county_code) | county_code == 0

* Apportion project spend across the project's logged properties (as in prep_hma.do)
bysort project_identifier: egen n_logged = total(n_properties_rec)
gen spend = cond(project_merge == 2, hma_spend, hma_spend * (n_properties_rec / n_logged))
gen n_props = cond(project_merge == 2, n_properties, n_properties_rec)
egen proj_tag = tag(state_code county_code project_identifier)
collapse (sum) hma_n_grants = proj_tag hma_n_properties = n_props hma_spend = spend ///
         (min) hma_year_min = obligation_year (max) hma_year_max = year_closed ///
         (firstnm) state county, by(state_code county_code)
gen countycode = string(state_code, "%02.0f") + string(county_code, "%03.0f")
drop county_code
label var hma_n_grants     "HMA elevation grants (closed projects) in county"
label var hma_n_properties "Properties elevated under closed HMA grants in county"
label var hma_spend        "HMA elevation spending, closed projects, in county (2023 $)"
order state state_code county countycode
sort countycode
compress
save "`data'/clean/hma_county_closed.dta", replace

* -----------------------------------------------------------------------------
* Section 2: summary rows on the analysis sample
* -----------------------------------------------------------------------------

use property_id policy_year post_firm censusblockgroupfips hma_n_properties hma_spend ///
    using "`data'/analysis/analysis.dta", clear
bysort property_id (policy_year): keep if _n == _N
gen countycode = substr(censusblockgroupfips, 1, 5)
rename (hma_n_properties hma_spend) (hma_n_properties_all hma_spend_all)
merge m:1 countycode using "`data'/clean/hma_county_closed.dta", keep(1 3) nogen ///
    keepusing(hma_n_properties hma_spend)
rename (hma_n_properties hma_spend) (hma_n_properties_closed hma_spend_closed)
foreach var in hma_n_properties_closed hma_spend_closed {
    replace `var' = 0 if mi(`var')
}

mat M = J(4, 4, .)
local i = 1
foreach var in hma_n_properties_all hma_spend_all hma_n_properties_closed hma_spend_closed {
    qui sum `var'
    mat M[`i', 1] = r(N)
    mat M[`i', 2] = r(mean)
    qui sum `var' if post_firm == 0
    mat M[`i', 3] = r(mean)
    qui sum `var' if post_firm == 1
    mat M[`i', 4] = r(mean)
    local i = `i' + 1
}
matrix rownames M = hma_n_properties_all hma_spend_all hma_n_properties_closed hma_spend_closed
matrix colnames M = N mean_all mean_pre_firm mean_post_firm
mat list M
putexcel set "`output'/tables/summary_table_hma_closed.xlsx", replace
putexcel A1 = matrix(M), names

/******************************************************************************
Authors: Anna Li
Date: 2026-07-01

Description: Collapses FEMA HMA grant data from project level to county level

******************************************************************************/

args data

use "`data'/clean/fma_elevation_grants.dta",clear


* Build a five-digit county FIPS key used by downstream merge files.
gen countycode = string(state_code, "%02.0f") + string(county_code, "%03.0f")
destring countycode, replace

* Count each grant/project row once in the county-level collapse.
gen fma_one = 1

* Collapse project-level funding, property counts, and benefits to county level.
collapse (sum)     fma_n_grants        = fma_one                  ///
                   fma_n_properties    = number_of_properties     ///
                   fma_project_amount  = project_amount           ///
                   fma_fed_obligated   = federal_share_obligated  ///
                   fma_net_benefits    = net_value_benefits       ///
         (mean)    fma_avg_bcr         = bcr, by(state state_code countycode)

* Add grant-presence flag and log funding measure.
gen fma_any               = fma_n_grants > 0
gen fma_log_fed_obligated = log(fma_fed_obligated + 1)

* Label variables created by the collapse.
label var fma_any               "County has any FMA elevation grant"
label var fma_n_grants          "FMA elevation grants in county"
label var fma_fed_obligated     "FMA federal share obligated in county"
label var fma_net_benefits      "FMA net benefits in county"

* Finalize the county-level FMA grants file.
order state state_code countycode
sort state_code countycode
compress
save "`data'/clean/fma_grants_county.dta", replace

/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-02

Description: Summary statistics table for the NFIP property analysis set.
    One row per variable, one column per statistic. Add a variable to a list
    at the top to add a row. Dollar variables are in 2023 dollars, deflated
    upstream in the cleaners.

******************************************************************************/

args data output

* Set table options
local vars policy_year_init sfha post_firm primary_residence ///
    premium_init premium_last ///
    any_claim claim_cb claim_cb_claimant claim_to_value claim_over_value ///
    nfip_rl nfip_srl ///
    attom_matched attom_value ///
    builty_elevated builty_elevation_year ///
    fma_n_grants_zip fma_spend_zip
local vars_builty builty_project_value
local stats N mean sd min p50 max

* -----------------------------------------------------------------------------

* Import data
use "`data'/analysis/analysis_no_diagnostics.dta", clear

* Create derived variables
// Note: ATTOM logs zero where it holds no value, so zeros are treated as missing
gen any_claim = claim_cb > 0 & !mi(claim_cb)
gen claim_cb_claimant = claim_cb if any_claim
gen attom_value = attom_market_value_total if attom_market_value_total > 0
gen claim_to_value = claim_cb / attom_value if any_claim
gen claim_over_value = claim_to_value > 1 if !mi(claim_to_value)

* Tabulate statistics
local nvars : word count `vars' `vars_builty'
local nstats : word count `stats'
mat M = J(`nvars', `nstats', .)
local i = 1
foreach var of local vars {
    qui sum `var', detail
    local j = 1
    foreach stat of local stats {
        mat M[`i', `j'] = r(`stat')
        local j = `j' + 1
    }
    local i = `i' + 1
}

* Add elevation project cost from the Builty property file
// Note: project_value is already in 2023 dollars (clean_builty.do); zeros are unreported
use "`data'/clean/builty_elevations.dta", clear
gen builty_project_value = project_value if project_value > 0
foreach var of local vars_builty {
    qui sum `var', detail
    local j = 1
    foreach stat of local stats {
        mat M[`i', `j'] = r(`stat')
        local j = `j' + 1
    }
    local i = `i' + 1
}

* Output table
matrix rownames M = `vars' `vars_builty'
matrix colnames M = `stats'
putexcel set "`output'/tables/summary_table.xlsx", replace
putexcel A1 = matrix(M), names

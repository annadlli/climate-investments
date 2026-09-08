/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-02

Description: Summary statistics table for the NFIP property-year analysis set
    (analysis/analysis.dta from complete.do).
    One row per variable, one column per statistic. Add a variable to a list
    at the top to add a row. Dollar variables are in 2023 dollars, deflated
    upstream in the cleaners.

Revisions: change variable list to match the new ones from property x policy-year panel
- one row per property level collapse

******************************************************************************/

args data output

* Set table options
local vars policy_year_first policy_years sfha post_firm primary_residence ///
    premium_first premium_last ///
    any_claim claim_cb claim_cb_claimant claim_to_value claim_over_value ///
    claim_to_value_fl claim_over_value_fl ///
    rl srl ///
    attom_matched attom_value attom_value_2023 ///
    builty_elevated builty_retrofit builty_new_construction builty_elevation_year ///
    fma_n_properties fma_spend
local vars_builty builty_project_value
local stats N mean se

* -----------------------------------------------------------------------------

* Import data and collapse to one row per property
// 09-07: the panel is property x policy-year; keep each property's last  year (cumulative claims = property total) and carry first/last premium
use property_id policy_year sfha post_firm primary_residence premium ///
    cumulative_claims rl srl attom_matched attom_market_value_total attom_value_2023 ///
    builty_elevated builty_retrofit builty_new_construction builty_elevation_year ///
    fma_n_properties fma_spend using "`data'/analysis/analysis.dta", clear
bysort property_id (policy_year): gen policy_year_first = policy_year[1]
bysort property_id (policy_year): gen premium_first = premium[1]
bysort property_id (policy_year): gen policy_years = _N
bysort property_id (policy_year): keep if _n == _N
rename (cumulative_claims premium) (claim_cb premium_last)
count
local properties = r(N)

* Create derived variables
// Note: ATTOM logs zero where it holds no value, so zeros are treated as missing
gen any_claim = claim_cb > 0 & !mi(claim_cb)
gen claim_cb_claimant = claim_cb if any_claim
gen attom_value = attom_market_value_total if attom_market_value_total > 0
gen claim_to_value = claim_cb / attom_value if any_claim
gen claim_over_value = claim_to_value > 1 if !mi(claim_to_value)
//2026-09-07: ratio only where the ATTOM value is at least $10,000; a handful of near-zero values otherwise dominate the mean
gen claim_to_value_fl = claim_cb / attom_value if any_claim & attom_value >= 10000
gen claim_over_value_fl = claim_to_value_fl > 1 if !mi(claim_to_value_fl)

* Tabulate statistics
local nvars : word count `vars' `vars_builty'
local nstats : word count `stats'
mat M = J(`nvars', `nstats', .)
local i = 1
foreach var of local vars {
    qui sum `var', detail
    local j = 1
    foreach stat of local stats {
        // 09-07: summarize has no r(se), so add it by computation
        if "`stat'" == "se" mat M[`i', `j'] = r(sd) / sqrt(r(N))
        else mat M[`i', `j'] = r(`stat')
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
        if "`stat'" == "se" mat M[`i', `j'] = r(sd) / sqrt(r(N))
        else mat M[`i', `j'] = r(`stat')
        local j = `j' + 1
    }
    local i = `i' + 1
}

* Report the sample size once
// Claude change: rows on the full sample repeat the same N; blank those and put
// the number in a final row, so what is left is a subsample count
forvalues r = 1/`nvars' {
    if M[`r', 1] == `properties' mat M[`r', 1] = .
}
mat M = M \ (`properties', J(1, `nstats' - 1, .))

* Output table
matrix rownames M = `vars' `vars_builty' N_properties
matrix colnames M = `stats'
putexcel set "`output'/tables/summary_table.xlsx", replace
putexcel A1 = matrix(M), names

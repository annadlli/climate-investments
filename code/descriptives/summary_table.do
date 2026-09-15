/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-02
Edited: 2026-09-15
Description: Summary statistics table for the NFIP property-year analysis set
    One row per variable, one block of columns per group (All, Pre-FIRM,
    Post-FIRM). Add a variable to a list at the top to add a row; add a group
    indicator to add a column block. Dollar variables are in 2023 dollars,
    deflated upstream in the cleaners.

Revisions: change variable list to match the new ones from property x policy-year panel
- one row per property level collapse
09-12: Revised variable list: real attom value, add in builty project value, and the number of properties of hmgp and fma
09-15: keep the one-row-per-variable layout; add Pre-FIRM and Post-FIRM mean columns
******************************************************************************/

args data output

* Set table options
// Revised variable list
local vars policy_year_first policy_years sfha post_firm primary_residence ///
    premium_first premium_last ///
    any_claim claim_cb claim_cb_claimant claim_to_value claim_over_value ///
    rl srl ///
    attom_matched attom_value_2023 ///
    elevation elevated builty_elevated builty_retrofit builty_elevation_year builty_project_value ///
    hma_p_fma hma_p_hmgp fma_n_properties fma_spend
//09-15: N is reported for All only; the stats below are repeated per group
local stats mean
local groups all pre_firm post_firm

* -----------------------------------------------------------------------------

* Import data and collapse to one row per property
// 09-07: the panel is property x policy-year; keep each property's last  year (cumulative claims = property total) and carry first/last premium
use property_id policy_year sfha post_firm primary_residence premium ///
    cumulative_claims rl srl attom_matched attom_value_2023 ///
    elevation elevated builty_elevated builty_retrofit builty_elevation_year builty_project_value ///
    hma_p_fma hma_p_hmgp fma_n_properties fma_spend using "`data'/analysis/analysis.dta", clear
bysort property_id (policy_year): gen policy_year_first = policy_year[1]
bysort property_id (policy_year): gen premium_first = premium[1]
bysort property_id (policy_year): gen policy_years = _N
bysort property_id (policy_year): keep if _n == _N
rename (cumulative_claims premium) (claim_cb premium_last)
count
local properties = r(N)

* Create derived variables
// 09-12: attom value is cleaned upstream in wide format, so removed cleaning that was here 
gen any_claim = claim_cb > 0 & !mi(claim_cb)
gen claim_cb_claimant = claim_cb if any_claim
gen claim_to_value = claim_cb / attom_value_2023 if any_claim
gen claim_over_value = claim_to_value > 1 if !mi(claim_to_value)

* Group indicators
// 09-15: post_firm is constant within property, so the last year's value is the property's
gen byte all = 1
gen byte pre_firm = post_firm == 0

* Tabulate statistics
// 09-15: columns are N, then each stat for each group (mean_all mean_pre_firm mean_post_firm)
local nvars : word count `vars'
local nstats : word count `stats'
local ngroups : word count `groups'
local ncols = 1 + `nstats' * `ngroups'
mat M = J(`nvars', `ncols', .)
local colnames N
foreach g of local groups {
    foreach stat of local stats {
        local colnames `colnames' `stat'_`g'
    }
}
local i = 1
foreach var of local vars {
    qui sum `var', detail
    mat M[`i', 1] = r(N)
    local j = 2
    foreach g of local groups {
        qui sum `var' if `g' == 1, detail
        foreach stat of local stats {
            // 09-07: summarize has no r(se), so add it by computation
            if "`stat'" == "se" mat M[`i', `j'] = r(sd) / sqrt(r(N))
            else mat M[`i', `j'] = r(`stat')
            local j = `j' + 1
        }
    }
    local i = `i' + 1
}

* Report the sample size once, by group
// 09-15: blank N where it equals the full count; the bottom row carries the property count per group
forvalues r = 1/`nvars' {
    if M[`r', 1] == `properties' mat M[`r', 1] = .
}
mat R = J(1, `ncols', .)
mat R[1, 1] = `properties'
local j = 2
foreach g of local groups {
    qui count if `g' == 1
    mat R[1, `j'] = r(N)
    local j = `j' + `nstats'
}
mat M = M \ R

* Output table
matrix rownames M = `vars' N_properties
matrix colnames M = `colnames'
mat list M
putexcel set "`output'/tables/summary_table.xlsx", replace
putexcel A1 = matrix(M), names

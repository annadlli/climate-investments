/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-21
Revised: 2026-09-22

Description: Summary statistics table (deck and paper): one row per statistic, All /
    Pre-FIRM / Post-FIRM columns, on the analysis dataset (NFIP-insured single-family
    homes in SFHAs, FL LA TX, 2009-2025). Premium, loss ratio and the claim share are
    over property-years; the rest over properties. Runs from master.do (summary_table
    switch). 09-22: was descriptives/scratch/summary_table_slides.do; the earlier means
    table is archived as descriptives/archive/summary_table_means.do.

******************************************************************************/

args data output

* -----------------------------------------------------------------------------
* Section 1: Property-year statistics
* -----------------------------------------------------------------------------

use property_id policy_year post_firm premium claim cumulative_claims rl srl elevated ///
    elevation_retrofit elevation_year elevation_cost property_value ///
    using "`data'/analysis/analysis.dta", clear
gen any_claim = claim > 0 & !mi(claim)
gen all = 1
gen pre_firm = post_firm == 0
local groups all pre_firm post_firm

mat S = J(11, 3, .)
local j = 1
foreach g of local groups {
    // premium: median over property-years with a positive premium
    qui sum premium if `g' & premium > 0, detail
    mat S[1, `j'] = r(p50)
    // loss ratio: claims paid over premiums collected, both 2023 dollars
    qui sum claim if `g'
    local claims = r(sum)
    qui sum premium if `g'
    mat S[2, `j'] = `claims' / r(sum)
    qui sum any_claim if `g'
    mat S[3, `j'] = r(mean)
    local j = `j' + 1
}

* -----------------------------------------------------------------------------
* Section 2: Property-level statistics
* -----------------------------------------------------------------------------

bysort property_id (policy_year): gen elevated_ex_ante = elevated[1] == 1 & ///
    (mi(elevation_year) | policy_year[1] < elevation_year)
bysort property_id (policy_year): keep if _n == _N
gen claimant = cumulative_claims > 0 & !mi(cumulative_claims)

local j = 1
foreach g of local groups {
    // cumulative claims: medians among claimants, then among repetitive-loss and
    // severe-repetitive-loss claimants
    qui sum cumulative_claims if `g' & claimant, detail
    mat S[4, `j'] = r(p50)
    qui sum cumulative_claims if `g' & claimant & rl == 1, detail
    mat S[5, `j'] = r(p50)
    qui sum cumulative_claims if `g' & claimant & srl == 1, detail
    mat S[6, `j'] = r(p50)
    // ATTOM value: median among matched properties
    qui sum property_value if `g', detail
    mat S[7, `j'] = r(p50)
    // elevations: counts of properties; retrofit cost is the Builty declared value (FL, TX)
    qui count if `g' & elevated_ex_ante
    mat S[8, `j'] = r(N)
    qui count if `g' & elevation_retrofit == 1
    mat S[9, `j'] = r(N)
    qui sum elevation_cost if `g' & elevation_cost > 0, detail
    mat S[10, `j'] = r(p50)
    qui count if `g'
    mat S[11, `j'] = r(N)
    local j = `j' + 1
}

* -----------------------------------------------------------------------------
* Section 3: Output
* -----------------------------------------------------------------------------

mat rownames S = premium_median claims_over_premiums any_claim_share ///
    cum_claims_median_claimants cum_claims_median_rl cum_claims_median_srl ///
    property_value_median elevated_ex_ante elevation_retrofit retrofit_cost_median n_properties
mat colnames S = all pre_firm post_firm
mat list S, format(%12.3f)
putexcel set "`output'/tables/summary_table.xlsx", replace
putexcel A1 = "NFIP-insured single-family properties in SFHAs, FL LA TX, 2009-2025; 2023 dollars; premium, loss ratio and claim share over property-years, the rest over properties"
putexcel A2 = matrix(S), names

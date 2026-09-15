/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-02
Edited: 2026-09-15
Description: Summary statistics table for the NFIP property-year analysis set
    (analysis/analysis.dta from complete.do), laid out like Table 1 of
    Bodere, Brancaccio and Kang (2026): lettered panels of rows, one column per
    group (All, Pre-FIRM, Post-FIRM), mean and median as separate rows, sample
    counts at the bottom. Dollar variables are in 2023 dollars, deflated
    upstream in the cleaners.

Revisions: change variable list to match the new ones from property x policy-year panel
- one row per property level collapse
09-12: Revised variable list: real attom value, add in builty project value, and the numebr of properties of hmgp and fma
09-15 (Claude change): panel layout with All / Pre-FIRM / Post-FIRM columns for the
    draft's Table 1 and the deck. Panel A (premiums, loss ratio) is at the
    property-year grain; Panels B-C and the counts are one row per property.
******************************************************************************/

args data output

* Set table options
// Claude change 09-15: rows are fixed statistics (see the `row` calls below); columns are the groups
local groups all pre_firm post_firm

* -----------------------------------------------------------------------------

* Import data
// Claude change 09-15: keep the panel grain for Panel A, collapse to properties afterwards
use property_id policy_year post_firm premium claim cumulative_claims rl srl ///
    elevated elevation elevation_source elevation_funded ///
    builty_retrofit builty_project_value using "`data'/analysis/analysis.dta", clear

* Group indicators (post_firm is constant within property; take the property's last value below)
gen byte all = 1
gen byte pre_firm = post_firm == 0

* Row builder: `row name expr [if] , stat()` appends one row to matrix M
// Claude change 09-15
cap mat drop M
cap program drop row
program define row
    syntax anything(name=label) [if], stat(string) [scale(real 1)]
    tokenize `label'
    local rowname `1'
    local var `2'
    local vals
    foreach g in all pre_firm post_firm {
        if "`if'" == "" local cond "if `g' == 1"
        else local cond "`if' & `g' == 1"
        if "`stat'" == "mean" {
            qui sum `var' `cond'
            local v = r(mean) * `scale'
        }
        else if "`stat'" == "median" {
            qui sum `var' `cond', detail
            local v = r(p50) * `scale'
        }
        else if "`stat'" == "count" {
            qui count `cond'
            local v = r(N) * `scale'
        }
        local vals `vals' `v'
    }
    mat R = (`=subinstr(trim("`vals'"), " ", ",", .)')
    cap mat M = M \ R
    if _rc mat M = R
    global rownames "$rownames `rowname'"
end
global rownames

* Panel A: insurance, property-year grain
row premium_mean premium, stat(mean)
row premium_median premium, stat(median)
// loss ratio = claims paid / premiums collected over the panel, one number per group
foreach g of local groups {
    qui sum claim if `g' == 1
    local c_`g' = r(sum)
    qui sum premium if `g' == 1
    local p_`g' = r(sum)
}
mat M = M \ (`c_all' / `p_all', `c_pre_firm' / `p_pre_firm', `c_post_firm' / `p_post_firm')
global rownames "$rownames loss_ratio"
row n_property_years property_id, stat(count) scale(1e-6)

* Collapse to one row per property: last policy year (cumulative claims = property total),
* elevated flag at first observation, elevation flags as property maxima
bysort property_id (policy_year): gen byte elevated_first = elevated[1]
foreach v in elevation elevation_funded builty_retrofit {
    bysort property_id: egen byte `v'_max = max(`v')
    drop `v'
    rename `v'_max `v'
}
bysort property_id: egen byte elevation_nfip = max(inlist(elevation_source, 1, 3))
bysort property_id: egen byte elevation_builty = max(inlist(elevation_source, 2, 3))
bysort property_id: egen double builty_project_value_max = max(builty_project_value)
drop builty_project_value
rename builty_project_value_max builty_project_value
bysort property_id (policy_year): keep if _n == _N
rename cumulative_claims claim_cb

* Panel B: claims, property grain
gen byte any_claim = claim_cb > 0 & !mi(claim_cb)
row any_claim any_claim, stat(mean)
row rl rl, stat(mean)
row srl srl, stat(mean)
row claim_claimants_mean claim_cb if any_claim == 1, stat(mean) scale(1e-3)
row claim_claimants_median claim_cb if any_claim == 1, stat(median) scale(1e-3)
row claim_rl_median claim_cb if rl == 1 & any_claim == 1, stat(median) scale(1e-3)  // Claude change 09-15: RL status predates the panel, so condition on a panel claim
row claim_srl_median claim_cb if srl == 1 & any_claim == 1, stat(median) scale(1e-3)

* Panel C: elevations, property grain
row elevated_first elevated_first if elevated_first == 1, stat(count)
row elevation_any elevation if elevation == 1, stat(count)
row elevation_nfip elevation_nfip if elevation_nfip == 1, stat(count)
row elevation_builty elevation_builty if elevation_builty == 1, stat(count)
row grant_funded_share elevation_funded if builty_retrofit == 1, stat(mean)
row retrofit_cost_mean builty_project_value if builty_retrofit == 1, stat(mean) scale(1e-3)
row retrofit_cost_median builty_project_value if builty_retrofit == 1, stat(median) scale(1e-3)
row retrofit_cost_n builty_project_value if builty_retrofit == 1 & !mi(builty_project_value), stat(count)

* Sample size
row n_properties property_id, stat(count) scale(1e-6)

* Output table
matrix rownames M = $rownames
matrix colnames M = All Pre_FIRM Post_FIRM
mat list M
putexcel set "`output'/tables/summary_table.xlsx", replace
putexcel A1 = matrix(M), names

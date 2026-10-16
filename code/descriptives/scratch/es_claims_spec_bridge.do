/******************************************************************************
Authors: Anna Li
Date: 2026-09-22
Claude change 09-22: new scratch script, run by hand (Anna 09-22: "see if it replicates last week's results").
Description: replicates the 09-16 six-questions claims event study (pooled two-way fixed effects,
    county x year effects, SE clustered by home, all permit cohorts, 5% control draw) and walks
    one change at a time to the sample and specification of analysis/es_elevation.do, so the
    difference between last week's plot and this week's can be attributed.
Output: output/tables/es_claims_spec_bridge.xlsx, output/figures/es_claims_spec_bridge.{png,gph}
******************************************************************************/

args data output

local window = 5
local ref_period = -1
local blue "23 107 135"
local opts graphregion(color(white)) plotregion(color(white))

cap log close bridge
log using "`output'/logs/es_claims_spec_bridge.log", replace text name(bridge)

use state property_id policy_year flood_zone claim elevation_retrofit elevation_source ///
    elevation_year attom_matched censusblockgroupfips using "`data'/analysis/analysis.dta", clear
gen countycode = substr(censusblockgroupfips, 1, 5)
gen zone = substr(flood_zone, 1, 1)
drop censusblockgroupfips flood_zone
gen any_claim = claim > 0
gen permit = elevation_source == 3
gen permit_year = elevation_year if permit
egen county_id = group(countycode)
egen cell_cy  = group(countycode policy_year)
egen cell_czy = group(countycode zone policy_year)
tempfile panel
save `panel'

* event-time dummies, pooled across cohorts, reference t = -1
cap program drop make_ev
program define make_ev
    args window ref
    gen ev = policy_year - permit_year if permit
    replace ev = -`window' if ev < -`window'
    replace ev =  `window' if ev >  `window' & !mi(ev)
    forvalues k = -`window'/`window' {
        if `k' == `ref' continue
        local nm = cond(`k' < 0, "ev_m" + string(-`k'), "ev_p" + string(`k'))
        gen `nm' = ev == `k'
    }
end

* one spec: fill two columns of the results matrix from two regressions
cap program drop run_spec
program define run_spec
    args M col fe vce window ref
    local evvars
    forvalues k = -`window'/`window' {
        if `k' == `ref' continue
        local evvars `evvars' `=cond(`k' < 0, "ev_m" + string(-`k'), "ev_p" + string(`k'))'
    }
    local c = `col'
    foreach y in claim any_claim {
        reghdfe `y' `evvars', absorb(property_id `fe') vce(cluster `vce')
        forvalues k = -`window'/`window' {
            if `k' == `ref' continue
            local nm = cond(`k' < 0, "ev_m" + string(-`k'), "ev_p" + string(`k'))
            matrix `M'[`k' + `window' + 1, `c']     = _b[`nm']
            matrix `M'[`k' + `window' + 1, `c' + 1] = _se[`nm']
        }
        local c = `c' + 2
    }
    quietly count if permit & ev == 0
    di "treated homes at t0: " r(N)
end

matrix B = J(2 * `window' + 1, 16, 0)

* A. last week: all cohorts, all counties, never-elevated controls at 5% (seed 20260915),
*    county x year effects, SE clustered by home
use `panel', clear
set seed 20260915
gen u = runiform()
bysort property_id: replace u = u[1]
keep if permit | (elevation_retrofit == 0 & u < 0.05)
make_ev `window' `ref_period'
di _n "=== A. last week's specification ==="
run_spec B 1 cell_cy property_id `window' `ref_period'

* B. A with SE clustered by county
di _n "=== B. + cluster by county ==="
run_spec B 5 cell_cy county_id `window' `ref_period'

* C. B with county x zone x year effects
di _n "=== C. + county x zone x year effects ==="
run_spec B 9 cell_czy county_id `window' `ref_period'

* D. C on this week's sample: ATTOM-matched homes, cohorts 2012-2022, counties with a permit,
*    never-elevated controls at 10% (seed 20260917); still pooled two-way fixed effects
use `panel', clear
keep if attom_matched == 1
drop if elevation_retrofit == 1 & !permit
bysort countycode: egen county_has_permit = max(permit)
keep if county_has_permit
set seed 20260917
gen u = runiform()
bysort property_id (policy_year): replace u = u[1]
keep if permit | u < 0.10
keep if !permit | inrange(permit_year, 2012, 2022)
make_ev `window' `ref_period'
di _n "=== D. + this week's sample (pooled TWFE; the interaction-weighted version is es_elevation.do's permit panel) ==="
run_spec B 13 cell_czy county_id `window' `ref_period'

local rows
forvalues k = -`window'/`window' {
    local rows `rows' t`k'
}
matrix rownames B = `rows'
matrix colnames B = A_claim_b A_claim_se A_any_b A_any_se B_claim_b B_claim_se B_any_b B_any_se ///
    C_claim_b C_claim_se C_any_b C_any_se D_claim_b D_claim_se D_any_b D_any_se
matrix list B, format(%10.1f)
putexcel set "`output'/tables/es_claims_spec_bridge.xlsx", replace
putexcel A1 = "Claims event study, pooled two-way FE, ref t = -1. A: 09-16 spec (county x year FE, cluster home, all cohorts, 5% controls). B: cluster county. C: county x zone x year FE. D: this week's sample (ATTOM-matched, cohorts 2012-2022, counties with a permit, 10% controls)"
putexcel A2 = matrix(B), names

* figure: paid claim, four panels on one scale
preserve
    clear
    svmat B, names(col)
    gen t = _n - `window' - 1
    foreach s in A B C D {
        gen lo_`s' = `s'_claim_b - 1.96 * `s'_claim_se
        gen hi_`s' = `s'_claim_b + 1.96 * `s'_claim_se
        twoway (rcap lo_`s' hi_`s' t, lcolor("`blue'")) (scatter `s'_claim_b t, mcolor("`blue'") msymbol(O)), ///
            yline(0, lcolor(gs8)) xline(-0.5, lpattern(dash) lcolor(gs8)) `opts' legend(off) ///
            xtitle("Years from permit") ytitle("Effect on paid claim (2023 \$)") xlabel(-`window'(1)`window') ///
            subtitle("`s'", size(medium) color(gs4)) name(g_`s', replace)
    }
    graph combine g_A g_B g_C g_D, rows(1) ycommon xsize(16) ysize(4) graphregion(color(white))
    graph save   "`output'/figures/es_claims_spec_bridge.gph", replace
    graph export "`output'/figures/es_claims_spec_bridge.png", width(3200) replace
restore

log close bridge

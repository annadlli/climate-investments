/******************************************************************************
Authors: Anna Li
Date: 2026-09-22
Description: claims of Builty-permitted homes before and after the permit against never-elevated
    homes hit by the same flood, and the cost-effectiveness translation.
******************************************************************************/

args data output

* setup
local control_share = 0.10    // share of never-elevated homes drawn as controls
local flood_share   = 0.05    // flood county-year: this share of insured homes has a paid claim
local min_flood_claims = 50   // and at least this many homes claim
local discount      = 0.07    // FEMA BCA discount rate (OMB Circular A-94)
local horizon       = 30      // years

local blue "23 107 135"
local opts graphregion(color(white)) plotregion(color(white))

set seed 20260917

cap log close same_flood
log using "`output'/logs/same_flood_claims.log", replace text name(same_flood)


* =============================================================================
* 1 sample
* =============================================================================

use property_id policy_year flood_zone claim construction_year post_firm ///
    elevation_retrofit elevation_source elevation_year elevation_cost attom_matched censusblockgroupfips ///
    using "`data'/analysis/analysis.dta", clear
gen countycode = substr(censusblockgroupfips, 1, 5)
gen zone = substr(flood_zone, 1, 1)
gen any_claim = claim > 0
drop censusblockgroupfips flood_zone

* flood county-years, from every insured home on the panel
preserve
    collapse (mean) share_claim = any_claim (sum) n_claims = any_claim, by(countycode policy_year)
    gen flood_year = share_claim >= `flood_share' & n_claims >= `min_flood_claims'
    keep countycode policy_year flood_year
    tempfile floods
    save `floods'
restore

* a permit can only attach to an ATTOM-matched home, so unmatched homes cannot be controls
keep if attom_matched == 1

* treated: Builty-permitted homes, since the permit dates the work; homes whose only elevation
* signal is an NFIP flag flip (dated after the work) or an ICC payment (dated by the flood) are set aside
gen permit = elevation_source == 3
drop if elevation_retrofit == 1 & !permit

* counties with at least one permitted home, and a random draw of the never-elevated homes there
bysort countycode: egen county_has_permit = max(permit)
keep if county_has_permit
gen u = runiform()
bysort property_id (policy_year): replace u = u[1]
keep if permit | u < `control_share'
drop u county_has_permit

egen cell = group(countycode zone policy_year)
egen county_id = group(countycode)
egen home_tag = tag(property_id)
merge m:1 countycode policy_year using `floods', keep(1 3) nogen

quietly count if home_tag & permit
di "Permitted homes: " r(N)
quietly count if home_tag & !permit
di "Control homes: " r(N)

* inputs for the cost-effectiveness calculation: how often a permitted home's county has a flood
* year, and the median declared cost of the permitted homes
quietly summarize flood_year if permit
local p_flood = r(mean)
quietly summarize elevation_cost if permit & home_tag & elevation_cost > 0, detail
local cost_med = r(p50)


* =============================================================================
* 2 same flood, before and after the permit
* =============================================================================

* flood county-years only: permitted homes before and after the permit against never-elevated
* homes in the same county x zone x year, construction decade x FIRM status absorbed
keep if flood_year == 1
gen pre_permit  = permit & policy_year <= elevation_year
gen post_permit = permit & policy_year >  elevation_year
gen decade = floor(construction_year / 10) * 10
egen decade_firm = group(decade post_firm)
matrix F = J(2, 6, .)
local r = 1
foreach y in any_claim claim {
    reghdfe `y' pre_permit post_permit, absorb(cell decade_firm) vce(cluster county_id)
    matrix F[`r', 1] = _b[pre_permit]
    matrix F[`r', 2] = _se[pre_permit]
    matrix F[`r', 3] = _b[post_permit]
    matrix F[`r', 4] = _se[post_permit]
    lincom pre_permit - post_permit
    matrix F[`r', 5] = r(estimate)
    matrix F[`r', 6] = r(se)
    local r = `r' + 1
}
matrix rownames F = any_claim claim
matrix colnames F = pre_b pre_se post_b post_se pre_minus_post_b pre_minus_post_se
matrix list F
quietly count if pre_permit & e(sample)
di "Pre-permit home-years in flood years: " r(N)
quietly count if post_permit & e(sample)
di "Post-permit home-years in flood years: " r(N)

* raw means in flood years by group
preserve
    gen group = cond(!permit, "1 never elevated", cond(pre_permit, "2 permitted, before", "3 permitted, after"))
    collapse (mean) any_claim claim (count) home_years = policy_year, by(group)
    list, clean noobs
    tempfile means
    save `means'
restore


* =============================================================================
* 3 cost effectiveness
* =============================================================================

* damage avoided per flood year (pre minus post excess claim) times the flood-year frequency, as a
* present value over the horizon, against the median declared permit cost
local delta = 1 / (1 + `discount')
local annuity = `delta' * (1 - `delta'^`horizon') / (1 - `delta')
local save_year = el(F, 2, 5) * `p_flood'
matrix N = J(7, 1, .)
matrix N[1, 1] = el(F, 2, 5)
matrix N[2, 1] = `p_flood'
matrix N[3, 1] = `save_year'
matrix N[4, 1] = `save_year' * `annuity'
matrix N[5, 1] = `cost_med'
matrix N[6, 1] = `save_year' * `annuity' / `cost_med'
matrix N[7, 1] = `cost_med' / `save_year'
matrix rownames N = saving_per_flood_year p_flood_year expected_annual_saving npv permit_cost_median npv_over_cost payback_years
matrix colnames N = value
matrix list N


* =============================================================================
* 4 figure and table
* =============================================================================

preserve
    clear
    set obs 2
    gen t = _n
    gen b_any  = cond(t == 1, el(F, 1, 1), el(F, 1, 3))
    gen se_any = cond(t == 1, el(F, 1, 2), el(F, 1, 4))
    gen b_usd  = cond(t == 1, el(F, 2, 1), el(F, 2, 3)) / 1000
    gen se_usd = cond(t == 1, el(F, 2, 2), el(F, 2, 4)) / 1000
    foreach v in any usd {
        gen lo_`v' = b_`v' - 1.96 * se_`v'
        gen hi_`v' = b_`v' + 1.96 * se_`v'
    }
    twoway (rcap lo_any hi_any t, lcolor("`blue'")) (scatter b_any t, mcolor("`blue'") msymbol(O) msize(large)), ///
        yline(0, lcolor(gs8)) xlabel(1 "Before permit" 2 "After permit", noticks) xscale(range(0.5 2.5)) ///
        xtitle("") ytitle("Probability of a paid claim, vs never-elevated neighbors") ylabel(0(0.1)0.3) ///
        `opts' legend(off) name(g_any, replace)
    twoway (rcap lo_usd hi_usd t, lcolor("`blue'")) (scatter b_usd t, mcolor("`blue'") msymbol(O) msize(large)), ///
        yline(0, lcolor(gs8)) xlabel(1 "Before permit" 2 "After permit", noticks) xscale(range(0.5 2.5)) ///
        xtitle("") ytitle("Paid claim (thousands of 2023 \$), vs never-elevated neighbors") ///
        `opts' legend(off) name(g_usd, replace)
    graph combine g_any g_usd, rows(1) graphregion(color(white))
    graph save   "`output'/figures/same_flood_claims.gph", replace
    graph export "`output'/figures/same_flood_claims.png", width(2400) replace
restore

putexcel set "`output'/tables/same_flood_claims.xlsx", sheet(same_flood) replace
putexcel A1 = "Flood county-years only (>= `=100 * `flood_share''% of insured homes and >= `min_flood_claims' homes claim): Builty-permitted homes before and after the permit vs never-elevated homes in the same county x zone x year; construction decade x FIRM status absorbed; SE clustered by county"
putexcel A2 = matrix(F), names
putexcel set "`output'/tables/same_flood_claims.xlsx", sheet(npv) modify
putexcel A1 = "Saving per flood year x share of permitted homes' years that are flood years, as a `horizon'-year present value at `=100 * `discount''%, against the median declared permit cost (2023 $)"
putexcel A2 = matrix(N), names
preserve
    use `means', clear
    export excel using "`output'/tables/same_flood_claims.xlsx", sheet("raw_means", replace) firstrow(variables)
restore

log close same_flood

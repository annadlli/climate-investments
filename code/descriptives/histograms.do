/******************************************************************************
Authors: Anna Li
Date: 2026-09-02

Description: Histograms of cumulative NFIP claims and Builty elevation project
    cost. Both are long-tailed -- claims run past $1m, elevation cost past
    $10m -- so a raw axis is one spike at the left. The x-axis is logged with
    ticks labelled in dollars. Dashed line is the median.

    Dollars are 2023 dollars, deflated in the cleaners.

******************************************************************************/

args data output

* Set figure options
local dollars `=ln(1000)' "1k" `=ln(10000)' "10k" `=ln(100000)' "100k" ///
    `=ln(1000000)' "1m" `=ln(10000000)' "10m"
local opts percent width(0.25) fcolor("51 122 183") lcolor("51 122 183") ///
    ytitle("Percent of properties") graphregion(color(white)) ///
    plotregion(color(white)) legend(off)

* -----------------------------------------------------------------------------

* Cumulative claims, among properties that ever claimed
//09-06: analysis_no_diagnostics.dta no longer exists, so keep each property's last policy year, where cumulative_claims equals the old property-level claim_cb. 
use property_id policy_year cumulative_claims using "`data'/analysis/analysis.dta", clear
bysort property_id (policy_year): keep if _n == _N
rename cumulative_claims claim_cb
keep if claim_cb > 0 & !mi(claim_cb)
gen ln_claim = ln(claim_cb)

qui sum claim_cb, detail
local med = r(p50)
histogram ln_claim, `opts' ///
    xtitle("Cumulative net claims, 2023 dollars") ///
    xlabel(`dollars') ///
    xline(`=ln(`med')', lcolor(cranberry) lpattern(dash))
graph save  "`output'/figures/claims_histogram.gph", replace
graph export "`output'/figures/claims_histogram.png", width(2000) replace

* Elevation project cost, among properties reporting one
// Note: project_value is unreported rather than zero where Builty has no cost
use "`data'/clean/builty_elevations.dta", clear
keep if project_value > 0 & !mi(project_value)
gen ln_cost = ln(project_value)

qui sum project_value, detail
local med = r(p50)
histogram ln_cost, `opts' ///
    xtitle("Elevation project cost, 2023 dollars") ///
    xlabel(`dollars') ///
    xline(`=ln(`med')', lcolor(cranberry) lpattern(dash))
graph save  "`output'/figures/elevation_cost_histogram.gph", replace
graph export "`output'/figures/elevation_cost_histogram.png", width(2000) replace

/******************************************************************************
Authors: Anna Li
Date: 2026-09-02

Description: Histograms of cumulative NFIP claims and declared elevation cost, both
    from the analysis panel. Both are long-tailed -- claims run past $1m, elevation cost past
    $10m -- so a raw axis is one spike at the left. The x-axis is logged with
    ticks labelled in dollars. Dashed line is the median.

    Dollars are 2023 dollars, deflated in the cleaners.

******************************************************************************/

args data output

* Set figure options
// Claude change 09-15 (Vendela's slide notes): both panels from analysis.dta, one x-axis
// so the overlap of the two distributions is visible, larger labels, no source name
local xmin = ln(1000)
local xmax = ln(10000000)
local dollars `=ln(1000)' "1k" `=ln(10000)' "10k" `=ln(100000)' "100k" ///
    `=ln(1000000)' "1m" `=ln(10000000)' "10m"
local opts percent width(0.25) fcolor("51 122 183") lcolor("51 122 183") ///
    ytitle("Percent of properties", size(medlarge)) ///
    xscale(range(`xmin' `xmax')) xlabel(`dollars', labsize(medlarge)) ///
    ylabel(, labsize(medlarge)) graphregion(color(white)) ///
    plotregion(color(white)) legend(off)

* -----------------------------------------------------------------------------

* One row per property from the panel: total claims and the declared elevation cost
// Note: cumulative_claims at the last policy year is the property total;
// builty_project_value is constant within property, retrofits only, cleaned in
// clean_builty.do ($1k-$1M, 2023 $)
use property_id policy_year cumulative_claims builty_project_value ///
    using "`data'/analysis/analysis.dta", clear
bysort property_id (policy_year): keep if _n == _N

* Cumulative claims, among properties that ever claimed
preserve
    // Claude change 09-15: the same $1k floor as the cost panel, so both axes start at 1k
    keep if cumulative_claims >= 1000 & !mi(cumulative_claims)
    gen ln_claim = ln(cumulative_claims)
    qui sum cumulative_claims, detail
    local med = r(p50)
    local n = r(N)
    histogram ln_claim, `opts' ///
        xtitle("Cumulative NFIP claims paid, 2023 dollars", size(medlarge)) ///
        xline(`=ln(`med')', lcolor(cranberry) lpattern(dash))
    graph save  "`output'/figures/claims_histogram.gph", replace
    graph export "`output'/figures/claims_histogram.png", width(2000) replace
restore

* Elevation cost, among retrofit homes reporting one
// Claude change 09-15, stopgap: the $1k-$1M rule also sits in complete.do (build step) and
// reaches the panel at its next run; applied here meanwhile so the figure is right for the
// 09-16 meeting. Remove this line once complete.do has rerun.
replace builty_project_value = . if builty_project_value < 1000 | builty_project_value > 1000000
keep if builty_project_value > 0 & !mi(builty_project_value)
gen ln_cost = ln(builty_project_value)
qui sum builty_project_value, detail
local med = r(p50)
local n = r(N)
histogram ln_cost, `opts' ///
    xtitle("Declared elevation cost, 2023 dollars", size(medlarge)) ///
    xline(`=ln(`med')', lcolor(cranberry) lpattern(dash))
graph save  "`output'/figures/elevation_cost_histogram.gph", replace
graph export "`output'/figures/elevation_cost_histogram.png", width(2000) replace

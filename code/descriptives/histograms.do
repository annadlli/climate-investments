/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-15

Description: Histograms of cumulative NFIP claims and elevation costs. 

******************************************************************************/

args data output

* -----------------------------------------------------------------------------
* Section 1: Set figure options
* -----------------------------------------------------------------------------

* Shared log dollar axis from a $1k display floor: below it are fees, document lines and
* property-count errors, not costs or losses. No ceiling: the largest cumulative claim on the
* panel is $1.3m once payouts are capped at coverage upstream.
local dollars `=ln(1000)' "1k" `=ln(10000)' "10k" `=ln(100000)' "100k" `=ln(1000000)' "1m" `=ln(10000000)' "10m"
local bars percent width(0.5)
local xaxis xscale(range(`=ln(1000)' `=ln(10000000)')) xlabel(`dollars', labsize(medlarge) nogrid) ///
    graphregion(color(white)) plotregion(color(white))
local axis `xaxis' yscale(range(0 25)) ylabel(0(5)25, labsize(medlarge) nogrid) ///
    ytitle("Percent of properties", size(medlarge))
local blue "51 122 183"

* -----------------------------------------------------------------------------
* Section 2: Prepare data
* -----------------------------------------------------------------------------

* Panel: one row per property, cumulative claims at the last policy year is the property total
use property_id policy_year cumulative_claims elevation_cost elevation_year ///
    using "`data'/analysis/analysis.dta", clear
// claims paid up to and including the elevation year, for the claims-vs-cost comparison
bysort property_id (policy_year): egen claims_pre = max(cond(policy_year <= elevation_year, cumulative_claims, .))
by property_id: keep if _n == _N
gen ln_claim = ln(cumulative_claims) if cumulative_claims >= 1000
gen ln_cost = ln(elevation_cost) if elevation_cost >= 1000
gen ln_claim_pre = ln(claims_pre) if claims_pre >= 1000

* -----------------------------------------------------------------------------
* Section 3: Plot histograms
* -----------------------------------------------------------------------------

* Cumulative claims, among properties that ever claimed
qui sum cumulative_claims if cumulative_claims >= 1000, detail
local n = string(r(N), "%12.0fc")
local med = string(r(p50), "%12.0fc")
histogram ln_claim, `bars' fcolor("`blue'") lcolor("`blue'") `axis' legend(off) ///
    xtitle("Cumulative NFIP claims paid (2023 \$)", size(medlarge)) ///
    xline(`=ln(r(p50))', lcolor(cranberry) lpattern(dash))
//    note("N = `n' properties with a claim; median \$`med' (dashed line)")
graph export "`output'/figures/claims_histogram.png", width(2000) replace

* Elevation cost, among Builty-elevated homes reporting one
qui sum elevation_cost if elevation_cost >= 1000, detail
local n = string(r(N), "%12.0fc")
local med = string(r(p50), "%12.0fc")
histogram ln_cost, `bars' fcolor("`blue'") lcolor("`blue'") `axis' legend(off) ///
    xtitle("Elevation cost (2023 \$)", size(medlarge)) ///
    xline(`=ln(r(p50))', lcolor(cranberry) lpattern(dash))
    // note("N = `n' Builty-elevated homes with a declared value (FL and TX permits); median \$`med' (dashed line)")
graph export "`output'/figures/elevation_cost_histogram.png", width(2000) replace

* Elevation cost against the claims paid before it, one point per Builty-elevated home
qui count if !mi(ln_cost) & !mi(elevation_year)
local n_cost = string(r(N), "%12.0fc")
qui count if !mi(ln_cost) & !mi(elevation_year) & claims_pre < 1000
local n_noclaim = string(r(N), "%12.0fc")
qui count if !mi(ln_cost) & !mi(ln_claim_pre)
local n = string(r(N), "%12.0fc")
qui count if !mi(ln_cost) & !mi(ln_claim_pre) & ln_cost > ln_claim_pre
local n_above = string(r(N), "%12.0fc")
twoway (function y = x, range(`=ln(1000)' `=ln(10000000)') lcolor(gs8) lpattern(dash)) ///
       (scatter ln_cost ln_claim_pre, msize(small) mcolor("`blue'%50")), ///
    xscale(range(`=ln(1000)' `=ln(10000000)')) xlabel(`dollars', labsize(medlarge) nogrid) ///
    yscale(range(`=ln(1000)' `=ln(10000000)')) ylabel(`dollars', labsize(medlarge) nogrid) ///
    aspectratio(1) graphregion(color(white)) plotregion(color(white)) ///
    xtitle("Cumulative NFIP claims paid before elevation (2023 \$)", size(medlarge)) ///
    ytitle("Elevation cost (2023 \$)", size(medlarge)) legend(off)
    // note("N = `n' Builty-elevated homes with a declared cost and >= \$1k of prior claims, `n_above' above the 45-degree line;" ///
    //      "a further `n_noclaim' of `n_cost' elevated homes had no prior claim and are not shown")
graph export "`output'/figures/claims_vs_elevation_cost.png", width(2000) replace

/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-08

Description: Fact: prices do not reward mitigation. On the NFIP property-year
    panel: (1) cross-sectional elevation discount in the SFHA before vs after
    RR2.0; (2) Sun-Abraham event study around the Builty retrofit permit;
    (3) event study around the NFIP elevated flag flipping 0 -> 1;
    (4) descriptives for the slide.

    Speed: the event studies use every retrofit / flag-flip property plus a
    random `control_share' of the never-treated properties as controls (drawn
    at the property level, seeded). Sections 1 and 4 use the full panel.

Output: output/logs/es_prices_mitigation.log, output/tables/discount.csv,
    output/figures/es_retrofit.pdf, output/figures/es_flip.pdf
Needs : reghdfe, ftools, eventstudyinteract, avar, coefplot, estout, distinct

******************************************************************************/

version 18
clear all
set more off
set seed 20260903

args data output

local control_share = 0.05   // share of never-treated properties kept as event-study controls

capture mkdir "`output'/logs"
cap log close
log using "`output'/logs/es_prices_mitigation.log", replace text

* eventstudyinteract leaves the interacted reghdfe in e(b); post the IW
* estimates so test / coefplot can use them
program define post_iw, eclass
    tempname b V
    matrix `b' = e(b_iw)
    matrix `V' = e(V_iw)
    ereturn post `b' `V'
end

use property_id policy_year state premium coverage_building elevated sfha ///
    risk_rating_2 post_firm flood_zone claim builty_retrofit ///
    builty_elevation_year builty_covered_strict ///
    using "`data'/analysis/analysis.dta", clear

*------------------------------------------------------------------------------
* 0. Sample and variables
*------------------------------------------------------------------------------
keep if premium > 0 & coverage_building > 0
gen lrate = ln(premium / coverage_building)
gen lprem = ln(premium)
egen cell = group(state flood_zone policy_year post_firm)
xtset property_id policy_year

di _n "Sample: " _N " property-years, " 
qui distinct property_id
di r(ndistinct) " properties"

* event time around the Builty retrofit permit (used in 2 and 4)
gen ev = policy_year - builty_elevation_year if builty_retrofit == 1
replace ev = -5 if ev < -5 & ev != .
replace ev =  5 if ev >  5 & ev != .

* first NFIP flag flip 0 -> 1 (used in 3)
bysort property_id (policy_year): gen flip = (elevated == 1 & elevated[_n-1] == 0) if _n > 1
bysort property_id: egen flip_year = min(cond(flip == 1, policy_year, .))

*------------------------------------------------------------------------------
* 1. Cross-sectional elevation discount, before vs after RR2.0
*    (SFHA only; cell FE = state x zone x year x FIRM status)
*------------------------------------------------------------------------------
di _n "==== 1. Elevation discount, SFHA ===="
eststo clear
foreach pf in 0 1 {
    foreach rr in 0 1 {
        eststo d_pf`pf'_rr`rr': ///
            reghdfe lrate elevated if sfha == 1 & post_firm == `pf' & risk_rating_2 == `rr', ///
            absorb(cell) vce(cluster property_id)
        estadd local firm = cond(`pf' == 1, "Post-FIRM", "Pre-FIRM")
        estadd local rr2  = cond(`rr' == 1, "RR2.0", "Pre-RR2.0")
    }
}
esttab d_*, keep(elevated) b(3) se(3) stats(firm rr2 N, fmt(%s %s %12.0fc)) ///
    title("Log premium rate on NFIP elevated flag, SFHA") nonotes
esttab d_* using "`output'/tables/discount.csv", keep(elevated) b(3) se(3) stats(firm rr2 N) replace

* pooled with interaction, for a single headline number
reghdfe lrate c.elevated##c.risk_rating_2 if sfha == 1, absorb(cell) vce(cluster property_id)

*------------------------------------------------------------------------------
* Event-study sample: all treated properties + a random draw of the rest
*------------------------------------------------------------------------------
preserve
gen treated = (builty_retrofit == 1 | flip_year != .)
gen u = runiform()
bysort property_id: replace u = u[1]
keep if treated == 1 | u < `control_share'
drop u
di _n "Event-study sample: " _N " property-years (control share `control_share')"
qui distinct property_id
di r(ndistinct) " properties"

*------------------------------------------------------------------------------
* 2. Event study around the Builty retrofit permit
*    Controls = never-retrofitted properties; Sun-Abraham; ref period t = -1
*------------------------------------------------------------------------------
di _n "==== 2. Retrofit event study ===="
forvalues k = 5(-1)2 {
    gen ev_m`k' = (ev == -`k')
}
forvalues k = 0/5 {
    gen ev_p`k' = (ev == `k')
}
gen never = (builty_retrofit != 1)
gen cohort = builty_elevation_year if builty_retrofit == 1

qui count if builty_retrofit == 1 & ev == 0
di "Retrofit properties at t=0: " r(N)

local evvars ev_m5 ev_m4 ev_m3 ev_m2 ev_p0 ev_p1 ev_p2 ev_p3 ev_p4 ev_p5

* 2a. log premium rate (headline)
eventstudyinteract lrate `evvars', cohort(cohort) control_cohort(never) ///
    absorb(property_id cell) vce(cluster property_id)
matrix b_rate = e(b_iw)
matrix V_rate = e(V_iw)
post_iw
test ev_m5 ev_m4 ev_m3 ev_m2          // pre-trend
test ev_p1 ev_p2 ev_p3 ev_p4 ev_p5    // post joint

* 2b. NFIP elevated flag (does the insurer ever learn?)
eventstudyinteract elevated `evvars', cohort(cohort) control_cohort(never) ///
    absorb(property_id cell) vce(cluster property_id)
matrix b_flag = e(b_iw)
matrix V_flag = e(V_iw)

* 2c. log premium in levels (in case coverage changes)
eventstudyinteract lprem `evvars', cohort(cohort) control_cohort(never) ///
    absorb(property_id cell) vce(cluster property_id)

* 2d. heterogeneity
foreach pf in 0 1 {
    di _n "--- retrofit ES, post_firm = `pf' ---"
    eventstudyinteract lrate `evvars' if post_firm == `pf', cohort(cohort) ///
        control_cohort(never) absorb(property_id cell) vce(cluster property_id)
}
foreach s in 0 1 {
    di _n "--- retrofit ES, sfha = `s' ---"
    eventstudyinteract lrate `evvars' if sfha == `s', cohort(cohort) ///
        control_cohort(never) absorb(property_id cell) vce(cluster property_id)
}

* 2e. drop retrofits with a claim in [-1, 0] (substantial-damage rebuilds)
bysort property_id: egen claim_at_event = max(cond(inrange(ev, -1, 0) & claim > 0, 1, 0))
di _n "--- retrofit ES, excluding retrofits with a claim at t in [-1,0] ---"
eventstudyinteract lrate `evvars' if claim_at_event == 0, cohort(cohort) ///
    control_cohort(never) absorb(property_id cell) vce(cluster property_id)

* 2f. Builty-covered counties only
di _n "--- retrofit ES, builty_covered_strict == 1 ---"
eventstudyinteract lrate `evvars' if builty_covered_strict == 1, cohort(cohort) ///
    control_cohort(never) absorb(property_id cell) vce(cluster property_id)

* figure: rate and flag on one plot
coefplot (matrix(b_rate), v(V_rate) label("Log premium rate")) ///
         (matrix(b_flag), v(V_flag) label("NFIP elevated flag")), ///
    vertical yline(0) xline(4.5, lpattern(dash)) ///
    coeflabels(ev_m5 = "-5" ev_m4 = "-4" ev_m3 = "-3" ev_m2 = "-2" ev_p0 = "0" ///
               ev_p1 = "+1" ev_p2 = "+2" ev_p3 = "+3" ev_p4 = "+4" ev_p5 = "+5") ///
    xtitle("Years relative to retrofit elevation permit") ytitle("Effect (ref. t = -1)") ///
    ciopts(recast(rcap)) legend(pos(6) rows(1)) graphregion(color(white))
graph export "`output'/figures/es_retrofit.pdf", replace

*------------------------------------------------------------------------------
* 3. Event study around the NFIP elevated flag flipping 0 -> 1
*    (what a recorded elevation is worth)
*------------------------------------------------------------------------------
di _n "==== 3. Flag-flip event study ===="
gen ev2 = policy_year - flip_year
replace ev2 = -5 if ev2 < -5 & ev2 != .
replace ev2 =  5 if ev2 >  5 & ev2 != .

forvalues k = 5(-1)2 {
    gen f_m`k' = (ev2 == -`k')
}
forvalues k = 0/5 {
    gen f_p`k' = (ev2 == `k')
}
gen never2 = (flip_year == .)
* RR2.0 status at the flip
bysort property_id: egen rr2_at_flip = max(cond(policy_year == flip_year, risk_rating_2, .))

qui count if ev2 == 0
di "Flag-flip properties at t=0: " r(N)

local fvars f_m5 f_m4 f_m3 f_m2 f_p0 f_p1 f_p2 f_p3 f_p4 f_p5

foreach rr in 0 1 {
    di _n "--- flag-flip ES, SFHA, RR2.0 at flip = `rr' ---"
    eventstudyinteract lrate `fvars' if sfha == 1 & (rr2_at_flip == `rr' | never2), ///
        cohort(flip_year) control_cohort(never2) ///
        absorb(property_id cell) vce(cluster property_id)
    matrix b_flip`rr' = e(b_iw)
    matrix V_flip`rr' = e(V_iw)
}

coefplot (matrix(b_flip0), v(V_flip0) label("Pre-RR2.0")) ///
         (matrix(b_flip1), v(V_flip1) label("RR2.0")), ///
    vertical yline(0) xline(4.5, lpattern(dash)) ///
    coeflabels(f_m5 = "-5" f_m4 = "-4" f_m3 = "-3" f_m2 = "-2" f_p0 = "0" ///
               f_p1 = "+1" f_p2 = "+2" f_p3 = "+3" f_p4 = "+4" f_p5 = "+5") ///
    xtitle("Years relative to NFIP recording elevation") ytitle("Log premium rate (ref. t = -1)") ///
    ciopts(recast(rcap)) legend(pos(6) rows(1)) graphregion(color(white))
graph export "`output'/figures/es_flip.pdf", replace

restore

*------------------------------------------------------------------------------
* 4. Descriptives to quote on the slide (full panel)
*------------------------------------------------------------------------------
di _n "==== 4. Descriptives ===="
* share of retrofits whose flag ever turns on afterwards
preserve
    keep if builty_retrofit == 1
    bysort property_id: egen pre_flag  = max(cond(ev < 0, elevated, .))
    bysort property_id: egen post_flag = max(cond(ev >= 1, elevated, .))
    bysort property_id: keep if _n == 1
    keep if pre_flag != . & post_flag != .
    di "Retrofits with pre & post obs: " _N
    count if pre_flag == 0 & post_flag == 1
    di "  flag 0 -> 1: " r(N)
    count if pre_flag == 1
    di "  flag already 1: " r(N)
    count if pre_flag == 0 & post_flag == 0
    di "  never flagged: " r(N)
restore

* median premium by event time, retrofits
tabstat premium if builty_retrofit == 1, by(ev) stat(median n)

* median rate by elevated flag, SFHA, pre-FIRM, before/after RR2.0
gen rate100 = premium / coverage_building * 100
tabstat rate100 if sfha == 1 & post_firm == 0 & risk_rating_2 == 0, by(elevated) stat(median n)
tabstat rate100 if sfha == 1 & post_firm == 0 & risk_rating_2 == 1, by(elevated) stat(median n)

log close

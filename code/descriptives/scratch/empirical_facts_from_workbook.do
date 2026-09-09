/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-03

Description: Creates figures and a main table for the empirical-facts section
    from output/tables/empirical_facts.xlsx. The exhibits are descriptive and
    do not interpret raw premium differences as causal effects of elevation.

******************************************************************************/

version 18
clear all
set more off

args output

local source "`output'/tables/empirical_facts.xlsx"
capture mkdir "`output'/figures/empirical_facts"

* -----------------------------------------------------------------------------
* Figure 1 and main table: mitigation remains rare in high-loss groups
* -----------------------------------------------------------------------------

import excel using "`source'", sheet("risk_group") firstrow clear case(lower)

gen lo = elevations_per_1000 - ci95_halfwidth
gen hi = elevations_per_1000 + ci95_halfwidth
gen group_order = _n

label define risk_group_label ///
    1 "All linked properties" ///
    2 "No paid claims" ///
    3 "Any paid claim" ///
    4 "At least USD 50,000 paid" ///
    5 "At least USD 100,000 paid" ///
    6 "Repetitive loss" ///
    7 "Severe repetitive loss"
label values group_order risk_group_label

twoway ///
    (bar elevations_per_1000 group_order, horizontal barwidth(.68) color("31 111 139")) ///
    (rcap lo hi group_order, horizontal lcolor("36 55 70")), ///
    title("Home elevation is rare, even among high-loss properties", size(medsmall)) ///
    subtitle("Builty permits linked to NFIP properties through assigned ATTOM records", size(vsmall)) ///
    xtitle("Recorded elevation permits per 1,000 linked properties", size(small)) ///
    ytitle("") ///
    ylabel(1(1)7, valuelabel angle(horizontal) labsize(small)) ///
    yscale(reverse) ///
    xlabel(0(1)7, labsize(small) grid glcolor("229 231 235")) ///
    legend(off) ///
    graphregion(color(white)) plotregion(color(white))

graph export "`output'/figures/empirical_facts/figure_1_mitigation_is_rare.pdf", replace
graph export "`output'/figures/empirical_facts/figure_1_mitigation_is_rare.png", width(2400) replace

gen elevation_percent = elevations_per_1000 / 10
format properties_with_builty_measure properties_with_elevation_permit %12.0fc
format elevations_per_1000 %9.2f
format elevation_percent %9.3f
export excel property_group properties_with_builty_measure ///
    properties_with_elevation_permit elevations_per_1000 elevation_percent ///
    using "`output'/tables/empirical_facts_main_table.xlsx", ///
    firstrow(variables) replace

quietly summarize elevations_per_1000 if group_order == 1, meanonly
local all_rate = r(mean)
quietly summarize elevations_per_1000 if group_order == 7, meanonly
local srl_rate = r(mean)
display as result "All linked properties: " %5.2f `all_rate' " elevations per 1,000"
display as result "Severe repetitive loss: " %5.2f `srl_rate' " elevations per 1,000"

* -----------------------------------------------------------------------------
* Figure 2: raw realized-loss gradient
* -----------------------------------------------------------------------------

import excel using "`source'", sheet("realized_loss") firstrow clear case(lower)

twoway ///
    (rarea lo hi risk_group, color("142 202 230%35") lcolor(none)) ///
    (connected elevations_per_1000 risk_group, color("31 111 139") ///
        mcolor("31 111 139") msymbol(O) msize(small)), ///
    title("Elevation rises with realized losses—but remains extremely rare", size(medsmall)) ///
    subtitle("Positive-loss block groups ranked from low to high; 95% confidence intervals", size(vsmall)) ///
    xtitle("Realized-loss group", size(small)) ///
    ytitle("Recorded elevation permits per 1,000 linked properties", size(small)) ///
    xlabel(0 "No losses" 1(1)10, labsize(small)) ///
    ylabel(0(.5)2.5, labsize(small) grid glcolor("229 231 235")) ///
    legend(off) ///
    graphregion(color(white)) plotregion(color(white))

graph export "`output'/figures/empirical_facts/figure_2_realized_loss_gradient.pdf", replace
graph export "`output'/figures/empirical_facts/figure_2_realized_loss_gradient.png", width(2400) replace

quietly correlate risk_group elevations_per_1000 if inrange(risk_group, 1, 10)
matrix correlation = r(C)
local risk_correlation = correlation[1,2]
local correlation_n = r(N)
local correlation_t = `risk_correlation' * ///
    sqrt((`correlation_n' - 2) / (1 - `risk_correlation'^2))
local correlation_p = 2 * ttail(`correlation_n' - 2, abs(`correlation_t'))

preserve
    clear
    set obs 1
    gen pearson_correlation = `risk_correlation'
    gen p_value = `correlation_p'
    gen observations = `correlation_n'
    gen definition = "Positive-loss realized-risk decile versus elevations per 1,000"
    format pearson_correlation p_value %9.4f
    export excel using "`output'/tables/empirical_facts_correlation.xlsx", ///
        firstrow(variables) replace
restore

quietly summarize elevations_per_1000 if risk_group == 1, meanonly
local decile_1 = r(mean)
quietly summarize elevations_per_1000 if risk_group == 10, meanonly
local decile_10 = r(mean)
display as result "Risk-decile correlation: " %5.3f `risk_correlation'
display as result "Two-sided p-value: " %6.4f `correlation_p' ///
    "; observations: " %2.0f `correlation_n'
display as result "Decile 1: " %5.2f `decile_1' "; decile 10: " %5.2f `decile_10' " per 1,000"

* -----------------------------------------------------------------------------
* Figure 2A: low versus high risk using the NFIP rated flood zone
* -----------------------------------------------------------------------------

import excel using "`source'", sheet("flood_zone_risk") firstrow clear case(lower)

gen lo = elevations_per_1000 - ci95_halfwidth
gen hi = elevations_per_1000 + ci95_halfwidth
gen group_order = high_flood_risk + 1

label define low_high_label 1 "Low risk: non-SFHA" 2 "High risk: SFHA (A/V zones)"
label values group_order low_high_label

twoway ///
    (bar elevations_per_1000 group_order, barwidth(.62) color("31 111 139")) ///
    (rcap lo hi group_order, lcolor("36 55 70")), ///
    title("Elevation by NFIP rated flood-risk zone", size(medsmall)) ///
    subtitle("High risk is SFHA: rated A/V zones; 95% confidence intervals", size(vsmall)) ///
    xtitle("") ///
    ytitle("Recorded elevation permits per 1,000 linked properties", size(small)) ///
    xlabel(1 "Low risk: non-SFHA" 2 "High risk: SFHA (A/V)", labsize(small)) ///
    ylabel(, labsize(small) grid glcolor("229 231 235")) ///
    legend(off) ///
    graphregion(color(white)) plotregion(color(white))

graph export "`output'/figures/empirical_facts/figure_2a_low_vs_high_flood_zone.pdf", replace
graph export "`output'/figures/empirical_facts/figure_2a_low_vs_high_flood_zone.png", width(2400) replace

format properties elevations %12.0fc
format elevations_per_1000 %9.3f
format elevation_share_percent %9.3f
export excel high_flood_risk properties elevations elevations_per_1000 ///
    elevation_share_percent ci95_halfwidth ///
    using "`output'/tables/empirical_facts_flood_zone_low_vs_high.xlsx", ///
    firstrow(variables) replace

quietly summarize elevations_per_1000 if high_flood_risk == 0, meanonly
local low_rate = r(mean)
quietly summarize elevations_per_1000 if high_flood_risk == 1, meanonly
local high_rate = r(mean)
display as result "Low-risk elevation rate: " %5.3f `low_rate' " per 1,000"
display as result "High-risk elevation rate: " %5.3f `high_rate' " per 1,000"
display as result "High/low rate ratio: " %5.2f (`high_rate' / `low_rate')

import excel using "`source'", sheet("flood_zone_correlation") firstrow clear case(lower)
display as result "SFHA-elevation correlation: " %7.5f pearson_correlation[1] ///
    "; p-value: " %7.5f p_value[1] "; N = " %12.0fc observations[1]

* -----------------------------------------------------------------------------
* Figure 3: reported project-cost distribution summarized by quartiles
* -----------------------------------------------------------------------------

import excel using "`source'", sheet("elevation_costs") firstrow clear case(lower)

gen row = 1
replace p25 = p25 / 1000
replace median = median / 1000
replace p75 = p75 / 1000

twoway ///
    (rcap p25 p75 row, horizontal lcolor("31 111 139") lwidth(thick)) ///
    (scatter row median, mcolor("224 122 95") msymbol(O) msize(large)), ///
    title("Elevation requires a large up-front investment", size(medsmall)) ///
    subtitle("Median and interquartile range of reported Builty project values", size(vsmall)) ///
    xtitle("Reported project value (2023 $1,000s)", size(small)) ///
    ytitle("") ylabel(none) ///
    xlabel(0(25)175, labsize(small) grid glcolor("229 231 235")) ///
    legend(off) ///
    graphregion(color(white)) plotregion(color(white))

graph export "`output'/figures/empirical_facts/figure_3_elevation_costs.pdf", replace
graph export "`output'/figures/empirical_facts/figure_3_elevation_costs.png", width(2400) replace

local p25_dollars = p25[1] * 1000
local median_dollars = median[1] * 1000
local p75_dollars = p75[1] * 1000
display as result "Elevation project value: p25 = $" %9.0fc `p25_dollars' ///
    ", median = $" %9.0fc `median_dollars' ", p75 = $" %9.0fc `p75_dollars'

* -----------------------------------------------------------------------------
* Appendix Figure A1: raw premium comparisons are not causal premium savings
* -----------------------------------------------------------------------------

import excel using "`source'", sheet("premium_rates") firstrow clear case(lower)

gen stratum = 1 + post_firm + 2 * risk_rating_2
gen x = stratum + cond(elevated == 1, .18, -.18)
gen lo = mean_premium_rate_percent - ci95_halfwidth
gen hi = mean_premium_rate_percent + ci95_halfwidth

label define premium_stratum ///
    1 "Legacy, pre-FIRM" ///
    2 "Legacy, post-FIRM" ///
    3 "RR2.0, pre-FIRM" ///
    4 "RR2.0, post-FIRM"
label values stratum premium_stratum

twoway ///
    (bar mean_premium_rate_percent x if elevated == 0, barwidth(.34) color("142 202 230")) ///
    (bar mean_premium_rate_percent x if elevated == 1, barwidth(.34) color("31 111 139")) ///
    (rcap lo hi x, lcolor("36 55 70")), ///
    title("Raw premium differences do not identify elevation savings", size(medsmall)) ///
    subtitle("Descriptive policy-year means; elevated properties are selected on flood risk", size(vsmall)) ///
    xtitle("") ///
    ytitle("Mean premium as percent of building coverage", size(small)) ///
    xlabel(1 "Legacy pre-FIRM" 2 "Legacy post-FIRM" ///
        3 "RR2.0 pre-FIRM" 4 "RR2.0 post-FIRM", labsize(small)) ///
    ylabel(0(.1).7, labsize(small) grid glcolor("229 231 235")) ///
    legend(order(1 "Not recorded elevated" 2 "Recorded elevated") rows(1) size(small)) ///
    graphregion(color(white)) plotregion(color(white))

graph export "`output'/figures/empirical_facts/figure_a1_raw_premium_rates.pdf", replace
graph export "`output'/figures/empirical_facts/figure_a1_raw_premium_rates.png", width(2400) replace

display as text "Premium figure is descriptive only; it does not estimate premium savings from elevation."

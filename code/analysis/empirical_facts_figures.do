/******************************************************************************
Authors: Anna Li
Date: 2026-09-03

Description: Preliminary exhibits for the empirical-facts section.
    Figure 1  -- recorded elevation rate by claim and repetitive-loss group.
    Figure 2  -- recorded elevation rate across block groups ranked by realized loss.
    Figure 3  -- distribution of reported elevation project value.
    Figure 4  -- Builty elevation permits by state: retrofit vs elevated new construction (09-07).
    Figure 5  -- mitigation cost: Builty retrofit project value vs FMA federal share per property (09-07).
    Figure A1 -- raw NFIP premium rates by pricing regime, FIRM status, elevation.

    Everything here is descriptive, not causal. Builty elevations are observed
    only for NFIP properties that were assigned an ATTOM record, so rates are
    taken over properties carrying a Builty measure. Figure A1 compares NFIP's
    recorded elevation stock, not an elevation event, and elevated properties
    are selected on underlying flood risk.

    09-05: Figures 1-2 read property x policy-year and restricted to county-years with Builty coverage.
    Each property is collapsed to its last policy year, so claims are the
    property total and SFHA is the latest rated status. Repetitive-loss status
    is NFIP's multiple-loss file only; the FMA RL flags no longer exist.

    Dollars are 2023 dollars, deflated in the cleaners, premiums included.

******************************************************************************/

args data output

* Set exhibit options
// Figure 1 rows: one label and the condition defining the group, in order.
// Both lists stay on one line each -- /// does not continue a quoted list.
local groups `" "All linked properties" "Outside SFHA" "In SFHA (zone A/V)" "No paid claims" "Any paid claim" "At least \$50,000 paid" "At least \$100,000 paid" "Repetitive loss" "Severe repetitive loss" "'
local conds  `" "observed" "observed & sfha == 0" "observed & sfha == 1" "observed & claim_cb == 0" "observed & claim_cb > 0" "observed & claim_cb >= 50000" "observed & claim_cb >= 100000" "observed & rl" "observed & srl" "'
local min_observed 50 // block groups below this carry too little Builty coverage
local blue   "23 107 135"
local light  "142 202 230"
local orange "224 122 95"
local opts graphregion(color(white)) plotregion(color(white))

* -----------------------------------------------------------------------------
* Figure 1: elevation is rare in every risk group
* -----------------------------------------------------------------------------

// 09-06 change: analysis_no_diagnostics.dta is gone; read the panel, keep the last
// policy year per property, and rename cumulative_claims to the old claim_cb
use state property_id policy_year censusblockgroupfips sfha cumulative_claims ///
    rl srl builty_elevated using "`data'/analysis/analysis.dta", clear
bysort property_id (policy_year): keep if _n == _N
rename cumulative_claims claim_cb

* Recode the group definitions
// Note: builty_elevated is missing where no Builty measure was assigned, which
// is not the same as a property with no elevation permit
gen observed = !mi(builty_elevated)
gen elevated = builty_elevated == 1
replace claim_cb = 0 if mi(claim_cb) | claim_cb < 0

* Count elevations within each group
tempname counts
tempfile fig1
postfile `counts' str40 property_group observed_n elevated_n using "`fig1'"
local ngroups : word count `groups'
forvalues i = 1/`ngroups' {
    local group : word `i' of `groups'
    local cond  : word `i' of `conds'
    qui count if `cond'
    local observed_n = r(N)
    qui count if (`cond') & elevated
    post `counts' ("`group'") (`observed_n') (`r(N)')
}
postclose `counts'

* -----------------------------------------------------------------------------
* Figure 2A: low versus high risk using the NFIP rated flood zone
* -----------------------------------------------------------------------------

* clean_nfip_policies.do defines SFHA from the rated flood zone: A/V zones are
* high risk; observed non-A/V zones are low risk.
gen high_flood_risk = sfha == 1 if !mi(sfha)

quietly correlate high_flood_risk elevated if observed & !mi(high_flood_risk)
matrix flood_zone_correlation = r(C)
local flood_zone_rho = flood_zone_correlation[1,2]
local flood_zone_n = r(N)
local flood_zone_t = `flood_zone_rho' * ///
    sqrt((`flood_zone_n' - 2) / (1 - `flood_zone_rho'^2))
local flood_zone_p = 2 * ttail(`flood_zone_n' - 2, abs(`flood_zone_t'))

preserve
    keep if observed & !mi(high_flood_risk)
    gen properties = 1
    collapse (sum) properties elevations = elevated, by(high_flood_risk)
    gen elevations_per_1000 = 1000 * elevations / properties
    gen elevation_share_percent = 100 * elevations / properties
    gen ci95_halfwidth = 1000 * 1.96 * sqrt((elevations / properties) * ///
        (1 - elevations / properties) / properties)
    gen lo = elevations_per_1000 - ci95_halfwidth
    gen hi = elevations_per_1000 + ci95_halfwidth
    gen group_order = high_flood_risk + 1

    label define flood_zone_risk_label ///
        1 "Low risk: non-SFHA" ///
        2 "High risk: SFHA (A/V zones)"
    label values group_order flood_zone_risk_label

    twoway (bar elevations_per_1000 group_order, barwidth(0.62) color("`blue'")) ///
           (rcap lo hi group_order, lcolor("36 55 70")), ///
        `opts' legend(off) ///
        title("Elevation by NFIP rated flood-risk zone", pos(11) size(medium)) ///
        subtitle("High risk is SFHA: rated A/V zones; 95% CI", ///
            pos(11) size(vsmall) color(gs7)) ///
        xtitle("") ///
        ytitle("Recorded elevation permits per 1,000 linked properties") ///
        xlabel(1 "Low risk: non-SFHA" 2 "High risk: SFHA (A/V)", noticks)
    graph save "`output'/figures/figure_2a_low_vs_high_flood_zone.gph", replace
    graph export "`output'/figures/figure_2a_low_vs_high_flood_zone.png", width(2000) replace

    export excel high_flood_risk properties elevations elevations_per_1000 ///
        elevation_share_percent ci95_halfwidth using ///
        "`output'/tables/empirical_facts.xlsx", ///
        sheet("flood_zone_risk") firstrow(variables) sheetreplace
restore

preserve
    clear
    set obs 1
    gen pearson_correlation = `flood_zone_rho'
    gen p_value = `flood_zone_p'
    gen observations = `flood_zone_n'
    gen definition = "SFHA indicator versus recorded Builty elevation indicator"
    export excel using "`output'/tables/empirical_facts.xlsx", ///
        sheet("flood_zone_correlation") firstrow(variables) sheetreplace
restore

display as result "SFHA-elevation correlation: " %7.5f `flood_zone_rho' ///
    "; p-value: " %7.5f `flood_zone_p' "; N = " %12.0fc `flood_zone_n'

* -----------------------------------------------------------------------------
* Figure 2B: elevation against realized losses, block group by block group
* -----------------------------------------------------------------------------

* Collapse the NFIP universe to block groups
gen one = 1
gen builty_count = elevated
drop if mi(censusblockgroupfips)
collapse (sum) nfip_properties = one paid_losses = claim_cb ///
    builty_observed = observed builty_count, by(state censusblockgroupfips)
gen losses_per_nfip_property = paid_losses / nfip_properties
keep if builty_observed >= `min_observed'

* Rank the positive-loss block groups into property-weighted deciles
// Zero-loss block groups are their own group: they are the bulk of the universe
// and a decile cut would put them all in one bin anyway
gen positive = losses_per_nfip_property > 0
sort positive losses_per_nfip_property
by positive: gen running = sum(builty_observed)
egen weight_total = total(builty_observed), by(positive)
gen midpoint = running - builty_observed / 2
gen risk_group = 0
replace risk_group = min(10, floor(10 * midpoint / weight_total) + 1) if positive

collapse (count) block_groups = positive ///
    (sum) properties = builty_observed elevations = builty_count ///
    losses = paid_losses nfip_properties, by(risk_group)
gen losses_per_nfip_property = losses / nfip_properties

* Graph
gen rate = 1000 * elevations / properties
gen ci   = 1000 * 1.96 * sqrt((elevations / properties) * ///
    (1 - elevations / properties) / properties)
gen lo = rate - ci
gen hi = rate + ci

twoway (rarea lo hi risk_group, color("`light'%35") lwidth(none)) ///
       (line rate risk_group, lcolor("`blue'") lwidth(medthick)) ///
       (scatter rate risk_group, mcolor("`blue'") msize(small)), ///
    `opts' legend(off) ///
    title("Elevation rises with realized losses, but stays extremely rare", ///
        pos(11) size(medium)) ///
    subtitle("Block groups with at least `min_observed' properties carrying a Builty measure; 95% CI", ///
        pos(11) size(vsmall) color(gs7)) ///
    xtitle("Realized-loss group (positive-loss block groups ranked low to high)") ///
    ytitle("Recorded elevation permits per 1,000 properties") ///
    xlabel(0 "Zero" 1(1)10, noticks)
graph save  "`output'/figures/figure_2_mitigation_by_realized_loss.gph", replace
graph export "`output'/figures/figure_2_mitigation_by_realized_loss.png", width(2000) replace

* Table
order risk_group block_groups properties elevations losses nfip_properties ///
    losses_per_nfip_property rate ci
rename (rate ci) (elevations_per_1000 ci95_halfwidth)
export excel using "`output'/tables/empirical_facts.xlsx", ///
    sheet("realized_loss") firstrow(variables) sheetreplace

* -----------------------------------------------------------------------------
* Figure 1 graph and table (built from the counts posted above)
* -----------------------------------------------------------------------------

use "`fig1'", clear
gen rate = 1000 * elevated_n / observed_n
gen ci   = 1000 * 1.96 * sqrt((elevated_n / observed_n) * ///
    (1 - elevated_n / observed_n) / observed_n)
gen lo = rate - ci
gen hi = rate + ci

* Order the bars top to bottom as listed above
gen order = _N - _n + 1
forvalues i = 1/`=_N' {
    label define group_order `=order[`i']' `"`=property_group[`i']'"', add
}
label values order group_order

qui sum hi
local right = r(max)
gen labelpos = hi + `right' * 0.025
gen ratelabel = string(rate, "%4.2f")

twoway (bar rate order, horizontal barwidth(0.6) color("`blue'")) ///
       (rcap lo hi order, horizontal lcolor("36 55 70") msize(small)) ///
       (scatter order labelpos, msymbol(none) mlabel(ratelabel) ///
            mlabcolor(black) mlabsize(vsmall) mlabpos(3)), ///
    `opts' legend(off) ///
    title("Home elevation is rare at every level of flood risk", ///
        pos(11) size(medium)) ///
    subtitle("Builty permits linked to NFIP properties through assigned ATTOM records", ///
        pos(11) size(vsmall) color(gs7)) ///
    xtitle("Properties with a recorded elevation permit per 1,000") ///
    ytitle("") ylabel(1/`=_N', valuelabel angle(0) noticks labsize(vsmall)) ///
    xscale(range(0 `=`right' * 1.15'))
graph save  "`output'/figures/figure_1_mitigation_is_rare.gph", replace
graph export "`output'/figures/figure_1_mitigation_is_rare.png", width(2000) replace

keep property_group observed_n elevated_n rate ci
rename (observed_n elevated_n rate ci) (properties_with_builty_measure ///
    properties_with_elevation_permit elevations_per_1000 ci95_halfwidth)
export excel using "`output'/tables/empirical_facts.xlsx", ///
    sheet("risk_group") firstrow(variables) sheetreplace

* -----------------------------------------------------------------------------
* Figure 3: elevation is a large up-front investment
* -----------------------------------------------------------------------------

* Note: project_value is unreported rather than zero where Builty has no cost
use project_value using "`data'/clean/builty_elevations.dta", clear
keep if project_value > 0 & !mi(project_value)
gen cost = project_value / 1000

qui sum cost, detail
local median = r(p50)
local top = r(p95)
replace cost = min(cost, `top') // top-code the tail so the axis is readable

histogram cost, bin(30) frequency fcolor("`blue'") lcolor(white) ///
    `opts' ///
    title("Elevation requires a large up-front investment", pos(11) size(medium)) ///
    subtitle("Dashed line is the median, \$`=string(`median', "%9.0fc")'k", ///
        pos(11) size(vsmall) color(gs7)) ///
    xtitle("Reported elevation project value (\$1,000s, top 5% winsorized)") ///
    ytitle("Properties") ///
    xline(`median', lcolor("`orange'") lwidth(medthick) lpattern(dash))
graph save  "`output'/figures/figure_3_elevation_costs.gph", replace
graph export "`output'/figures/figure_3_elevation_costs.png", width(2000) replace

* Table
qui sum project_value, detail
clear
set obs 1
gen projects_with_positive_value = `r(N)'
gen p25 = `r(p25)'
gen median = `r(p50)'
gen p75 = `r(p75)'
export excel using "`output'/tables/empirical_facts.xlsx", ///
    sheet("elevation_costs") firstrow(variables) sheetreplace
* -----------------------------------------------------------------------------
* Figure A1: raw premium rates do not identify elevation savings
* -----------------------------------------------------------------------------

use elevated post_firm risk_rating_2 premium coverage_building ///
    using "`data'/clean/nfip_policies_panel.dta", clear
gen rate = 100 * premium / coverage_building
keep if !mi(elevated, post_firm, risk_rating_2) & premium > 0 & ///
    coverage_building > 0 & inrange(rate, 0, 10)

collapse (count) policy_years = rate (mean) mean_rate = rate (sd) sd_rate = rate, ///
    by(risk_rating_2 post_firm elevated)
gen ci = 1.96 * sd_rate / sqrt(policy_years)

* Position the two elevation series side by side within each pricing-FIRM stratum
gen stratum = 1 + 2 * risk_rating_2 + post_firm
gen x = stratum + cond(elevated, 0.18, -0.18)

twoway (bar mean_rate x if !elevated, barwidth(0.36) color("`light'")) ///
       (bar mean_rate x if elevated, barwidth(0.36) color("`blue'")), ///
    `opts' ///
    legend(order(1 "Not recorded elevated" 2 "Recorded elevated") ///
        pos(1) ring(0) col(1) region(lcolor(white)) size(vsmall)) ///
    title("Raw premium differences do not identify elevation savings", ///
        pos(11) size(medium)) ///
    subtitle("Descriptive policy-year means; elevated properties are selected on flood risk", ///
        pos(11) size(vsmall) color(gs7)) ///
    xtitle("") ytitle("Mean premium as percent of building coverage") ///
    yscale(range(0)) ylabel(0(0.1)0.7) ///
    xlabel(1 `""Legacy" "Pre-FIRM""' 2 `""Legacy" "Post-FIRM""' ///
        3 `""RR2.0" "Pre-FIRM""' 4 `""RR2.0" "Post-FIRM""', noticks)
graph save  "`output'/figures/figure_a1_raw_premium_rates.gph", replace
graph export "`output'/figures/figure_a1_raw_premium_rates.png", width(2000) replace

* Table
drop stratum x
rename (mean_rate ci) (mean_premium_rate_percent ci95_halfwidth)
export excel using "`output'/tables/empirical_facts.xlsx", ///
    sheet("premium_rates") firstrow(variables) sheetreplace

* -----------------------------------------------------------------------------
* Figure 4: retrofit vs elevated new construction, by state (09-07)
* -----------------------------------------------------------------------------

* Note: clean_builty.do keeps elevated new construction since 09-06 (new_construction = 1)
use state retrofit new_construction using "`data'/clean/builty_elevations.dta", clear
gen one = 1
collapse (sum) properties = one retrofits = retrofit new_builds = new_construction, by(state)
gen share_new = 100 * new_builds / properties
gsort -properties
gen order = _n
forvalues i = 1/`=_N' {
    label define state_order4 `=order[`i']' "`=state[`i']'", add
}
label values order state_order4

graph bar retrofits new_builds, over(order, label(labsize(small))) stack ///
    bar(1, color("`blue'")) bar(2, color("`orange'")) ///
    `opts' ///
    legend(order(1 "Retrofit: existing structure raised" 2 "Elevated new construction") ///
        rows(1) pos(6) region(lcolor(white)) size(small)) ///
    blabel(total, size(vsmall) format(%9.0fc)) ///
    ytitle("Properties with a Builty elevation permit") ///
    title("Most Builty elevation permits are retrofits", pos(11) size(medium)) ///
    subtitle("Screened permits, one row per property; new construction identified from permit text", ///
        pos(11) size(vsmall) color(gs7))
graph save  "`output'/figures/figure_4_retrofit_vs_new_construction.gph", replace
graph export "`output'/figures/figure_4_retrofit_vs_new_construction.png", width(2000) replace

keep state properties retrofits new_builds share_new
export excel using "`output'/tables/empirical_facts.xlsx", ///
    sheet("retrofit_vs_new") firstrow(variables) sheetreplace

* -----------------------------------------------------------------------------
* Figure 5: mitigation cost, Builty retrofit permits vs FMA grants (09-07)
* -----------------------------------------------------------------------------

* Builty: reported project value on retrofit permits; unreported rather than zero
// Note: LA permits carry no project value at all
use state retrofit project_value using "`data'/clean/builty_elevations.dta", clear
keep if retrofit == 1 & project_value > 0 & !mi(project_value)
gen source = 1
rename project_value cost
tempfile builty_cost
save `builty_cost'

* FMA: federal share obligated per property in the project (2023 $)
use state fma_spend n_properties using "`data'/clean/fma_elevation.dta", clear
replace state = "FL" if state == "Florida"
replace state = "LA" if state == "Louisiana"
replace state = "NJ" if state == "New Jersey"
replace state = "TX" if state == "Texas"
keep if inlist(state, "FL", "LA", "NJ", "TX") & fma_spend > 0 & n_properties > 0
gen cost = fma_spend / n_properties
gen source = 2
append using `builty_cost'
gen ln_cost = ln(cost)
label define source_lbl 1 "Builty retrofit permit: project value" 2 "FMA grant: federal share per property"
label values source source_lbl

* Medians for the subtitle and the table
qui sum cost if source == 1, detail
local med_builty = r(p50)
local n_builty = r(N)
qui sum cost if source == 2, detail
local med_fma = r(p50)
local n_fma = r(N)

twoway (histogram ln_cost if source == 1, percent width(0.25) color("`orange'%60")) ///
       (histogram ln_cost if source == 2, percent width(0.25) color("`blue'%60")), ///
    `opts' ///
    legend(order(1 "Builty retrofit permit: project value (N = `=string(`n_builty', "%9.0fc")')" ///
                 2 "FMA grant: federal share per property (N = `=string(`n_fma', "%9.0fc")')") ///
        rows(2) pos(6) region(lcolor(white)) size(small)) ///
    xtitle("Cost per property, 2023 dollars (log scale)") ytitle("Percent") ///
    xlabel(`=ln(10000)' "10k" `=ln(30000)' "30k" `=ln(100000)' "100k" ///
           `=ln(300000)' "300k" `=ln(1000000)' "1m") ///
    title("Permit values sit far below what FMA pays per elevation", pos(11) size(medium)) ///
    subtitle("Medians: Builty \$`=string(`med_builty' / 1000, "%9.0f")'k, FMA \$`=string(`med_fma' / 1000, "%9.0f")'k; FL LA NJ TX", ///
        pos(11) size(vsmall) color(gs7))
graph save  "`output'/figures/figure_5_mitigation_cost.gph", replace
graph export "`output'/figures/figure_5_mitigation_cost.png", width(2000) replace

* Table
collapse (count) n = cost (p25) p25 = cost (p50) median = cost (p75) p75 = cost, by(source)
decode source, gen(source_name)
drop source
order source_name
export excel using "`output'/tables/empirical_facts.xlsx", ///
    sheet("mitigation_cost") firstrow(variables) sheetreplace

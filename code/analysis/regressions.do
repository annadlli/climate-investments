********************************************************************************
* regressions_revised.do
*
* Core regression analysis for climate-investment project
*
* RESEARCH QUESTIONS
*
*   Q1. County / county-year targeting:
*       Do wealthier counties receive more FEMA mitigation funding,
*       conditional on flood risk exposure?
*
*   Q2. Permit-level conditional cost:
*       Among observed elevation permits, do higher-value properties
*       have higher reported elevation costs?
*
* IMPORTANT
*
*   This project has two distinct levels of analysis:
*
*   (A) County / county-year:
*       speaks to geographic targeting of FEMA resources
*
*   (B) Permit-level:
*       speaks to conditional cost differences among observed elevation permits
*
*   These do NOT answer the same question and should not be interpreted
*   interchangeably.
*
* KEY LIMITATIONS
*
*   - County-level FEMA analysis: no property-level HMA linkage
*   - Wealth proxy in county analysis = NRI building_value
*     (replacement cost / exposure proxy, not household market wealth)
*   - Panel is sparse and likely disaster-driven in some years
*   - Permit-level analysis is conditional on observing an elevation permit
*     with reported job value; it does not explain selection into elevating
*
* INPUTS
*
*   data/merged_panel.dta
*   data/flood_elevation_filters.dta
*
********************************************************************************

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments"
local data "`root'/data"
local out  "`root'/results"

capture mkdir "`out'"

********************************************************************************
* PART A: COUNTY CROSS-SECTION
* Unit: county (N ~ 70), outcomes summed over panel window
*
* PURPOSE:
*   Cross-sectional targeting patterns:
*   Are richer counties associated with more FEMA funding / projects,
*   conditional on flood risk?
********************************************************************************

use "`data'/merged_panel.dta", clear

*----------------------------*
*  A0. Basic cleanup
*----------------------------*
capture confirm string variable fips_county
if _rc != 0 {
    tostring fips_county, replace force
}

replace fips_county = trim(fips_county)

*----------------------------*
*  A1. Collapse to county
*----------------------------*
collapse ///
    (sum)  fed_total        = total_fed_obligated ///
    (sum)  total_funding_cs = total_funding       ///
    (sum)  n_projects_tot   = n_projects          ///
    (sum)  n_elev_tot       = n_elevation         ///
    (sum)  n_buyout_tot     = n_buyout            ///
    (sum)  n_permit_tot     = n_elev_permits      ///
    (max)  ever_hmgp        = has_hmgp            ///
    (firstnm) county_name                         ///
    (mean) population building_value area_sqmi   ///
           nri_score ifl_risk_score ifl_eal_total ///
           svi_score resilience_score             ///
           ifl_eal_rate_building ifl_exp_building ///
           cfl_risk_score hur_risk_score          ///
    , by(fips_county)

*----------------------------*
*  A2. Variable construction
*----------------------------*
gen log_fed        = log(fed_total + 1)
gen log_totalfund  = log(total_funding_cs + 1)
gen log_building   = log(building_value) if building_value > 0
gen log_pop        = log(population) if population > 0
gen log_ifl_eal    = log(ifl_eal_total + 1)
gen log_n_permits  = log(n_permit_tot + 1)

gen fed_per_cap    = fed_total / population if population > 0
gen log_fed_pc     = log(fed_per_cap + 1) if population > 0

gen any_hma        = n_projects_tot > 0 if !missing(n_projects_tot)
gen any_elevproj   = n_elev_tot > 0 if !missing(n_elev_tot)
gen any_buyoutproj = n_buyout_tot > 0 if !missing(n_buyout_tot)
gen any_permit     = n_permit_tot > 0 if !missing(n_permit_tot)

xtile bv_q5 = building_value if !missing(building_value), nq(5)

label var log_fed         "Log(HMA federal obligated + 1)"
label var log_totalfund   "Log(total funding + 1)"
label var log_building    "Log building value (NRI)"
label var log_pop         "Log population"
label var log_ifl_eal     "Log inland flood EAL"
label var ifl_risk_score  "Inland flood risk score"
label var svi_score       "Social vulnerability index"
label var any_hma         "Any HMA project"
label var any_elevproj    "Any elevation project"
label var any_buyoutproj  "Any buyout project"
label var any_permit      "Any elevation permit"

*----------------------------*
*  A3. Sanity checks
*----------------------------*
summ fed_total n_projects_tot n_elev_tot n_buyout_tot n_permit_tot ///
     building_value ifl_risk_score ifl_eal_total population, detail

pwcorr building_value ifl_risk_score ifl_eal_total fed_total n_projects_tot, sig

*----------------------------*
*  A4. Baseline county OLS
*----------------------------*
reg log_fed log_building ifl_risk_score, robust
estimates store m1

reg log_fed log_building ifl_risk_score svi_score, robust
estimates store m2

reg log_fed log_building ifl_risk_score svi_score log_pop, robust
estimates store m3

reg log_fed log_building log_ifl_eal svi_score log_pop, robust
estimates store m4

esttab m1 m2 m3 m4 using "`out'/tab_grant_county_ols.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County cross-section: total HMA funding ~ building value + flood risk") ///
    mtitles("Baseline" "+SVI" "+Population" "EAL spec") ///
    keep(log_building ifl_risk_score log_ifl_eal svi_score log_pop) ///
    stats(N r2, fmt(0 3) labels("N" "R-squared")) ///
    note("Unit: county. Outcome = log(HMA federal obligated + 1). Robust SEs. County-level wealth proxy is NRI building value.")

*----------------------------*
*  A5. Variants
*----------------------------*
gen log_bv_pc = log(building_value / population) if population > 0 & building_value > 0
label var log_bv_pc "Log(building value per capita)"

reg log_fed_pc log_bv_pc ifl_risk_score svi_score, robust
estimates store m5

probit ever_hmgp log_building ifl_risk_score svi_score log_pop, robust
estimates store m6_probit

reg log_fed i.bv_q5 ifl_risk_score svi_score, robust
estimates store m7

esttab m5 m7 using "`out'/tab_grant_variants.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County cross-section: variants") ///
    keep(log_bv_pc 2.bv_q5 3.bv_q5 4.bv_q5 5.bv_q5 ifl_risk_score svi_score) ///
    stats(N r2, fmt(0 3)) ///
    note("Per-capita and quintile specifications. Unit = county.")

*----------------------------*
*  A6. Interaction: high-risk counties
*----------------------------*
quietly summarize ifl_risk_score, detail
gen hi_risk = ifl_risk_score > r(p75) if !missing(ifl_risk_score)

reg log_fed c.log_building##i.hi_risk svi_score log_pop, robust
estimates store m8_interact

margins hi_risk, dydx(log_building)
marginsplot, name(mp_hi_risk, replace)

********************************************************************************
* PART B: COUNTY-YEAR PANEL
* Unit: county-year
*
* PURPOSE:
*   Geographic targeting over time and permit-project alignment.
*
* INTERPRETATION:
*   These regressions speak to COUNTY-LEVEL TARGETING, not household behavior.
********************************************************************************

use "`data'/merged_panel.dta", clear

capture confirm string variable fips_county
if _rc != 0 {
    tostring fips_county, replace force
}
replace fips_county = trim(fips_county)

*----------------------------*
*  B0. ID setup
*----------------------------*
egen county_id = group(fips_county), label
xtset county_id year

*----------------------------*
*  B1. Core variables
*----------------------------*
gen log_fed_yr      = log(total_fed_obligated + 1)
gen log_totalfund   = log(total_funding + 1)
gen log_n_proj      = log(n_projects + 1)
gen log_n_permit    = log(n_elev_permits + 1)
gen log_n_elev      = log(n_elevation + 1)
gen log_n_buyout    = log(n_buyout + 1)

gen got_hma         = n_projects > 0 if !missing(n_projects)
gen got_elevproj    = n_elevation > 0 if !missing(n_elevation)
gen got_buyoutproj  = n_buyout > 0 if !missing(n_buyout)
gen got_permit      = n_elev_permits > 0 if !missing(n_elev_permits)

gen log_building    = log(building_value) if building_value > 0
gen log_pop         = log(population) if population > 0
gen log_ifl_eal     = log(ifl_eal_total + 1)

label var got_hma        "Any HMA project"
label var got_elevproj   "Any elevation project"
label var got_buyoutproj "Any buyout project"
label var got_permit     "Any elevation permit"

*----------------------------*
*  B2. Sanity checks
*----------------------------*
misstable summarize total_fed_obligated n_projects n_elevation n_buyout ///
    n_elev_permits building_value ifl_risk_score ifl_eal_total population

tab got_hma
tab got_elevproj
tab got_buyoutproj
tab got_permit

*----------------------------*
*  B3. Preferred county-year targeting regressions
*  Uses cross-county variation + year FE
*----------------------------*
reg got_hma log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store t1

reg got_elevproj log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store t2

reg got_buyoutproj log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store t3

reg log_fed_yr log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store t4

esttab t1 t2 t3 t4 using "`out'/tab_county_targeting.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County-year targeting: FEMA outcomes ~ county building value + flood risk") ///
    mtitles("Any HMA" "Any elevation project" "Any buyout" "Log HMA dollars") ///
    keep(log_building ifl_risk_score svi_score log_pop) ///
    stats(N r2, fmt(0 3)) ///
    note("Unit = county-year. Wealth proxy = county-level NRI building value. Year FE included. SE clustered by county.")

*----------------------------*
*  B4. Alternative risk specification
*----------------------------*
reg log_fed_yr log_building log_ifl_eal svi_score log_pop i.year, vce(cluster county_id)
estimates store t5

reg got_elevproj log_building log_ifl_eal svi_score log_pop i.year, vce(cluster county_id)
estimates store t6

esttab t5 t6 using "`out'/tab_county_targeting_eal.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County-year targeting: alternative EAL specification") ///
    mtitles("Log HMA dollars" "Any elevation project") ///
    keep(log_building log_ifl_eal svi_score log_pop) ///
    stats(N r2, fmt(0 3)) ///
    note("Uses inland flood expected annual loss instead of risk score.")

*----------------------------*
*  B5. County FE panel
*  Caution: time-invariant NRI vars are absorbed
*----------------------------*
xtreg log_fed_yr i.year, fe vce(cluster county_id)
estimates store p1_fe

xtreg log_n_proj i.year, fe vce(cluster county_id)
estimates store p2_fe

xtreg log_n_elev i.year, fe vce(cluster county_id)
estimates store p3_fe

esttab p1_fe p2_fe p3_fe using "`out'/tab_panel_fe_outcomes.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County FE panel: within-county outcome variation over time") ///
    mtitles("Log HMA dollars" "Log # projects" "Log # elevation projects") ///
    stats(N r2_w, fmt(0 3) labels("N" "Within R-squared")) ///
    note("County FE absorb time-invariant wealth/risk proxies. These specs describe within-county time patterns only.")

*----------------------------*
*  B6. Lagged FEMA -> permit alignment
*  Descriptive only
*----------------------------*
sort county_id year
by county_id: gen lag1_fed = total_fed_obligated[_n-1]
by county_id: gen lag2_fed = total_fed_obligated[_n-2]
by county_id: gen lag1_elevproj = n_elevation[_n-1]
by county_id: gen lag2_elevproj = n_elevation[_n-2]

gen log_lag1_fed = log(lag1_fed + 1)
gen log_lag2_fed = log(lag2_fed + 1)

label var log_lag1_fed "Log lagged HMA dollars (t-1)"
label var log_lag2_fed "Log lagged HMA dollars (t-2)"

areg log_n_permit log_lag1_fed log_lag2_fed i.year, absorb(county_id) vce(cluster county_id)
estimates store lag1

areg got_permit log_lag1_fed log_lag2_fed i.year, absorb(county_id) vce(cluster county_id)
estimates store lag2

areg log_n_permit lag1_elevproj lag2_elevproj i.year, absorb(county_id) vce(cluster county_id)
estimates store lag3

esttab lag1 lag2 lag3 using "`out'/tab_permits_laghma.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County-year: permits ~ lagged FEMA activity") ///
    mtitles("Log permits" "Any permit (LPM)" "Log permits ~ lagged elev. projects") ///
    keep(log_lag1_fed log_lag2_fed lag1_elevproj lag2_elevproj) ///
    stats(N r2, fmt(0 3)) ///
    note("County + year FE. SE clustered by county. Descriptive only: both FEMA activity and permits may be driven by disasters.")

********************************************************************************
* PART C: PERMIT-LEVEL CONDITIONAL COST ANALYSIS
* Unit: individual elevation permit
*
* IMPORTANT INTERPRETATION
*
*   - In this section, the RHS is PROPERTY-LEVEL tax value
*     (TAXMARKETVALUETOTAL / TAXASSESSEDVALUETOTAL),
*     not county average wealth.
*
*   - Therefore, coefficients here are interpreted as within-sample
*     permit/property gradients, not county-level targeting.
*
*   - If county FE are added, log_prop_val reflects within-county
*     differences across permitted properties.
*
* WHAT THIS SECTION CAN ANSWER
*
*   - Among observed elevation permits, do higher-value properties have
*     higher reported elevation job values?
*
* WHAT THIS SECTION CANNOT ANSWER
*
*   - Whether FEMA targets wealthier counties
*   - Whether wealthier homes are more likely to elevate at all
*     (sample is already restricted to observed elevation permits)
********************************************************************************

use "`data'/flood_elevation_filters.dta", clear

*----------------------------*
*  C0. Sample definition
*----------------------------*
keep if flood_elev_broad == 1

gen permit_year = year(dofc(PERMIT_DATE))
keep if permit_year >= 2000 & !missing(permit_year)

keep if JOB_VALUE > 0

gen prop_val = TAXMARKETVALUETOTAL
replace prop_val = TAXASSESSEDVALUETOTAL if missing(prop_val)
drop if missing(prop_val) | prop_val <= 0

*----------------------------*
*  C1. Core variables
*----------------------------*
gen log_job_val  = log(JOB_VALUE)
gen log_prop_val = log(prop_val)

gen fips_county = trim(SITUSSTATECOUNTYFIPS)
replace fips_county = substr(fips_county,1,2) + "0" + substr(fips_county,3,.) ///
    if length(fips_county) == 4

egen county_id = group(fips_county), label

label var log_job_val  "Log reported elevation job value"
label var log_prop_val "Log property tax value"

*----------------------------*
*  C2. Sanity checks
*----------------------------*
summ JOB_VALUE prop_val log_job_val log_prop_val, detail
tab RESIDENTIAL

*----------------------------*
*  C3. Main permit-level regressions
*----------------------------*
reg log_job_val log_prop_val, robust
estimates store c1

reg log_job_val log_prop_val RESIDENTIAL i.permit_year, robust
estimates store c2

areg log_job_val log_prop_val RESIDENTIAL i.permit_year, absorb(county_id) vce(cluster county_id)
estimates store c3

*----------------------------*
*  C4. Nonlinear bins
*----------------------------*
xtile pv_q4 = prop_val if !missing(prop_val), nq(4)

reg log_job_val i.pv_q4 RESIDENTIAL i.permit_year, robust
estimates store c4

*----------------------------*
*  C5. Interpretation notes
*----------------------------*
*
* c1-c2:
*   Cross-sectional gradient: permits on higher-value properties tend
*   to have higher reported job values.
*
* c3:
*   County FE absorb across-county construction cost differences.
*   The coefficient on log_prop_val then reflects within-county
*   differences across permitted properties.
*
* If log_prop_val becomes small / insignificant with county FE:
*   the raw positive gradient is mostly driven by across-county
*   differences, not richer properties spending more within county.
*
* This section is conditional on observing an elevation permit and does
* not identify selection into elevating.

esttab c1 c2 c3 c4 using "`out'/tab_permit_level.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("Permit-level: reported elevation cost ~ property tax value") ///
    mtitles("OLS" "+Year FE" "+County FE" "Quartile bins") ///
    keep(log_prop_val RESIDENTIAL 2.pv_q4 3.pv_q4 4.pv_q4) ///
    stats(N r2, fmt(0 3)) ///
    note("Unit = permit. Outcome = log(job value). Sample = elevation permits with JOB_VALUE > 0. RHS uses property-level tax value, not county average wealth. County-FE spec identifies within-county differences among permitted properties.")

	
//county level
********************************************************************************
* PART B: COUNTY-YEAR REGRESSIONS
* Unit: county-year
* Main question:
*   Are FEMA mitigation outcomes concentrated in wealthier counties,
*   conditional on flood risk?
********************************************************************************

use "`data'/merged_panel.dta", clear

*----------------------------*
* B0. IDs and setup
*----------------------------*
capture confirm string variable fips_county
if _rc != 0 {
    tostring fips_county, replace force
}
replace fips_county = trim(fips_county)

egen county_id = group(fips_county), label
xtset county_id year

*----------------------------*
* B1. Core variables
*----------------------------*
gen log_fed_yr     = log(total_fed_obligated + 1)
gen log_n_proj     = log(n_projects + 1)
gen log_n_elev     = log(n_elevation + 1)
gen log_n_buyout   = log(n_buyout + 1)
gen log_n_permit   = log(n_elev_permits + 1)

gen got_hma        = n_projects > 0 if !missing(n_projects)
gen got_elevproj   = n_elevation > 0 if !missing(n_elevation)
gen got_buyoutproj = n_buyout > 0 if !missing(n_buyout)
gen got_permit     = n_elev_permits > 0 if !missing(n_elev_permits)

gen log_building   = log(building_value) if building_value > 0
gen log_pop        = log(population) if population > 0
gen log_ifl_eal    = log(ifl_eal_total + 1)

label var log_fed_yr     "Log(HMA federal obligated + 1)"
label var log_n_proj     "Log(# HMA projects + 1)"
label var log_n_elev     "Log(# elevation projects + 1)"
label var log_n_buyout   "Log(# buyout projects + 1)"
label var log_n_permit   "Log(# elevation permits + 1)"
label var got_hma        "Any HMA project"
label var got_elevproj   "Any elevation project"
label var got_buyoutproj "Any buyout project"
label var got_permit     "Any elevation permit"
label var log_building   "Log building value (NRI)"
label var ifl_risk_score "Inland flood risk score"
label var log_ifl_eal    "Log inland flood EAL"
label var svi_score      "Social vulnerability index"
label var log_pop        "Log population"

*----------------------------*
* B2. Main county-year targeting regressions
* Year FE + county-clustered SE
*----------------------------*

* Any FEMA activity
reg got_hma log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store cy1

* Any elevation project
reg got_elevproj log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store cy2

* Any buyout project
reg got_buyoutproj log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store cy3

* HMA dollars
reg log_fed_yr log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store cy4

esttab cy1 cy2 cy3 cy4 using "`out'/tab_county_year_targeting.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County-year: FEMA outcomes ~ county building value + flood risk") ///
    mtitles("Any HMA" "Any elevation project" "Any buyout project" "Log HMA dollars") ///
    keep(log_building ifl_risk_score svi_score log_pop) ///
    stats(N r2, fmt(0 3) labels("N" "R-squared")) ///
    note("Unit = county-year. Wealth proxy = county-level NRI building value. Year fixed effects included. Standard errors clustered by county.")

*----------------------------*
* B3. Alternative risk measure: EAL instead of risk score
*----------------------------*
reg got_elevproj log_building log_ifl_eal svi_score log_pop i.year, vce(cluster county_id)
estimates store cy5

reg log_fed_yr log_building log_ifl_eal svi_score log_pop i.year, vce(cluster county_id)
estimates store cy6

esttab cy5 cy6 using "`out'/tab_county_year_targeting_eal.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County-year: FEMA outcomes ~ county building value + inland flood EAL") ///
    mtitles("Any elevation project" "Log HMA dollars") ///
    keep(log_building log_ifl_eal svi_score log_pop) ///
    stats(N r2, fmt(0 3) labels("N" "R-squared")) ///
    note("Unit = county-year. Uses inland flood expected annual loss instead of flood risk score. Year fixed effects included. Standard errors clustered by county.")

*----------------------------*
* B4. Permit alignment: are more permits seen where FEMA elevation activity occurs?
*----------------------------*
reg got_permit got_elevproj log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store cy7

reg log_n_permit got_elevproj log_building ifl_risk_score svi_score log_pop i.year, vce(cluster county_id)
estimates store cy8

esttab cy7 cy8 using "`out'/tab_county_year_permit_alignment.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County-year: elevation permits ~ FEMA elevation activity") ///
    mtitles("Any permit" "Log permits") ///
    keep(got_elevproj log_building ifl_risk_score svi_score log_pop) ///
    stats(N r2, fmt(0 3) labels("N" "R-squared")) ///
    note("Unit = county-year. Descriptive correlation only. Year fixed effects included. Standard errors clustered by county.")

*----------------------------*
* B5. Lagged FEMA activity -> permits
*----------------------------*
sort county_id year
by county_id: gen lag1_fed = total_fed_obligated[_n-1]
by county_id: gen lag2_fed = total_fed_obligated[_n-2]
by county_id: gen lag1_elev = n_elevation[_n-1]
by county_id: gen lag2_elev = n_elevation[_n-2]

gen log_lag1_fed = log(lag1_fed + 1)
gen log_lag2_fed = log(lag2_fed + 1)

label var log_lag1_fed "Log lagged HMA dollars (t-1)"
label var log_lag2_fed "Log lagged HMA dollars (t-2)"

areg got_permit log_lag1_fed log_lag2_fed i.year, absorb(county_id) vce(cluster county_id)
estimates store cy9

areg log_n_permit log_lag1_fed log_lag2_fed i.year, absorb(county_id) vce(cluster county_id)
estimates store cy10

areg log_n_permit lag1_elev lag2_elev i.year, absorb(county_id) vce(cluster county_id)
estimates store cy11

esttab cy9 cy10 cy11 using "`out'/tab_county_year_lags.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("County-year: permits ~ lagged FEMA activity") ///
    mtitles("Any permit" "Log permits ~ lagged $" "Log permits ~ lagged elev. projects") ///
    keep(log_lag1_fed log_lag2_fed lag1_elev lag2_elev) ///
    stats(N r2, fmt(0 3) labels("N" "R-squared")) ///
    note("Unit = county-year. County and year fixed effects in areg specifications. Standard errors clustered by county. Descriptive only.")
********************************************************************************
* PART D: OPTIONAL GRAPHICS / DIAGNOSTICS
********************************************************************************

*----------------------------*
*  D1. County-level descriptives
*----------------------------*
use "`data'/merged_panel.dta", clear

capture confirm string variable fips_county
if _rc != 0 {
    tostring fips_county, replace force
}
replace fips_county = trim(fips_county)

egen county_id = group(fips_county), label

gen log_fed_yr    = log(total_fed_obligated + 1)
gen log_building  = log(building_value) if building_value > 0
gen got_hma       = n_projects > 0 if !missing(n_projects)
gen got_elevproj  = n_elevation > 0 if !missing(n_elevation)
gen got_buyout    = n_buyout > 0 if !missing(n_buyout)

xtile bv_q4 = building_value if !missing(building_value), nq(4)

preserve
collapse (mean) got_hma got_elevproj got_buyout log_fed_yr ifl_risk_score, by(bv_q4)
graph bar got_hma got_elevproj got_buyout, over(bv_q4) ///
    title("Mean FEMA activity by building-value quartile") ///
    legend(order(1 "Any HMA" 2 "Any elevation proj." 3 "Any buyout"))
graph export "`out'/fig_fema_by_bv_quartile.png", replace
restore

* Scatter: HMA dollars vs building value
twoway ///
    (scatter total_fed_obligated building_value, msize(small)) ///
    (lfit total_fed_obligated building_value), ///
    title("County-year HMA dollars vs building value")
graph export "`out'/fig_hma_vs_building.png", replace

* Scatter: HMA dollars vs inland flood risk
twoway ///
    (scatter total_fed_obligated ifl_risk_score, msize(small)) ///
    (lfit total_fed_obligated ifl_risk_score), ///
    title("County-year HMA dollars vs inland flood risk")
graph export "`out'/fig_hma_vs_risk.png", replace

********************************************************************************
* END
********************************************************************************
display "Revised regressions do-file completed successfully."

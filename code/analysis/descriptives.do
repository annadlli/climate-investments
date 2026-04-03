* descriptives.do
* Key descriptive analysis for flood mitigation equity paper
*
* Covers:
*   7.1 Elevation permit frequency vs property value
*   7.2 Grant exposure vs property value (conditional on flood risk)
*   8.1-8.2 Buyout vs elevation comparison
*
* Inputs:
*   data/flood_elevation_filters.dta  (permit-level)
*   data/merged_panel.dta             (county×year panel)
*   data/fema/hma_tx_projects.dta     (project-level HMA)
*
* Run after build pipeline is complete.

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments"
local data "`root'/data"
local out  "`root'/results"

capture mkdir "`out'"

********************************************************************************
* 1. PERMIT-LEVEL: Elevation frequency vs property value
* Question: do higher-value homes elevate more?
********************************************************************************
use "`data'/flood_elevation_filters.dta", clear

* Keep elevation permits only
keep if flood_elev_broad == 1

* Permit year (PERMIT_DATE is %tc — must use dofc())
gen permit_year = year(dofc(PERMIT_DATE))
keep if permit_year >= 2000 & !missing(permit_year)

* Property value: prefer market, fall back to assessed
gen prop_val = TAXMARKETVALUETOTAL
replace prop_val = TAXASSESSEDVALUETOTAL if missing(prop_val)

* Drop zero / implausible property values
drop if missing(prop_val) | prop_val <= 0

* Log property value
gen log_prop_val = log(prop_val)
label var log_prop_val "Log property value"

* Job value (cost of elevation work)
gen log_job_val = log(JOB_VALUE) if JOB_VALUE > 0
label var log_job_val "Log job value (elevation cost)"

* -------------------------------------------------------
* 1a. Summary stats by property value quintile
* -------------------------------------------------------
xtile pv_q5 = prop_val, nq(5)
label var pv_q5 "Property value quintile (1=lowest)"

tabstat prop_val JOB_VALUE RESIDENTIAL, by(pv_q5) ///
    stat(n mean p50) col(stat) nototal

* Share of residential permits by quintile
bysort pv_q5: egen q_total = count(pv_q5)
bysort pv_q5: egen q_resid = total(RESIDENTIAL == 1)
gen pct_resid = q_resid / q_total * 100

tabstat pct_resid, by(pv_q5) stat(mean) col(stat)

* -------------------------------------------------------
* 1b. Log-log regression: job value on property value
* (among permits with nonzero job value)
* -------------------------------------------------------
reg log_job_val log_prop_val RESIDENTIAL i.permit_year if JOB_VALUE > 0, robust
estimates store m_jobval

* With county FE
areg log_job_val log_prop_val RESIDENTIAL i.permit_year if JOB_VALUE > 0, ///
    absorb(COUNTY_FIPS) robust
estimates store m_jobval_cfe

esttab m_jobval m_jobval_cfe, ///
    b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("Log job value ~ log property value") ///
    keep(log_prop_val RESIDENTIAL)

* -------------------------------------------------------
* 1c. Scatter: median job value by property value decile
* (for figures)
* -------------------------------------------------------
xtile pv_d10 = prop_val, nq(10)

preserve
    keep if JOB_VALUE > 0
    collapse (median) med_job_val = JOB_VALUE ///
             (mean)   avg_job_val = JOB_VALUE ///
             (count)  n_permits   = JOB_VALUE ///
             (p50)    med_prop    = prop_val  ///
             , by(pv_d10)

    list pv_d10 med_prop med_job_val n_permits, clean noobs sep(0)

    * Plot
    twoway scatter med_job_val med_prop, ///
        msymbol(circle) mcolor(navy) msize(medlarge) ///
        || lfit med_job_val med_prop, lcolor(cranberry) lwidth(medthick) ///
        xtitle("Median property value ($, by decile)") ///
        ytitle("Median elevation job value ($)") ///
        title("Elevation cost vs property value") ///
        legend(off) ///
        note("Each point = property value decile. Sample: permits with non-zero job value.")
    graph export "`out'/fig_jobval_propval.png", replace width(1200)
restore

********************************************************************************
* 2. COUNTY-LEVEL: Grant exposure vs property value
* Question: do wealthier counties receive more HMA funding, conditional on risk?
********************************************************************************
use "`data'/merged_panel.dta", clear

* Collapse to county cross-section (sum over years for outcomes,
* use NRI variables as cross-sectional controls)
collapse ///
    (sum)  total_fed_cum    = total_fed_obligated    ///
    (sum)  n_proj_cum       = n_projects             ///
    (sum)  n_elev_cum       = n_elevation            ///
    (sum)  n_buyout_cum     = n_buyout               ///
    (sum)  n_permit_cum     = n_elev_permits         ///
    (max)  ever_hmgp        = has_hmgp               ///
    (max)  ever_fma         = has_fma                ///
    (firstnm) county_name state_name nri_rating       ///
              ifl_risk_rating svi_rating              ///
    (mean) population building_value area_sqmi        ///
           nri_score ifl_risk_score ifl_eal_total      ///
           svi_score resilience_score                  ///
           cfl_risk_score hur_risk_score               ///
    , by(fips_county)

* Log transforms
gen log_building_val   = log(building_value) if building_value > 0
gen log_fed_cum        = log(total_fed_cum)  if total_fed_cum  > 0
gen log_pop            = log(population)     if population     > 0
gen fed_per_1000pop    = total_fed_cum / (population / 1000) if population > 0
gen log_fed_per_1000   = log(fed_per_1000pop) if fed_per_1000pop > 0
gen log_ifl_eal        = log(ifl_eal_total) if ifl_eal_total > 0

label var log_building_val  "Log total building value (NRI, $)"
label var log_fed_cum       "Log cumulative federal HMA obligated ($)"
label var log_pop           "Log population"
label var fed_per_1000pop   "Federal HMA per 1,000 residents ($)"
label var log_fed_per_1000  "Log federal HMA per 1,000 residents"
label var log_ifl_eal       "Log inland flood EAL ($)"

* -------------------------------------------------------
* 2a. Summary: who gets HMA money?
* -------------------------------------------------------
tabstat total_fed_cum nri_score ifl_risk_score svi_score population, ///
    by(ever_hmgp) stat(n mean p50) col(stat) nototal

* -------------------------------------------------------
* 2b. Property value quintiles vs grant receipt
* -------------------------------------------------------
xtile bv_q5 = building_value, nq(5)
label var bv_q5 "Building value quintile (1=lowest)"

tabstat total_fed_cum n_proj_cum ever_hmgp ifl_risk_score svi_score, ///
    by(bv_q5) stat(n mean p50) col(stat) nototal

* -------------------------------------------------------
* 2c. Core regression: grant = f(building value, flood risk)
* See regressions.do for full results
* -------------------------------------------------------

* Quick OLS check (N=70)
reg log_fed_cum log_building_val ifl_risk_score svi_score log_pop if total_fed_cum > 0, robust
estimates store m_grant_ols

reg log_fed_per_1000 log_building_val ifl_risk_score svi_score if fed_per_1000pop > 0, robust
estimates store m_grant_percap

esttab m_grant_ols m_grant_percap, ///
    b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("Log HMA funding ~ log building value + flood risk") ///
    keep(log_building_val ifl_risk_score svi_score log_pop)

* -------------------------------------------------------
* 2d. Scatter: cumulative HMA $ vs building value
* -------------------------------------------------------
twoway ///
    (scatter total_fed_cum building_value if total_fed_cum > 0, ///
        msymbol(circle) mcolor(navy%60) msize(small)) ///
    (lfit total_fed_cum building_value if total_fed_cum > 0, ///
        lcolor(cranberry) lwidth(medthick)) ///
    , xtitle("Total building value ($, NRI)") ///
      ytitle("Cumulative federal HMA obligated ($, 2000-2023)") ///
      title("HMA funding vs building value") ///
      legend(off) ///
      note("County-level. Excludes counties with zero HMA funding.")
graph export "`out'/fig_hma_buildingval.png", replace width(1200)

********************************************************************************
* 3. BUYOUT vs ELEVATION PATTERNS
* Question: do different types of properties get different interventions?
********************************************************************************
use "`data'/fema/hma_tx_projects.dta", clear

* Keep elevation and buyout only
keep if inlist(main_type, "elevation", "buyout")

* Clean numeric vars
foreach v of varlist projectamount federalshareobligated benefitcostratio ///
    numberofproperties numberoffinalproperties {
    capture destring `v', replace force
}

* -------------------------------------------------------
* 3a. Summary stats by project type
* -------------------------------------------------------
tabstat projectamount federalshareobligated benefitcostratio ///
    numberofproperties numberoffinalproperties, ///
    by(main_type) stat(n mean p25 p50 p75) col(stat) nototal

* -------------------------------------------------------
* 3b. Cost per property: elevation vs buyout
* -------------------------------------------------------
gen cost_per_prop = federalshareobligated / numberoffinalproperties ///
    if numberoffinalproperties > 0
label var cost_per_prop "Federal cost per completed property ($)"

tabstat cost_per_prop, by(main_type) stat(n mean p25 p50 p75 p99) col(stat) nototal

* -------------------------------------------------------
* 3c. Program mix: elevation vs buyout by program
* -------------------------------------------------------
tab main_type programarea, row

* -------------------------------------------------------
* 3d. Time trends: elevation vs buyout projects per year
* -------------------------------------------------------
collapse (count) n = main_type (sum) n_elev = elev_flag (sum) n_buyout = buyout_flag ///
    (sum) fed_obligated = federalshareobligated, by(year main_type)

* Reshape for plotting
reshape wide n n_elev n_buyout fed_obligated, i(year) j(main_type) string

replace nelevation  = 0 if missing(nelevation)
replace nbuyout     = 0 if missing(nbuyout)

twoway ///
    (connected nelevation  year, lcolor(navy)   mcolor(navy)   msymbol(circle)) ///
    (connected nbuyout     year, lcolor(cranberry) mcolor(cranberry) msymbol(square)) ///
    , xtitle("Year") ytitle("Number of projects") ///
      title("HMA projects by type: Texas") ///
      legend(order(1 "Elevation (202.x)" 2 "Buyout (200.x/201.x)")) ///
      xlabel(1990(5)2025) ///
      note("Source: FEMA HMA Projects database.")
graph export "`out'/fig_elev_buyout_time.png", replace width(1200)

********************************************************************************
* 4. OVERLAP: Do elevation permit counties also get HMA elevation grants?
********************************************************************************
use "`data'/merged_panel.dta", clear

* County-level summary
collapse ///
    (sum)  permit_total  = n_elev_permits  ///
    (sum)  n_elev_hma    = n_elevation     ///
    (sum)  n_buyout_hma  = n_buyout        ///
    (sum)  fed_total     = total_fed_obligated ///
    (firstnm) county_name ifl_risk_rating nri_score ifl_eal_total svi_score ///
    , by(fips_county)

gen has_permits = permit_total > 0
gen has_hma_elev = n_elev_hma > 0
gen has_hma_buyout = n_buyout_hma > 0

* 2x2: elevation permits vs HMA elevation grants
tab has_permits has_hma_elev, row col

* Correlation at county level
pwcorr permit_total n_elev_hma n_buyout_hma fed_total nri_score ifl_eal_total, sig

list county_name permit_total n_elev_hma n_buyout_hma fed_total ifl_risk_rating ///
    if has_permits == 1, sep(0) clean noobs

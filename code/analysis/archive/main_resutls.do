********************************************************************************
* main_results.do
*
* CLEAN VERSION — MAIN RESULTS ONLY
*
* Sections:
*   1. RD: 50% damage threshold → ICC payments
*   2. Application vs approval (elevation vs buyout)
*   3. Time trends: HMA projects by type
*   4. Permit-level cost elasticity
*
********************************************************************************

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments"
local data "`root'/data"
local out  "`root'/results"

capture mkdir "`out'"

********************************************************************************
* 1. RD: 50% DAMAGE THRESHOLD → ICC PAYMENTS
********************************************************************************

// use "`data'/fema/nfip_tx_claims.dta", clear
//
// keep if !missing(damage_ratio) & ratio_suspect == 0
//
// * Density test
// rddensity damage_ratio, c(0.50)
// rddensity damage_ratio, c(0.50) plot
// graph export "`out'/fig_rd_density.png", replace
//
// * RD plot
// rdplot got_icc damage_ratio, c(0.50) ///
//     nbins(40 40) ///
//     graph_options(xtitle("Damage ratio") ///
//                   ytitle("Probability of ICC payment") ///
//                   title("RD: ICC payment at 50% threshold") ///
//                   xline(0.50, lcolor(red) lpattern(dash)))
// graph export "`out'/fig_rd_icc.png", replace
//
// * RD estimate
// rdrobust got_icc damage_ratio, c(0.50)

********************************************************************************
* 2. APPLICATION VS APPROVAL (ELEVATION PENALTY)
********************************************************************************

use "`data'/fema/hma_tx_projects.dta", clear

* Clean numeric variables
foreach v of varlist projectamount benefitcostratio {
    destring `v', replace force
}

*got grant
gen got_grant = (status == "Approved" | status == "Obligated" | status == "Closed")
* Keep only elevation and buyout projects
keep if elev_flag == 1 | buyout_flag == 1

* Log project amount
gen log_proj_amt = log(projectamount) if projectamount > 0

* -------------------------------------------------------
* 1. Simple LPM: elevation relative to buyout
* Since sample is restricted to elevation/buyout only,
* buyout is the omitted category when elev_flag = 0
* -------------------------------------------------------
reg got_grant elev_flag benefitcostratio log_proj_amt, robust
estimates store m_app_lpm

* -------------------------------------------------------
* 2. Logit version
* -------------------------------------------------------
logit got_grant elev_flag benefitcostratio log_proj_amt, robust
margins, dydx(elev_flag benefitcostratio log_proj_amt ) post
estimates store m_app_logit

* -------------------------------------------------------
* 3. Export
* -------------------------------------------------------
esttab m_app_lpm m_app_logit using "`out'/tab_approval_main.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("Grant approval: elevation vs buyout") ///
    mtitles("LPM" "Logit marginal effects") ///
    keep(elev_flag benefitcostratio log_proj_amt ) ///
    stats(N r2, fmt(0 3) labels("N" "R-squared")) ///
    note("Sample restricted to elevation and buyout projects. Coefficient on elev_flag is relative to buyout.")
********************************************************************************
* 3. TIME TRENDS: PROJECTS BY TYPE
********************************************************************************

use "`data'/fema/hma_tx_projects.dta", clear

collapse ///
    (sum) n_elev = elev_flag ///
    (sum) n_buy  = buyout_flag ///
    , by(year)

twoway ///
    (line n_elev year, lcolor(navy) lwidth(medthick)) ///
    (line n_buy year, lcolor(red) lwidth(medthick)), ///
    title("HMA projects by type: Texas") ///
    legend(order(1 "Elevation" 2 "Buyout")) ///
    ytitle("Number of projects")

graph export "`out'/fig_projects_time.png", replace

********************************************************************************
* 4. PERMIT-LEVEL COST ELASTICITY
********************************************************************************

use "`data'/flood_elevation_filters.dta", clear

keep if flood_elev_final == 1
keep if JOB_VALUE > 0

gen prop_val = TAXMARKETVALUETOTAL
replace prop_val = TAXASSESSEDVALUETOTAL if missing(prop_val)
drop if missing(prop_val) | prop_val <= 0

gen log_job_val  = log(JOB_VALUE)
gen log_prop_val = log(prop_val)

gen fips_county = trim(SITUSSTATECOUNTYFIPS)
egen county_id = group(fips_county)
gen year = year(dofc(PERMIT_DATE))
* Main regression
areg log_job_val log_prop_val i.year, absorb(county_id) vce(cluster county_id)
estimates store m_cost

esttab m_cost using "`out'/tab_permit_cost.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("Elevation cost ~ property value (within county)")

********************************************************************************
* DONE
********************************************************************************

display "Main results do-file completed."

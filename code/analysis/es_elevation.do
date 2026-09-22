/******************************************************************************
Authors: Anna Li
Date: 2026-09-17
 09-22: section 4, the same event study run separately by elevation source (Anna 09-22).
Description: event study of elevation permits, nfip premiums, and claims
******************************************************************************/

args data output

* setup
local control_share = 0.10 //arbitrary choice
local cohort_first  = 2012
local cohort_last   = 2022
local window        = 5
local ref_period    = -1
//arbitrary define flood event year
local flood_share   = 0.05
local min_flood_claims = 50

local blue "23 107 135"
local opts graphregion(color(white)) plotregion(color(white))

set seed 20260917

cap log close es_elevation
log using "`output'/logs/es_elevation.log", replace text name(es_elevation)


* =============================================================================
* 1 sample
* =============================================================================

* load variables used in the analysis
use state property_id policy_year flood_zone premium claim ///
    construction_year post_firm elevation_retrofit elevation_source ///
    elevation_year elevation_cost attom_matched censusblockgroupfips ///
    using "`data'/analysis/analysis.dta", clear

* create county, flood zone, and claim indicator
gen countycode = substr(censusblockgroupfips, 1, 5)
gen zone = substr(flood_zone, 1, 1)
gen any_claim = claim > 0
drop censusblockgroupfips flood_zone

* define flood county-years using the full insured sample
preserve
    collapse (mean) share_claim = any_claim (sum) n_claims = any_claim, by(countycode policy_year)
    gen flood_year = share_claim >= `flood_share' & n_claims >= `min_flood_claims'
    keep countycode policy_year flood_year
    tempfile floods
    save `floods'
restore

* keep attom-matched homes so treatment can be observed for both groups
keep if attom_matched == 1

* define treated and never-elevated homes: use harmonized elevation variable
gen permit = elevation_retrofit

* keep counties with at least one treated home
bysort countycode: egen county_has_permit = max(permit)
keep if county_has_permit

* randomly keep a share of control properties
gen u = runiform()
bysort property_id (policy_year): replace u = u[1]
keep if permit | u < `control_share'
drop u county_has_permit

* create fixed effect and cluster ids
egen cell = group(countycode zone policy_year)
egen county_id = group(countycode)

* merge flood county-years back onto the property panel
merge m:1 countycode policy_year using `floods', keep(1 3) nogen


* =============================================================================
* 2 event study
* =============================================================================

* keep treatment cohorts with enough pre and post years
gen es_sample = !permit | inrange(elevation_year, `cohort_first', `cohort_last')

* define treatment cohort and event time, binned at plus or minus the window
gen cohort = elevation_year if permit & es_sample
gen event = policy_year - cohort
replace event = -`window' if event < -`window'
replace event =  `window' if event > `window' & !missing(event)

* cohort by event-time indicators, omitting the year before treatment
levelsof cohort, local(cohorts)
local terms
foreach g of local cohorts {
    forvalues j = 0/`=2 * `window'' {
        if `j' - `window' == `ref_period' continue
        gen d`g'_`j' = cohort == `g' & event == `j' - `window'
        local terms `terms' d`g'_`j'
    }
}

* report sample sizes
egen home_tag = tag(property_id)
egen county_tag = tag(countycode)
quietly count if home_tag & permit & es_sample
di "Permitted homes in event study: " r(N)
quietly count if home_tag & !permit
di "Control homes: " r(N)
quietly count if county_tag
di "Counties: " r(N)

* estimate cohort-specific effects for each outcome, then average them at each event time with the cohorts' sample shares as weights
//define outcomes and results matrix
local outcomes premium claim any_claim
matrix E = J(`=2 * `window' + 1', `=2 * `: word count `outcomes''', 0)
local col = 1

//regression
foreach y of local outcomes {

    local sample if es_sample
    if "`y'" == "premium" local sample if es_sample & premium > 0 & !missing(premium)

    reghdfe `y' `terms' `sample', absorb(property_id cell) vce(cluster county_id)

    forvalues j = 0/`=2 * `window'' {
        if `j' - `window' == `ref_period' continue
        local total = 0
        local expr
        foreach g of local cohorts {
            quietly count if d`g'_`j' & e(sample)
            if r(N) == 0 continue
            local total = `total' + r(N)
            local expr `expr' + `r(N)' * d`g'_`j'
        }
        quietly lincom (`expr') / `total'
        matrix E[`j' + 1, `col']     = r(estimate)
        matrix E[`j' + 1, `col' + 1] = r(se)
    }

    local col = `col' + 2
}

* label and display event-study estimates
local rows
forvalues k = -`window'/`window' {
    local rows `rows' t`k'
}
matrix rownames E = `rows'
matrix colnames E = premium_b premium_se claim_b claim_se any_claim_b any_claim_se
matrix list E


* =============================================================================
* 3 event-study figures
* =============================================================================

local title_premium   "Effect on premium (2023 \$)"
local title_claim     "Effect on paid claim (2023 \$)"
local title_any_claim "Effect on probability of a paid claim"

foreach y of local outcomes {
    preserve
        clear
        svmat E, names(col)
        gen t  = _n - `window' - 1
        gen lo = `y'_b - 1.96 * `y'_se
        gen hi = `y'_b + 1.96 * `y'_se

        * plot estimates and 95 percent confidence intervals
        twoway (rcap lo hi t, lcolor("`blue'")) (scatter `y'_b t, mcolor("`blue'") msymbol(O)), ///
            yline(0, lcolor(gs8)) xline(-0.5, lpattern(dash) lcolor(gs8)) `opts' legend(off) ///
            xtitle("Years from permit") ytitle("`title_`y''") xlabel(-`window'(1)`window')

        graph save   "`output'/figures/es_elevation_`y'.gph", replace
        graph export "`output'/figures/es_elevation_`y'.png", width(2000) replace
    restore
}

* =============================================================================
* 4 event study by elevation source   (09-22)
* =============================================================================

* same specification, treated homes of one source at a time against all never-elevated homes:
* 3 = Builty permit (permit year), 1 = NFIP flag flip (re-rating year), 2 = ICC payment (flood year)
local lab3 permit
local lab1 flip
local lab2 icc
foreach y of local outcomes {
    local panels_`y'
}
foreach s in 3 1 2 {
    quietly count if home_tag & permit & es_sample & elevation_source == `s'
    di "Source `s' (`lab`s''): treated homes in event study: " r(N)

    matrix E`s' = J(`=2 * `window' + 1', `=2 * `: word count `outcomes''', 0)
    local col = 1
    foreach y of local outcomes {
        local sample if es_sample & (!permit | elevation_source == `s')
        if "`y'" == "premium" local sample `sample' & premium > 0 & !missing(premium)

        reghdfe `y' `terms' `sample', absorb(property_id cell) vce(cluster county_id)

        forvalues j = 0/`=2 * `window'' {
            if `j' - `window' == `ref_period' continue
            local total = 0
            local expr
            foreach g of local cohorts {
                quietly count if d`g'_`j' & e(sample)
                if r(N) == 0 continue
                local total = `total' + r(N)
                local expr `expr' + `r(N)' * d`g'_`j'
            }
            if `total' == 0 continue
            quietly lincom (`expr') / `total'
            matrix E`s'[`j' + 1, `col']     = r(estimate)
            matrix E`s'[`j' + 1, `col' + 1] = r(se)
        }
        local col = `col' + 2
    }
    matrix rownames E`s' = `rows'
    matrix colnames E`s' = premium_b premium_se claim_b claim_se any_claim_b any_claim_se
    di "Source `s' (`lab`s'')"
    matrix list E`s'

    * one figure per outcome and source; the three sources are then combined per outcome
    foreach y of local outcomes {
        local lab `lab`s''
        preserve
            clear
            svmat E`s', names(col)
            gen t  = _n - `window' - 1
            gen lo = `y'_b - 1.96 * `y'_se
            gen hi = `y'_b + 1.96 * `y'_se
            twoway (rcap lo hi t, lcolor("`blue'")) (scatter `y'_b t, mcolor("`blue'") msymbol(O)), ///
                yline(0, lcolor(gs8)) xline(-0.5, lpattern(dash) lcolor(gs8)) `opts' legend(off) ///
                xtitle("Years from elevation") ytitle("`title_`y''") xlabel(-`window'(1)`window') ///
                subtitle("`lab'", size(medium) color(gs4)) name(g_`y'_`lab', replace)
            graph save   "`output'/figures/es_elevation_`y'_`lab'.gph", replace
            graph export "`output'/figures/es_elevation_`y'_`lab'.png", width(2000) replace
        restore
        local panels_`y' `panels_`y'' g_`y'_`lab'
    }
}
foreach y of local outcomes {
    graph combine `panels_`y'', rows(1) ycommon xsize(12) ysize(4) graphregion(color(white))
    graph save   "`output'/figures/es_elevation_`y'_by_source.gph", replace
    graph export "`output'/figures/es_elevation_`y'_by_source.png", width(3000) replace
}

log close es_elevation

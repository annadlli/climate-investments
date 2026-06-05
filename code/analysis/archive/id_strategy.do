* id_strategy.do
* Identification strategy analysis
*
* Covers:
*   12.1 RD idea: 50% damage threshold (NFIP Substantial Damage rule)
*   12.2 Application vs approval: subapplications data
*   12.3 BCA-based selection patterns
*
* Key finding from data exploration:
*   - BCA discontinuity at 1.0 has very few obs below threshold (N≈15 with BCA<1),
*     making a formal RD underpowered. BCA near-threshold (0.8-1.2) has ~246 obs.
*   - Elevation applications have a much lower grant rate (43%) vs buyouts (75%),
*     suggesting substantial discretion or HMGP/FMA program differences.
*   - Harvey-related applications (DR-4332) have only 28% grant rate, likely pending.
*   - 50% damage RD is not feasible with current data (requires NFIP claims/damage
*     assessments at the property level).
*
* Inputs:
*   data/fema/txsubapp.dta         (application-level, 2,055 obs)
*   data/fema/hma_tx_projects.dta  (approved project-level, 2,516 obs)
*   data/merged_panel.dta          (county×year panel)

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments"
local data "`root'/data"
local out  "`root'/results"

capture mkdir "`out'"

********************************************************************************
* 12.1 RD: 50% Substantial Damage Threshold
*
* NFIP rule: if repair cost > 50% of pre-damage market value,
* the structure must be brought into compliance (elevation above BFE).
* This creates a discontinuity: properties just above the 50% threshold
* face mandatory elevation; those just below do not.
*
* FEASIBILITY NOTE:
*   A clean RD requires individual property damage estimates relative to
*   market value. This requires NFIP claims data (not currently in hand).
*   The current permit data has job value (elevation cost) and property value,
*   but NOT flood damage amount.
*
* PROXY APPROACH (limited):
*   - JOB_VALUE / prop_value ≈ repair-cost-to-value ratio at time of permit
*   - This is NOT the damage ratio (job value = elevation cost, not damage repair)
*   - Can still show the distribution and flag permits near the 50% threshold
*     as a descriptive exercise — but NOT a valid RD.
*
* WHAT TO DO NEXT FOR 12.1:
*   Obtain NFIP claims data (available from FEMA NFIP policy/claims files or
*   OpenFEMA). Merge to permit addresses. The damage-to-value ratio at the
*   property level is the running variable for the RD.
********************************************************************************

* --- Proxy distribution (descriptive only, not RD) ---
use "`data'/flood_elevation_filters.dta", clear

keep if flood_elev_broad == 1
gen permit_year = year(dofc(PERMIT_DATE))
keep if permit_year >= 2000 & !missing(permit_year)
keep if JOB_VALUE > 0

gen prop_val = TAXMARKETVALUETOTAL
replace prop_val = TAXASSESSEDVALUETOTAL if missing(prop_val)
drop if missing(prop_val) | prop_val <= 0

gen cost_ratio = JOB_VALUE / prop_val
label var cost_ratio "Elevation cost / property value"

* Distribution of cost ratio
tabstat cost_ratio, stat(n mean p10 p25 p50 p75 p90 p99) col(stat)

* Share near/above 50% threshold
gen above_50pct = cost_ratio >= 0.50
tab above_50pct

* Histogram: cost ratio distribution
* (shows where elevation costs fall relative to property value)
twoway histogram cost_ratio if cost_ratio < 1.5, ///
    width(0.05) color(navy%60) lcolor(white) ///
    xline(0.50, lcolor(cranberry) lwidth(medthick) lpattern(dash)) ///
    xtitle("Elevation cost / property value") ///
    ytitle("Density") ///
    title("Distribution of elevation cost relative to property value") ///
    note("Red dashed line = 50% threshold (NFIP Substantial Damage rule)." ///
         "NOTE: job value ≠ damage amount. This is NOT the RD running variable.")
graph export "`out'/fig_cost_ratio_dist.png", replace width(1200)

********************************************************************************
* 12.2 APPLICATION vs APPROVAL: Subapplications Data
*
* txsubapp.dta: 2,055 TX HMA applications (HMGP + FMA)
* got_grant = 1 if application resulted in approval/obligation
* Grant rate: 76% overall; 43% for elevation, 75% for buyouts
*
* Research questions:
*   a) Does BCA predict approval? (Should: FEMA requires BCA >= 1 for HMGP)
*   b) Do elevation applications face higher rejection rates than buyouts?
*   c) Conditional on BCA, are there county-level patterns (capacity, SVI)?
********************************************************************************
use "`data'/fema/txsubapp.dta", clear

* Destring financial variables
foreach v of varlist projectamount federalshareobligated benefitcostratio ///
    netvaluebenefits numberofproperties numberoffinalproperties {
    destring `v', replace force
}

* Project type flags
gen byte elev_flag   = regexm(projecttype, "202\.")
gen byte buyout_flag = regexm(projecttype, "200\.|201\.")
gen str10 proj_cat = "other"
replace proj_cat = "buyout"    if buyout_flag == 1
replace proj_cat = "elevation" if elev_flag   == 1

* Program flags
gen byte hmgp_flag = (programarea == "HMGP")
gen byte fma_flag  = (programarea == "FMA")

* Harvey disaster
gen byte harvey = regexm(disasternumber, "4332")

* -------------------------------------------------------
* 12.2a Grant rate by project type and program
* -------------------------------------------------------
tabstat got_grant, by(proj_cat) stat(n mean) col(stat) nototal
tabstat got_grant, by(programarea) stat(n mean) col(stat) nototal

* Joint
tab proj_cat programarea if inlist(proj_cat, "elevation", "buyout"), row chi2

* -------------------------------------------------------
* 12.2b Logit: got_grant ~ BCA + project type + program
* -------------------------------------------------------
* Drop Harvey-era (likely still pending, artificially low grant rate)
drop if harvey == 1

* Standardize BCA for coefficient comparability
sum benefitcostratio if benefitcostratio > 0 & !missing(benefitcostratio)
gen bca_std = (benefitcostratio - r(mean)) / r(sd) ///
    if benefitcostratio > 0 & !missing(benefitcostratio)
label var bca_std "BCA (standardized)"

* Logit: probability of grant approval
logit got_grant bca_std i.hmgp_flag i.elev_flag i.buyout_flag, robust
estimates store log1
margins, dydx(*) post
estimates store mfx1

* With log project amount
gen log_proj_amt = log(projectamount) if projectamount > 0
logit got_grant bca_std log_proj_amt i.hmgp_flag i.elev_flag i.buyout_flag, robust
estimates store log2
margins, dydx(bca_std log_proj_amt) post
estimates store mfx2

esttab mfx1 mfx2 using "`out'/tab_subapp_approval.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("Marginal effects: probability of grant approval") ///
    mtitles("Baseline" "+Proj. size") ///
    note("Logit marginal effects. Harvey-era apps excluded. Elev. = 202.x, Buyout = 200.x/201.x.")

* -------------------------------------------------------
* 12.2c Approval gap: elevation vs buyout, controlling for BCA
* -------------------------------------------------------
* Does the elevation penalty persist after controlling for BCA?
reg got_grant elev_flag bca_std log_proj_amt i.hmgp_flag, robust
estimates store app_ols

* Are elevation applications systematically lower BCA?
ttest benefitcostratio if inlist(proj_cat,"elevation","buyout"), by(elev_flag)

tabstat benefitcostratio projectamount, ///
    by(proj_cat) stat(n mean p25 p50 p75) col(stat) nototal

********************************************************************************
* 12.3 BCA-BASED SELECTION PATTERNS
*
* BCA (benefit-cost ratio) is FEMA's primary quantitative criterion.
* Projects with BCA < 1 are technically ineligible for HMGP (some exceptions).
*
* Key data facts:
*   - Among non-missing, non-zero BCA: median 1.78, only N≈15 below 1
*   - The threshold provides almost no discontinuity in practice
*     (FEMA may override, or projects with BCA<1 are pre-screened out)
*   - More useful: BCA predicts TYPE of project (elevation vs buyout),
*     and high-BCA projects cluster in certain counties/risk tiers
********************************************************************************

* --- Reload full subapp data ---
use "`data'/fema/txsubapp.dta", clear
foreach v of varlist projectamount federalshareobligated benefitcostratio ///
    netvaluebenefits numberofproperties {
    destring `v', replace force
}
gen byte elev_flag   = regexm(projecttype, "202\.")
gen byte buyout_flag = regexm(projecttype, "200\.|201\.")
gen str10 proj_cat = "other"
replace proj_cat = "buyout"    if buyout_flag == 1
replace proj_cat = "elevation" if elev_flag   == 1

* -------------------------------------------------------
* 12.3a BCA distribution by project type
* -------------------------------------------------------
tabstat benefitcostratio if benefitcostratio > 0, ///
    by(proj_cat) stat(n mean p10 p25 p50 p75 p90 p99) col(stat) nototal

* BCA < 1 vs >= 1: how many in each bucket?
gen bca_group = .
replace bca_group = 1 if benefitcostratio > 0 & benefitcostratio < 1
replace bca_group = 2 if benefitcostratio >= 1 & benefitcostratio < 2
replace bca_group = 3 if benefitcostratio >= 2 & !missing(benefitcostratio)
label define bca_lbl 1 "BCA < 1" 2 "BCA 1-2" 3 "BCA >= 2"
label values bca_group bca_lbl

tabstat got_grant projectamount, ///
    by(bca_group) stat(n mean) col(stat) nototal

* -------------------------------------------------------
* 12.3b BCA RD: grant ~ f(BCA) near threshold of 1.0
*
* NOTE: Very limited variation below BCA=1 (N≈15).
* This RD is underpowered. Present as exploratory only.
* -------------------------------------------------------
* Restrict to near threshold
keep if benefitcostratio > 0 & benefitcostratio <= 5

gen bca_c = benefitcostratio - 1     // center at threshold
gen above = (bca_c >= 0)             // treatment = BCA >= 1

* Visual: local linear fit on each side of threshold
twoway ///
    (lfit got_grant bca_c if above == 0 & bca_c >= -1, ///
        lcolor(cranberry) lwidth(medthick)) ///
    (lfit got_grant bca_c if above == 1 & bca_c <= 3, ///
        lcolor(navy) lwidth(medthick)) ///
    (scatter got_grant bca_c, msymbol(circle) mcolor(gs10) msize(tiny)) ///
    , xline(0, lcolor(black) lwidth(thin)) ///
      xtitle("BCA - 1.0 (0 = eligibility threshold)") ///
      ytitle("Got grant (0/1)") ///
      title("BCA discontinuity: grant approval around BCA = 1") ///
      legend(order(1 "Below threshold" 2 "Above threshold" 3 "Application")) ///
      note("CAUTION: Only ~15 obs with BCA < 1. Insufficient for formal RD.")
graph export "`out'/fig_bca_rd.png", replace width(1200)

* -------------------------------------------------------
* 12.3c BCA selection: what predicts high BCA?
* (Higher BCA = more cost-effective project → more likely approved)
* -------------------------------------------------------
use "`data'/fema/txsubapp.dta", clear
foreach v of varlist projectamount benefitcostratio numberofproperties {
    destring `v', replace force
}
gen byte elev_flag   = regexm(projecttype, "202\.")
gen byte buyout_flag = regexm(projecttype, "200\.|201\.")

keep if benefitcostratio > 0 & !missing(benefitcostratio)
gen log_bca      = log(benefitcostratio)
gen log_proj_amt = log(projectamount) if projectamount > 0
gen log_n_props  = log(numberofproperties + 1)
gen hmgp_flag    = (programarea == "HMGP")

label var log_bca      "Log BCA"
label var log_proj_amt "Log project amount"
label var elev_flag    "Elevation project (202.x)"
label var buyout_flag  "Buyout project (200.x/201.x)"
label var hmgp_flag    "HMGP program (vs FMA/other)"

* OLS: what project characteristics predict high BCA?
reg log_bca elev_flag buyout_flag log_proj_amt log_n_props i.hmgp_flag, robust
estimates store bca_reg

esttab bca_reg using "`out'/tab_bca_determinants.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("What predicts high BCA? (selection into high-quality projects)") ///
    keep(elev_flag buyout_flag log_proj_amt log_n_props hmgp_flag) ///
    stats(N r2, fmt(0 3)) ///
    note("OLS. Outcome: log BCA. Sample: HMA subapplications with BCA > 0.")

********************************************************************************
* NOTES ON IDENTIFICATION STRATEGY GOING FORWARD
*
* 12.1 50% RD — NOT YET FEASIBLE
*   Need: NFIP claims or damage assessment data at property level.
*   Source: OpenFEMA NFIP Claims dataset (public), FEMA BRIC damage reports.
*   Running variable: damage / pre-damage market value.
*   Outcome: elevation permit within 2 years of damage event.
*   This would be the cleanest identification in the paper.
*
* 12.2 Application vs Approval — FEASIBLE, LIMITED POWER
*   Key result: elevation apps have 43% grant rate vs 75% for buyouts.
*   This gap persists after controlling for BCA (suggest program design
*   or capacity constraints drive rejection, not just cost-effectiveness).
*   Interpretation: FEMA prioritizes buyouts over elevations in HMGP,
*   even though elevations have similar or higher BCA.
*
* 12.3 BCA RD — UNDERPOWERED WITH CURRENT DATA
*   Very few obs below BCA = 1. Not viable as formal RD.
*   Better use of BCA: as a control variable for project quality
*   in the approval regression (12.2), or to study selection
*   (do richer counties submit higher-BCA projects?).
********************************************************************************

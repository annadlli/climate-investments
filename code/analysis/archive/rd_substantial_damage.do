* rd_substantial_damage.do
* RD analysis: NFIP 50% Substantial Damage Rule
*
* Design:
*   Running variable: damage_ratio = bldg_damage_amt / bldg_property_value
*   Threshold:        0.50
*   Treatment:        substantially_damaged = (damage_ratio >= 0.50)
*   Outcomes:
*     (1) got_icc     — ICC compliance payment received (first stage / direct test)
*     (2) [future]    — elevation permit within 2 years (requires address linkage)
*
* Intuition:
*   NFIP rules require structures with damage >= 50% of pre-damage market value
*   to be brought into compliance (elevated above BFE) before reconstruction.
*   This creates a sharp discontinuity in the obligation to elevate.
*   ICC is FEMA's payment mechanism ($0-$30K) specifically for this compliance cost.
*   A jump in ICC receipt at damage_ratio = 0.50 validates that the rule is binding.
*
* Sample:
*   Residential claims (occupancy_type 1, 2, 11), Texas, 2000-2023
*   With valid bldg_damage_amt and bldg_property_value
*   Excluding ratio_suspect == 1 (damage_ratio > 2, likely bad property value)
*
* Install RD packages if not already present
foreach pkg in rdrobust rddensity {
    capture which `pkg'
    if _rc != 0 ssc install `pkg', replace
}
*
* Input:  data/fema/nfip_tx_claims.dta

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments"
local data "`root'/data"
local out  "`root'/results"

capture mkdir "`out'"

* -------------------------------------------------------
* 0. Load and sample restriction
* -------------------------------------------------------
use "`data'/fema/nfip_tx_claims.dta", clear

* RD-valid sample: non-missing damage ratio, no extreme outliers
keep if !missing(damage_ratio) & ratio_suspect == 0

* Optional: further restrict to bandwidth around threshold
* (rdrobust will select optimal bandwidth automatically)

count
sum damage_ratio, detail

* -------------------------------------------------------
* 1. DENSITY TEST: McCrary / rddensity
* Null: no manipulation of the running variable around threshold
* If damaged properties are systematically under/over-reported near 0.50,
* the RD is invalid.
* -------------------------------------------------------
rddensity damage_ratio, c(0.50)
* Interpretation: p-value > 0.10 means we fail to reject no manipulation.
* Concern: property owners or adjusters may strategically report damage just
* below 50% to avoid elevation requirements.

* Visual density check
rddensity damage_ratio, c(0.50) plot
graph export "`out'/fig_rd_density.png", replace width(1200)

* -------------------------------------------------------
* 2. FIRST STAGE / REDUCED FORM: ICC receipt at 50% threshold
* Outcome: got_icc (binary — received ICC payment)
* Expected: sharp jump at damage_ratio = 0.50
* -------------------------------------------------------

* 2a. Visual: binned scatter around threshold
* Use damage_ratio_c (centered) for the plot
rdplot got_icc damage_ratio, c(0.50) ///
    nbins(40 40) ///
    graph_options(xtitle("Damage ratio (building damage / property value)") ///
                  ytitle("Probability of ICC payment") ///
                  title("RD: ICC compliance payment at 50% damage threshold") ///
                  xline(0.50, lcolor(cranberry) lpattern(dash)) ///
                  note("ICC = Increased Cost of Compliance (up to $30K for elevation)." ///
                       "Sample: TX residential NFIP claims 2000-2023, damage ratio <= 2."))
graph export "`out'/fig_rd_icc_binscatter.png", replace width(1200)

* 2b. rdrobust: optimal bandwidth, RD estimate
rdrobust got_icc damage_ratio, c(0.50)
* Key output: coefficient on treatment (jump at threshold), SE, p-value, bandwidth

* Store results
local rd_coef  = e(tau_cl)
local rd_se    = e(se_tau_cl)
local rd_bw    = e(h_l)         // left bandwidth (same as right by default)
local rd_n_l   = e(N_h_l)
local rd_n_r   = e(N_h_r)

di "RD estimate (ICC jump at 50%): `rd_coef' (SE = `rd_se')"
di "Optimal bandwidth: `rd_bw'"
di "N in bandwidth: left = `rd_n_l', right = `rd_n_r'"

* 2c. Sensitivity: different bandwidths
foreach bw in 0.05 0.10 0.15 0.20 0.25 {
    rdrobust got_icc damage_ratio, c(0.50) h(`bw')
    di "BW = `bw': coef = " e(tau_cl) ", SE = " e(se_tau_cl)
}

* 2d. Parametric check: OLS with polynomial controls
* (standard RD robustness check)
gen above_50 = (damage_ratio >= 0.50)

* Restrict to [0.30, 0.70] window for parametric
reg got_icc above_50 damage_ratio_c if damage_ratio >= 0.30 & damage_ratio <= 0.70, robust
estimates store rd_linear

reg got_icc above_50 c.damage_ratio_c c.damage_ratio_c#i.above_50 ///
    if damage_ratio >= 0.30 & damage_ratio <= 0.70, robust
estimates store rd_interact

* Quadratic polynomial
gen dmg_c2 = damage_ratio_c^2
reg got_icc above_50 damage_ratio_c dmg_c2 ///
    c.damage_ratio_c#i.above_50 c.dmg_c2#i.above_50 ///
    if damage_ratio >= 0.25 & damage_ratio <= 0.75, robust
estimates store rd_quadratic

esttab rd_linear rd_interact rd_quadratic using "`out'/tab_rd_parametric.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("RD parametric: ICC receipt at 50% damage threshold") ///
    keep(above_50) ///
    stats(N r2, fmt(0 3)) ///
    note("Sample restricted to [0.25, 0.75] damage ratio window. Outcome: got ICC payment.")

* -------------------------------------------------------
* 3. HETEROGENEITY: Does the jump differ by subgroup?
* -------------------------------------------------------

* By flood zone (higher risk zones should have stronger compliance)
rdrobust got_icc damage_ratio if regexm(flood_zone, "^A"), c(0.50)
di "RD estimate (Zone A/AE): " e(tau_cl)

rdrobust got_icc damage_ratio if regexm(flood_zone, "^X"), c(0.50)
di "RD estimate (Zone X/outside SFHA): " e(tau_cl)

* By pre-existing elevation status
rdrobust got_icc damage_ratio if is_elevated == 0, c(0.50)
di "RD estimate (not previously elevated): " e(tau_cl)

* By post-FIRM construction
rdrobust got_icc damage_ratio if post_firm == 1, c(0.50)
di "RD estimate (post-FIRM): " e(tau_cl)

rdrobust got_icc damage_ratio if post_firm == 0, c(0.50)
di "RD estimate (pre-FIRM, more likely below BFE): " e(tau_cl)

* -------------------------------------------------------
* 4. PLACEBO TESTS: False thresholds
* The RD estimate should be zero at false cutoffs (0.30, 0.40, 0.60, 0.70)
* -------------------------------------------------------
foreach placebo_c in 0.30 0.40 0.60 0.70 {
    rdrobust got_icc damage_ratio, c(`placebo_c')
    di "Placebo c = `placebo_c': coef = " e(tau_cl) ", p = " e(pv_cl)
}

* -------------------------------------------------------
* 5. COVARIATE BALANCE CHECKS
* RD assumption: other characteristics should NOT jump at threshold
* (would indicate manipulation or confounding)
* -------------------------------------------------------
local covs "water_depth primary_residence is_elevated post_firm"
foreach v of local covs {
    capture rdrobust `v' damage_ratio, c(0.50)
    if _rc == 0 {
        di "Balance check — `v': coef = " e(tau_cl) ", p = " e(pv_cl)
    }
}

* Visual balance: water depth (should be continuous, no jump)
rdplot water_depth damage_ratio, c(0.50) nbins(30 30) ///
    graph_options(xtitle("Damage ratio") ytitle("Water depth (feet)") ///
                  title("Balance check: water depth at 50% threshold") ///
                  note("Should see no jump — depth is a forcing variable for damage."))
graph export "`out'/fig_rd_balance_depth.png", replace width(1200)

* -------------------------------------------------------
* 6. SECOND STAGE PREVIEW: damage events and elevation permits
* (requires nfip_tx_claims + merged_panel_nfip)
* -------------------------------------------------------
* At the county-year level: does a higher share of substantially damaged claims
* predict more elevation permits in subsequent years?
*
* This is the reduced form of the 2SLS:
*   First stage:  pct_subst_dmgd → HMA projects / ICC payouts
*   Second stage: HMA / ICC → elevation permits
*
* Run from merged_panel_nfip.dta

use "`data'/merged_panel_nfip.dta", clear

encode fips_county, gen(county_id)
xtset county_id year
sort county_id year

gen log_n_permit = log(n_elev_permits + 1)
gen log_claims   = log(n_claims_total + 1)
gen log_damage   = log(total_damage + 1)

* Lagged substantial damage as predictor of future permits
by county_id: gen lag1_subst = n_substantially_dmgd[_n-1]
by county_id: gen lag2_subst = n_substantially_dmgd[_n-2]

gen log_lag1_subst = log(lag1_subst + 1)
gen log_lag2_subst = log(lag2_subst + 1)

label var log_lag1_subst "Log(substantially damaged claims, t-1)"
label var log_lag2_subst "Log(substantially damaged claims, t-2)"

* OLS: permits ~ lagged substantial damage (county + year FE)
areg log_n_permit log_lag1_subst log_lag2_subst ///
    ifl_risk_score log_claims i.year, ///
    absorb(county_id) cluster(county_id)
estimates store panel_subst

* Permits ~ lagged NFIP payouts (total damage signal)
by county_id: gen lag1_payout = total_payout[_n-1]
gen log_lag1_payout = log(lag1_payout + 1)

areg log_n_permit log_lag1_payout ifl_risk_score i.year, ///
    absorb(county_id) cluster(county_id)
estimates store panel_payout

esttab panel_subst panel_payout using "`out'/tab_panel_nfip_permits.rtf", ///
    replace b(3) se(3) star(* 0.10 ** 0.05 *** 0.01) ///
    title("Elevation permits ~ lagged NFIP substantial damage (county + year FE)") ///
    mtitles("Subst. damaged claims" "Total NFIP payout") ///
    keep(log_lag1_subst log_lag2_subst log_lag1_payout ifl_risk_score log_claims) ///
    stats(N r2, fmt(0 3)) ///
    note("County + year FE. SE clustered by county. Outcome: log(elevation permits + 1).")

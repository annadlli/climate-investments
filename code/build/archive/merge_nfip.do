clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments/data"

*******************************************************
* merge_nfip.do
* Merge NFIP county-year data onto merged_panel.dta
*
* Input:
*   `root'/merged_panel.dta
*   `root'/fema/nfip_tx_countyyear.dta
*
* Output:
*   `root'/merged_panel_nfip.dta
*
* Notes:
* - Merge is 1:1 on fips_county x year
* - NFIP file only contains county-years with claims/activity
* - Unmatched panel rows are set to zero for NFIP flow variables
*******************************************************

* -------------------------------------------------------
* 1. Load existing panel
* -------------------------------------------------------
use "`root'/merged_panel.dta", clear
isid fips_county year

* -------------------------------------------------------
* 2. Check NFIP file uniqueness before merge
* -------------------------------------------------------
preserve
use "`root'/fema/nfip_tx_countyyear.dta", clear
isid fips_county year
restore

* -------------------------------------------------------
* 3. Merge NFIP onto panel
* -------------------------------------------------------
merge 1:1 fips_county year using "`root'/fema/nfip_tx_countyyear.dta", ///
    keep(master match) nogen

* Drop NFIP variables you do not want to keep
drop n_got_icc pct_post_firm n_harvey n_ike n_allison pct_got_icc total_icc_payout

* Fill zeros for county-years with no NFIP claims/activity
foreach v of varlist n_claims n_claims_total n_substantially_dmgd total_damage total_payout {
    replace `v' = 0 if missing(`v')
}

* Recompute logs if they were missing from unmatched rows
replace log_total_damage = log(total_damage + 1) if missing(log_total_damage)
replace log_total_payout = log(total_payout + 1) if missing(log_total_payout)

* -------------------------------------------------------
* 4. Derived NFIP variables
* -------------------------------------------------------

* Claim intensity per 1,000 residents
gen claims_per_1000 = (n_claims_total / population) * 1000 if population > 0
label var claims_per_1000 "NFIP claims per 1,000 residents"

* Damage per capita
gen damage_per_cap = total_damage / population if population > 0
label var damage_per_cap "Total NFIP building damage per capita ($)"

* Payout per capita
gen payout_per_cap = total_payout / population if population > 0
label var payout_per_cap "Total NFIP building payout per capita ($)"

* Share of claims that are substantially damaged
gen pct_subst_dmgd = (n_substantially_dmgd / n_claims_total) * 100 if n_claims_total > 0
replace pct_subst_dmgd = 0 if n_claims_total == 0
label var pct_subst_dmgd "% of NFIP claims with substantial damage"

* Any substantial-damage claim this year
gen byte any_subst_dmgd = (n_substantially_dmgd > 0)
label var any_subst_dmgd "Any substantial-damage NFIP claim this year"

* -------------------------------------------------------
* 5. Basic checks
* -------------------------------------------------------

di "Merged panel size: " _N

di "County-years with any NFIP claims:"
count if n_claims_total > 0
di r(N)

di "County-years with any substantial-damage claim:"
count if n_substantially_dmgd > 0
di r(N)

* Internal consistency checks
di "Check: n_claims <= n_claims_total"
count if n_claims > n_claims_total
di r(N)

di "Check: n_substantially_dmgd <= n_claims_total"
count if n_substantially_dmgd > n_claims_total
di r(N)

* Summary statistics
tabstat n_claims n_claims_total n_substantially_dmgd total_damage total_payout ///
        claims_per_1000 damage_per_cap payout_per_cap pct_subst_dmgd, ///
        stat(n mean p50 p99) col(stat)

* NFIP claims by year
tabstat n_claims_total, by(year) stat(sum mean) col(stat)

* Correlations with mitigation activity
pwcorr n_claims_total n_substantially_dmgd ///
       n_elev_total_fema n_buyout_total_fema ///
       n_permits_builty, sig

* -------------------------------------------------------
* 6. Order and save
* -------------------------------------------------------
order fips_county county_name year ///
      n_permits_builty n_resid_permits ///
      n_elev_total_fema n_buyout_total_fema ///
      n_elev_hmgp n_elev_fma n_buyout_hmgp n_buyout_fma ///
      fema_elev fema_buyout fema_elev_hmgp fema_elev_fma ///
      fema_buyout_hmgp fema_buyout_fma ///
      n_claims n_claims_total n_substantially_dmgd ///
      total_damage total_payout ///
      log_total_damage log_total_payout ///
      claims_per_1000 damage_per_cap payout_per_cap pct_subst_dmgd any_subst_dmgd

sort fips_county year
save "`root'/merged_panel_nfip.dta", replace

describe
local k = r(k)
di "Saved: merged_panel_nfip.dta (" _N " obs, " `k' " variables)"

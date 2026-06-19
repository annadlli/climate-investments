/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-05-29

Description: Prepares the NFIP claims data and collapses to county x year. (The
    full claims-to-property merge uses the Wagner tiered-cell method, applied in
    build_nfip_hma_panels.do off the NFIP policies file.)

NOTE (needs reconciling with Anna): this reads clean/nfip_claims.dta and expects
    variables (damage_ratio, substantially_damaged, got_icc, bldg_damage_amt,
    bldg_claim_paid, icc_amount) that clean_nfip_claims.do does NOT currently
    produce, and its county-year output is not consumed by
    build_nfip_hma_panels.do -- so it will not run as-is. Left in place pending
    reconciliation; paths parametrized, logic unchanged.
******************************************************************************/

args data

use "`data'/clean/nfip_claims.dta", clear
* Flag SFHA and eliminate them from sample
*note: will use ratedfloodzone as measure, as that was how NFIP was actually used to price and rate
*SFHA determined as FEMA describes: https://www.fema.gov/about/glossary/special-flood-hazard-area-sfha
gen sfha = 0

replace sfha = 1 if ///
    regexm(ratedfloodzone, "^A[0-9][0-9]?$") | ///
    regexm(ratedfloodzone, "^V[0-9][0-9]?$")

replace sfha = 1 if inlist(ratedfloodzone, ///
    "A","AE","AH","AHB","AO","AOB")

replace sfha = 1 if inlist(ratedfloodzone, ///
    "A99","AR","V","VE")

replace sfha = 1 if inlist(ratedfloodzone, ///
    "AR/A","AR/AE","AR/AH","AR/AO","AR/A1-A30")
		
label define sfha_lbl 0 "Non-SFHA" 1 "SFHA"
label values sfha sfha_lbl
tab ratedfloodzone sfha, missing

drop if sfha ==1
* -------------------------------------------------------
* 9. Collapse to county × year (for panel merge)
* -------------------------------------------------------
* Helper for total-row count (can't use by-variables as collapse sources)
gen byte one = 1

collapse ///
    (count)  n_claims              = damage_ratio  ///
    (sum)    n_claims_total        = one           ///
    (sum)    n_substantially_dmgd  = substantially_damaged ///
    (sum)    n_got_icc             = got_icc              ///
    (sum)    total_damage          = bldg_damage_amt      ///
    (sum)    total_payout          = bldg_claim_paid      ///
    (sum)    total_icc_payout      = icc_amount           ///
    (mean)   avg_damage_ratio      = damage_ratio         ///
    (mean)   avg_water_depth       = water_depth          ///
    (mean)   pct_post_firm         = post_firm            ///
    (mean)   pct_elevated          = is_elevated          ///
    (mean)   pct_primary_res       = primary_residence    ///
    (sum)    n_harvey              = event_harvey         ///
    (sum)    n_ike                 = event_ike            ///
    (sum)    n_allison             = event_allison        ///
    , by(fips_county year)

* Derived
gen pct_substantially_dmgd = n_substantially_dmgd / n_claims * 100 ///
    if n_claims > 0
gen pct_got_icc = n_got_icc / n_claims * 100 ///
    if n_claims > 0
gen log_total_damage  = log(total_damage + 1)
gen log_total_payout  = log(total_payout + 1)

label var n_claims              "NFIP claims with valid damage ratio"
label var n_claims_total        "Total residential NFIP claims"
label var n_substantially_dmgd  "Claims with damage ratio >= 0.50"
label var n_got_icc             "Claims with ICC payment (substantial damage compliance)"
label var total_damage          "Total building damage amount ($)"
label var total_payout          "Total building claim payout ($)"
label var total_icc_payout      "Total ICC compliance payments ($)"
label var avg_damage_ratio      "Mean damage ratio (damage / property value)"
label var avg_water_depth       "Mean flood water depth (feet)"
label var pct_post_firm         "Share of claims: post-FIRM construction"
label var pct_elevated          "Share of claims: building already elevated"
label var pct_substantially_dmgd "% claims with damage ratio >= 0.50"
label var pct_got_icc           "% claims with ICC payment"
label var log_total_damage      "Log(total building damage + 1)"
label var log_total_payout      "Log(total building payout + 1)"
label var n_harvey              "NFIP claims from Hurricane Harvey (DR-4332)"
label var n_ike                 "NFIP claims from Hurricane Ike (DR-1791)"
label var n_allison             "NFIP claims from Tropical Storm Allison (DR-1379)"

* Inspect
describe
tabstat n_claims n_substantially_dmgd n_got_icc total_damage total_payout, ///
    stat(n mean p50 p99) col(stat)

tab year

* -------------------------------------------------------
* 10. Save county × year file
* -------------------------------------------------------
sort fips_county year
save "`data'/build/nfip_tx_countyyear.dta", replace

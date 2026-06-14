* nfip_clean.do
* Clean FEMA NFIP Claims (Texas) and produce two analysis files:
*
*   1. data/fema/nfip_tx_claims.dta     — property-level (residential, 2000-2023)
*                                          for RD analysis around 50% damage threshold
*
*   2. data/fema/nfip_tx_countyyear.dta — county × year aggregates
*                                          for merging onto merged_panel.dta
*
* Prerequisite: python3 code/build/nfip_prep.py
*
* Key variables:
*   damage_ratio   = bldg_damage_amt / bldg_property_value
*                    RD running variable; threshold = 0.50 (NFIP substantial damage rule)
*   substantially_damaged = damage_ratio >= 0.50
*   got_icc        = icc_paid > 0
*                    ICC payment is FEMA's mechanism for substantial-damage compliance
*   icc_amount     = icc_paid (up to $30K toward elevation/compliance costs)
*
* Merge key: fips_county (str5), year_of_loss → year

* -------------------------------------------------------
* 0. Load
* -------------------------------------------------------
use "/Users/anna/Desktop/Research/climate-investments/data/fema/nfip_tx_prepped.dta", clear
// ~393K Texas claims, 1978-2026

* -------------------------------------------------------
* 1. Build county FIPS merge key
* -------------------------------------------------------
* fips_raw is a float like 48201 — zero-pad to str5
* Drop the 2,132 obs with missing countyCode (no FIPS → unusable for county analysis)
drop if missing(fips_raw)

tostring fips_raw, gen(sf) format("%05.0f")
gen str5 fips_county = sf
assert length(fips_county) == 5
drop sf fips_raw
label var fips_county "5-digit county FIPS, merge key"

* -------------------------------------------------------
* 2. Year variable
* -------------------------------------------------------
rename year_of_loss year
label var year "Year of loss (from yearOfLoss)"

* -------------------------------------------------------
* 3. Filter: Texas residential, 2000-2023
* -------------------------------------------------------
* occupancy_type codes:
*   1  = Single-family residential
*   2  = 2-4 family residential
*   11 = Residential condo (individual unit)
*   Others: commercial, industrial, etc.
keep if inlist(occupancy_type, 1, 2, 11)
keep if year >= 2000 & year <= 2023
// ~185K obs

* -------------------------------------------------------
* 4. Core damage variables and RD running variable
* -------------------------------------------------------

* Damage ratio: repair cost relative to property value
* This is the NFIP Substantial Damage running variable
gen damage_ratio = bldg_damage_amt / bldg_property_value ///
    if bldg_damage_amt > 0 & bldg_property_value > 0 & !missing(bldg_damage_amt, bldg_property_value)
label var damage_ratio "Building damage / property value (RD running variable)"

* Cap outliers: ratio > 2 is almost certainly a bad property value
* (we keep but flag; drop in RD sample)
gen byte ratio_suspect = (damage_ratio > 2) if !missing(damage_ratio)
label var ratio_suspect "Damage ratio > 2 (likely bad property value, exclude from RD)"

* Substantial damage indicator (50% rule)
gen byte substantially_damaged = (damage_ratio >= 0.50) if !missing(damage_ratio)
label var substantially_damaged "damage_ratio >= 0.50 (NFIP substantial damage threshold)"

* ICC indicator and amount
* ICC = Increased Cost of Compliance — FEMA pays up to $30K for elevation/compliance
* after a substantial damage determination
gen byte got_icc    = (icc_paid > 0) if !missing(icc_paid)
gen double icc_amount = icc_paid
replace icc_amount = 0 if missing(icc_amount)
label var got_icc    "ICC payment received (substantial damage compliance)"
label var icc_amount "ICC payment amount ($, max $30K)"

* Total payout
gen double total_payout = bldg_claim_paid
replace total_payout = 0 if missing(total_payout)
label var total_payout "Amount paid on building claim ($)"

* Centered running variable (for RD regressions)
gen damage_ratio_c = damage_ratio - 0.50
label var damage_ratio_c "Damage ratio - 0.50 (centered at threshold)"

* -------------------------------------------------------
* 5. Flood zone and building characteristics
* -------------------------------------------------------
* Post-FIRM: built after flood map effective date — should be elevated
label var post_firm  "Post-FIRM construction (built to elevation standards)"
label var is_elevated "Building already elevated above grade"

* Elevation difference: positive = floor above BFE (compliant), negative = below
label var elev_difference "Lowest floor - BFE (+ = above, - = below)"

* Single-family flag
gen byte single_family = (occupancy_type == 1)
label var single_family "Single-family residential (occupancy_type == 1)"

* Primary residence
label var primary_residence "Primary residence indicator"

* -------------------------------------------------------
* 6. Major disaster events
* -------------------------------------------------------
* Flag key Texas flood disasters for analysis
* Harvey (2017): DR-4332
* Imelda (2019): DR-4459
* Tax Day / Memorial Day (2016/2015): no single DR
* Allison (2001): DR-1379

gen byte event_harvey  = (year == 2017) & regexm(disaster_number, "4332")
gen byte event_imelda  = (year == 2019) & regexm(disaster_number, "4459")
gen byte event_ike     = (year == 2008) & regexm(disaster_number, "1791")
gen byte event_allison = (year == 2001) & regexm(disaster_number, "1379")

label var event_harvey  "Hurricane Harvey (DR-4332, 2017)"
label var event_imelda  "Tropical Storm Imelda (DR-4459, 2019)"
label var event_ike     "Hurricane Ike (DR-1791, 2008)"
label var event_allison "Tropical Storm Allison (DR-1379, 2001)"

* -------------------------------------------------------
* 7. Inspect
* -------------------------------------------------------
describe

* Claims with valid damage ratio
count if !missing(damage_ratio)
count if !missing(damage_ratio) & ratio_suspect == 0

tabstat damage_ratio if ratio_suspect == 0, ///
    stat(n mean p10 p25 p50 p75 p90 p99) col(stat)

* ICC / substantial damage
tab substantially_damaged got_icc if !missing(damage_ratio) & ratio_suspect == 0

* Observations near the threshold
count if damage_ratio >= 0.40 & damage_ratio <= 0.60 & ratio_suspect == 0

* Year distribution
tab year

* -------------------------------------------------------
* 8. Save property-level file (for RD)
* -------------------------------------------------------
sort fips_county year
save "/Users/anna/Desktop/Research/climate-investments/data/fema/nfip_tx_claims.dta", replace

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
save "/Users/anna/Desktop/Research/climate-investments/data/fema/nfip_tx_countyyear.dta", replace

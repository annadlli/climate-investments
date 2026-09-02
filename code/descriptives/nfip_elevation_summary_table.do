/******************************************************************************
Authors: Anna Li
Date: 2026-09-01

Description: Reusable summary table for the NFIP/elevation pipeline.
    The goal is to keep iterating on a compact long-form table without locking
    the output into a final presentation matrix. For now it covers the base NFIP
    sample, the year distribution, states, elevation prevalence, costs, premiums,
    and claims.
******************************************************************************/

args data output

* This is the compact table we keep iterating on: sample size, year range,
* states, elevation prevalence, premiums, claims, and permit cost.
local outfile "`output'/descriptives/nfip_elevation_summary_table"

tempfile table
tempname posth
postfile `posth' int row_order str24 section str52 measure ///
    str40 universe str20 statistic double value value_real2023 ///
    numerator denominator str244 note using "`table'", replace

* -----------------------------------------------------------------------------
* 1. Household sample and year/state coverage
* -----------------------------------------------------------------------------
* This is the base of the table: how big the property sample is, and what years/states it covers.
use "`data'/analysis/analysis_no_diagnostics.dta", clear
isid property_id

* Household proxy for now: one row in the final property file = one policyholder.
count
local households = r(N)
post `posth' (1) ("Sample") ("NFIP policyholder properties") ///
    ("NFIP properties") ("Count") (`households') (.) (.) (.) ///
    ("One row = one policyholder household proxy until we build a proper panel")

* First observed policy year gives the sample entry-cohort window.
summarize policy_year_init, meanonly
local first_year = r(min)
local last_year = r(max)
post `posth' (2) ("Sample") ("First observed policy year") ///
    ("NFIP properties") ("Minimum") (`first_year') (.) (.) (.) ///
    ("Earliest policy year in the property-level sample")
post `posth' (3) ("Sample") ("First observed policy year") ///
    ("NFIP properties") ("Maximum") (`last_year') (.) (.) (.) ///
    ("Latest policy year in the property-level sample")

* The sample is multi-state; this is a simple count of distinct state codes.
levelsof state, local(state_list)
local state_count : word count `state_list'
post `posth' (4) ("Sample") ("States") ///
    ("NFIP properties") ("Count") (`state_count') (.) (.) (.) ///
    ("Distinct states represented in the property sample")

* -----------------------------------------------------------------------------
* 2. Elevation prevalence
* -----------------------------------------------------------------------------
generate byte observed_elevation = builty_elevated == 1
count if observed_elevation
local elevated_n = r(N)
local elevated_share = `elevated_n' / `households'

post `posth' (10) ("Elevations") ("Observed elevation") ///
    ("All NFIP properties") ("Count") (`elevated_n') (.) (.) (.) ///
    ("Properties with a Builty elevation flag after ATTOM matching")
post `posth' (11) ("Elevations") ("Observed elevation prevalence") ///
    ("All NFIP properties") ("Share") (`elevated_share') ///
    (.) (`elevated_n') (`households') ///
    ("Unmatched properties stay in the denominator as no observed elevation")

* When the elevations happen, relative to Risk Rating 2.0 (effective 2021).
summarize builty_elevation_year if observed_elevation, detail
post `posth' (13) ("Elevations") ("Elevation year") ///
    ("Properties with an observed elevation") ("Median") (r(p50)) (.) (.) (.) ///
    ("Earliest Builty permit year at the assigned ATTOM property")
count if observed_elevation & builty_elevation_year >= 2021
local post_rr2 = r(N)
post `posth' (14) ("Elevations") ("Elevated on or after 2021") ///
    ("Properties with an observed elevation") ("Share") (`post_rr2'/`elevated_n') (.) ///
    (`post_rr2') (`elevated_n') ///
    ("Share of observed elevations occurring after Risk Rating 2.0 took effect")

count if attom_matched == 1
local matched_n = r(N)
count if attom_matched == 1 & observed_elevation
local matched_elevated_n = r(N)
local matched_elevated_share = `matched_elevated_n' / `matched_n'
post `posth' (12) ("Elevations") ("Observed elevation prevalence") ///
    ("ATTOM-matched NFIP properties") ("Share") (`matched_elevated_share') ///
    (.) (`matched_elevated_n') (`matched_n') ///
    ("Same prevalence restricted to properties that got an ATTOM match")

* -----------------------------------------------------------------------------
* 3. Premiums
* -----------------------------------------------------------------------------
* Two premiums per property, but only the first observed premium is safe to deflate here.
*   nfip_premium_init  first observed policy year. Comes from the same row as every
*                      _init matching key, and its year is known (policy_year_init),
*                      so it can be deflated to 2023 dollars.
*   nfip_premium       last observed policy year. NOT reported here: its year is not
*                      retained so it cannot be deflated, and only ~8% of properties
*                      are still in the panel at the end, so for the rest it records
*                      the premium at exit -- and exit is plausibly a response to price.
*
* Everything in this table is in 2023 dollars: premiums deflated here, claims deflated
* per claim at year of loss in clean_nfip_claims.do, elevation costs in clean_builty.do.

* CPI is keyed on year; rename so it merges onto the first policy year.
preserve
    use "`data'/clean/cpi.dta", clear
    rename year policy_year_init
    tempfile cpi_by_init
    save `cpi_by_init', replace
restore
merge m:1 policy_year_init using `cpi_by_init', keepusing(cpi) keep(1 3) nogen
generate double nfip_premium_init_r = nfip_premium_init / cpi

summarize nfip_premium_init, detail
local pi_mean = r(mean)
local pi_p50  = r(p50)
local pi_p25  = r(p25)
local pi_p75  = r(p75)
summarize nfip_premium_init_r, detail
post `posth' (15) ("Premiums") ("NFIP premium, first observed year") ///
    ("All NFIP properties") ("Mean") (`pi_mean') (r(mean)) (.) (.) ///
    ("Premium in the property's first observed policy year; real column deflated with clean/cpi.dta")
post `posth' (16) ("Premiums") ("NFIP premium, first observed year") ///
    ("All NFIP properties") ("Median") (`pi_p50') (r(p50)) (.) (.) ///
    ("Premium in the property's first observed policy year")
post `posth' (17) ("Premiums") ("NFIP premium, first observed year") ///
    ("All NFIP properties") ("25th percentile") (`pi_p25') (r(p25)) (.) (.) ///
    ("Premium in the property's first observed policy year")
post `posth' (18) ("Premiums") ("NFIP premium, first observed year") ///
    ("All NFIP properties") ("75th percentile") (`pi_p75') (r(p75)) (.) (.) ///
    ("Premium in the property's first observed policy year")

* Repricing (last minus first) is deliberately omitted: it can only be built in
* nominal terms, since nfip_premium's year is not retained and the change can be
* neither deflated nor annualized. Restore once policy_year_last exists.

* -----------------------------------------------------------------------------
* 4. Claims
* -----------------------------------------------------------------------------
generate byte any_claim = claim_cb > 0 & !missing(claim_cb)
count if any_claim
local claimant_n = r(N)
local claimant_share = `claimant_n' / `households'

post `posth' (30) ("Claims") ("Properties with positive claims") ///
    ("All NFIP properties") ("Count") (`claimant_n') (.) (.) (.) ///
    ("Positive cumulative net building-plus-contents claims")
post `posth' (31) ("Claims") ("Properties with positive claims") ///
    ("All NFIP properties") ("Share") (`claimant_share') ///
    (.) (`claimant_n') (`households') ///
    ("Positive cumulative net building-plus-contents claims")

summarize claim_cb, detail
post `posth' (32) ("Claims") ("Cumulative net claims") ///
    ("All NFIP properties") ("Mean") (r(mean)) (.) (.) (.) ///
    ("Zeros included; 2023 dollars, deflated per claim at year of loss in clean_nfip_claims.do")

summarize claim_cb if any_claim, detail
post `posth' (33) ("Claims") ("Cumulative net claims") ///
    ("Properties with positive claims") ("Mean") (r(mean)) (.) (.) (.) ///
    ("2023 dollars; building plus contents, deflated per claim at year of loss")
post `posth' (34) ("Claims") ("Cumulative net claims") ///
    ("Properties with positive claims") ("Median") (r(p50)) (.) (.) (.) ///
    ("2023 dollars; building plus contents, deflated per claim at year of loss")

* Repetitive-loss designations: the properties the FMA grant program prioritises.
foreach v in nfip_rl nfip_srl {
    count if `v' == 1
    local n_`v' = r(N)
}
post `posth' (35) ("Claims") ("Repetitive loss (RL)") ///
    ("All NFIP properties") ("Count") (`n_nfip_rl') (.) (.) (.) ///
    ("FEMA repetitive-loss designation carried from the multiple-loss file")
post `posth' (36) ("Claims") ("Severe repetitive loss (SRL)") ///
    ("All NFIP properties") ("Count") (`n_nfip_srl') (.) (.) (.) ///
    ("FEMA severe repetitive-loss designation; the FMA priority group")

* Claims relative to what the house is worth -- the motivating statistic.
generate double claim_to_value = claim_cb / attom_market_value_total ///
    if any_claim & attom_market_value_total > 0 & !missing(attom_market_value_total)
summarize claim_to_value, detail
local ctv_n = r(N)
post `posth' (37) ("Claims") ("Claims divided by market value") ///
    ("Claimants with an ATTOM value") ("Median") (r(p50)) (.) (.) (.) ///
    ("Cumulative claims over ATTOM total market value")
count if claim_to_value > 1 & !missing(claim_to_value)
post `posth' (38) ("Claims") ("Paid more in claims than market value") ///
    ("Claimants with an ATTOM value") ("Share") (r(N)/`ctv_n') (.) ///
    (r(N)) (`ctv_n') ///
    ("Claims exceed the property's assessed market value")

* -----------------------------------------------------------------------------
* 5. Elevation cost at the permit level
* -----------------------------------------------------------------------------
* The property file does not carry cost, so switch to the permit file for the cost rows.
use "`data'/clean/builty_elevations.dta", clear

* Permit file has the actual project cost, so switch over here for the cost rows.
count
local permit_n = r(N)
count if !missing(project_value) & project_value > 0
local cost_n = r(N)

post `posth' (40) ("Elevation cost") ("Observed elevation properties") ///
    ("Builty elevation properties") ("Count") (`permit_n') (.) (.) (.) ///
    ("One row per address after clean_builty.do collapses; 9,853 properties cover 13,387 permits")
post `posth' (41) ("Elevation cost") ("Properties reporting project cost") ///
    ("Builty elevation properties") ("Count") (`cost_n') (.) (.) (.) ///
    ("Positive nonmissing project_value")

summarize project_value if project_value > 0, detail
post `posth' (42) ("Elevation cost") ("Elevation project cost") ///
    ("Properties reporting project cost") ("Mean") (r(mean)) (.) (.) (.) ///
    ("(max) project_value across permits at the property, 2023 dollars -- not necessarily the elevation permit")
post `posth' (43) ("Elevation cost") ("Elevation project cost") ///
    ("Properties reporting project cost") ("Median") (r(p50)) (.) (.) (.) ///
    ("(max) project_value across permits at the property, 2023 dollars -- not necessarily the elevation permit")
post `posth' (44) ("Elevation cost") ("Elevation project cost") ///
    ("Properties reporting project cost") ("25th percentile") (r(p25)) (.) (.) (.) ///
    ("(max) project_value across permits at the property, 2023 dollars -- not necessarily the elevation permit")
post `posth' (45) ("Elevation cost") ("Elevation project cost") ///
    ("Properties reporting project cost") ("75th percentile") (r(p75)) (.) (.) (.) ///
    ("(max) project_value across permits at the property, 2023 dollars -- not necessarily the elevation permit")

postclose `posth'

* -----------------------------------------------------------------------------
* 6. Save and export
* -----------------------------------------------------------------------------
* Final step: save the long-form table and throw a quick Excel output.
use "`table'", clear
sort row_order

label data "NFIP household, elevation, premium, costs, and claim summary"
label var row_order "Table row"
label var section "Section"
label var measure "Measure"
label var universe "Estimation universe"
label var statistic "Statistic"
label var value "Value"
label var value_real2023 "Value (2023 $)"
label var numerator "Rate numerator"
label var denominator "Rate denominator"
label var note "Definition or caveat"

format value value_real2023 numerator denominator %15.2fc
save "`outfile'.dta", replace
export excel using "`outfile'.xlsx", sheet("Summary") ///
    firstrow(varlabels) replace

* Final formatting polish for Excel output.
local excel_last = _N + 1
putexcel set "`outfile'.xlsx", sheet("Summary") modify
putexcel A1:J1, bold font("Arial", 10, "white") ///
    fpattern("solid", "navy") hcenter vcenter txtwrap
putexcel A2:J`excel_last', font("Arial", 10, "black") vcenter
putexcel A2:A`excel_last', right nformat("#,##0")
putexcel F2:F`excel_last', right
putexcel H2:I`excel_last', right nformat("#,##0")
putexcel J2:J`excel_last', txtwrap

forvalues i = 1/`=_N' {
    local row = `i' + 1
    local stat = statistic[`i']
    local sect = section[`i']

    if "`stat'" == "Share" {
        putexcel F`row', nformat("0.00%")
    }
    else if inlist("`sect'", "Premiums", "Claims", "Elevation cost") & ///
        inlist("`stat'", "Mean", "Median", "25th percentile", "75th percentile") {
        putexcel F`row', nformat("$#,##0")
        putexcel G`row', nformat("$#,##0")
    }
    else {
        putexcel F`row', nformat("#,##0")
    }
}

di as result "Saved: `outfile'.dta"
di as result "Saved: `outfile'.xlsx"

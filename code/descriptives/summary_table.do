/******************************************************************************
Authors: Anna Li
Date: 2026-09-01

Description: Reusable summary table for the NFIP/elevation pipeline. Long form,
    one row per statistic, so rows can be added without redesigning the table.

    Everything is in 2023 dollars: premiums deflated here on policy_year_init,
    claims per claim at year of loss in clean_nfip_claims.do, elevation costs in
    clean_builty.do.
******************************************************************************/

args data output

local outfile "`output'/descriptives/summary_table"

tempfile table
tempname results
postfile `results' int row_order str24 section str52 measure ///
    str40 universe str20 statistic double value value_real2023 ///
    numerator denominator str244 note using "`table'", replace

* -----------------------------------------------------------------------------
* Helpers. Each kind of row is written once here and called with its labels.
* -----------------------------------------------------------------------------

* A distribution in three rows: mean, standard error, count.
* real() names a deflated twin of the variable and fills the 2023-dollar column.
capture program drop postdist
program define postdist
    syntax varname(numeric) [if], results(name) ROW(int) SECTION(string) ///
        MEASURE(string) UNIVERSE(string) [REAL(varname numeric) NOTE(string)]

    summarize `varlist' `if', detail
    local n = r(N)
    local m = r(mean)
    local se = r(sd) / sqrt(`n')

    local r_n = .
    local r_m = .
    local r_se = .
    if "`real'" != "" {
        summarize `real' `if', detail
        local r_n = r(N)
        local r_m = r(mean)
        local r_se = r(sd) / sqrt(r(N))
    }

    post `results' (`row') ("`section'") ("`measure'") ("`universe'") ///
        ("Mean") (`m') (`r_m') (.) (.) ("`note'")
    post `results' (`row' + 1) ("`section'") ("`measure'") ("`universe'") ///
        ("Standard error") (`se') (`r_se') (.) (.) ("`note'")
    post `results' (`row' + 2) ("`section'") ("`measure'") ("`universe'") ///
        ("Count") (`n') (.) (.) (.) ("`note'")
end

* Count
capture program drop postcount
program define postcount
    syntax , RESULTS(name) ROW(int) SECTION(string) MEASURE(string) ///
        UNIVERSE(string) VALUE(real) [NOTE(string)]
    post `results' (`row') ("`section'") ("`measure'") ("`universe'") ///
        ("Count") (`value') (.) (.) (.) ("`note'")
end

* Share, carrying its numerator and denominator so the rate is auditable
capture program drop postshare
program define postshare
    syntax , RESULTS(name) ROW(int) SECTION(string) MEASURE(string) ///
        UNIVERSE(string) NUM(real) DEN(real) [NOTE(string)]
    post `results' (`row') ("`section'") ("`measure'") ("`universe'") ///
        ("Share") (`num' / `den') (.) (`num') (`den') ("`note'")
end

* -----------------------------------------------------------------------------
* 1. Sample
* -----------------------------------------------------------------------------
use "`data'/analysis/analysis_no_diagnostics.dta", clear
isid property_id

count
local households = r(N)
postcount, results(`results') row(1) section("Sample") ///
    measure("NFIP policyholder properties") universe("NFIP properties") ///
    value(`households') ///
    note("One row = one policyholder household proxy until we build a proper panel")

summarize policy_year_init, meanonly
post `results' (2) ("Sample") ("First observed policy year") ///
    ("NFIP properties") ("Minimum") (r(min)) (.) (.) (.) ///
    ("Entry-cohort window, not the span of the policy panel")
post `results' (3) ("Sample") ("First observed policy year") ///
    ("NFIP properties") ("Maximum") (r(max)) (.) (.) (.) ///
    ("Entry-cohort window, not the span of the policy panel")

levelsof state, local(state_list)
local state_count : word count `state_list'
postcount, results(`results') row(4) section("Sample") measure("States") ///
    universe("NFIP properties") value(`state_count') ///
    note("Distinct states represented in the property sample")

* -----------------------------------------------------------------------------
* 2. Elevation prevalence
* -----------------------------------------------------------------------------
generate byte observed_elevation = builty_elevated == 1
count if observed_elevation
local elevated_n = r(N)

postcount, results(`results') row(10) section("Elevations") ///
    measure("Observed elevation") universe("All NFIP properties") ///
    value(`elevated_n') ///
    note("Properties with a Builty elevation flag")
postshare, results(`results') row(11) section("Elevations") ///
    measure("Observed elevation prevalence") universe("All NFIP properties") ///
    num(`elevated_n') den(`households') ///
    note("Share of all NFIP properties with an observed elevation")

summarize builty_elevation_year if observed_elevation, detail
post `results' (13) ("Elevations") ("Elevation year") ///
    ("Properties with an observed elevation") ("Median") (r(p50)) (.) (.) (.) ///
    ("Median Builty permit year for observed elevations")

count if observed_elevation & builty_elevation_year >= 2021
postshare, results(`results') row(14) section("Elevations") ///
    measure("Elevated on or after 2021") ///
    universe("Properties with an observed elevation") ///
    num(`=r(N)') den(`elevated_n') ///
    note("Share of observed elevations after Risk Rating 2.0 took effect")

* -----------------------------------------------------------------------------
* 3. Premiums
* -----------------------------------------------------------------------------
* CPI is keyed on year; rename so it merges onto the first policy year.
preserve
    use "`data'/clean/cpi.dta", clear
    rename year policy_year_init
    tempfile cpi_by_init
    save `cpi_by_init', replace
restore
merge m:1 policy_year_init using `cpi_by_init', keepusing(cpi) keep(1 3) nogen
generate double nfip_premium_init_r = nfip_premium_init / cpi

postdist nfip_premium_init, results(`results') row(15) section("Premiums") ///
    measure("NFIP premium, first observed year") ///
    universe("All NFIP properties") real(nfip_premium_init_r) ///
    note("First observed policy year; real column deflated with clean/cpi.dta")

* -----------------------------------------------------------------------------
* 4. Claims
* -----------------------------------------------------------------------------
generate byte any_claim = claim_cb > 0 & !missing(claim_cb)
count if any_claim
local claimant_n = r(N)

* The basic claim amount is reported as a per-property average, zeros included.
postdist claim_cb, results(`results') row(26) section("Claims") ///
    measure("Cumulative net claims") universe("All NFIP properties") ///
    note("Zeros included -- per-property average across all NFIP properties")

postcount, results(`results') row(30) section("Claims") ///
    measure("Properties with positive claims") universe("All NFIP properties") ///
    value(`claimant_n') ///
    note("Positive cumulative net building-plus-contents claims")

* Summarize claim amounts among properties with positive claims only.
postdist claim_cb if any_claim, results(`results') row(32) section("Claims") ///
    measure("Cumulative net claims") universe("Properties with positive claims") ///
    note("Claims for claimant properties only; deflated per claim at year of loss")

foreach v in nfip_rl nfip_srl {
    count if `v' == 1
    local n_`v' = r(N)
}
postcount, results(`results') row(38) section("Claims") ///
    measure("Repetitive loss (RL)") universe("All NFIP properties") ///
    value(`n_nfip_rl') ///
    note("FEMA repetitive-loss designation from the multiple-loss file")
postcount, results(`results') row(39) section("Claims") ///
    measure("Severe repetitive loss (SRL)") universe("All NFIP properties") ///
    value(`n_nfip_srl') ///
    note("FEMA severe repetitive-loss designation; the FMA priority group")

* Claims against what the house is worth. 
generate double claim_to_value = claim_cb / attom_market_value_total ///
    if any_claim & attom_market_value_total > 0 & !missing(attom_market_value_total)
count if !missing(claim_to_value)
local ctv_n = r(N)
postdist claim_to_value, results(`results') row(40) section("Claims") ///
    measure("Claims divided by market value") ///
    universe("Claimants with an ATTOM value") ///
    note("Claim amount relative to ATTOM market value; read the mean and SE, not the outliers")

count if claim_to_value > 1 & !missing(claim_to_value)
postshare, results(`results') row(46) section("Claims") ///
    measure("Paid more in claims than market value") ///
    universe("Claimants with an ATTOM value") num(`=r(N)') den(`ctv_n') ///
    note("Cumulative claims exceed the property's assessed market value")

* -----------------------------------------------------------------------------
* 5. Elevation cost, from the Builty property elevation file
* -----------------------------------------------------------------------------
use "`data'/clean/builty_elevations.dta", clear

count
local builty_n = r(N)

postcount, results(`results') row(50) section("Elevation cost") ///
    measure("Observed elevation properties") ///
    universe("Builty elevation properties") value(`builty_n') ///
    note("One row per address after clean_builty.do collapses; 9,853 properties cover 13,387 permits")
* Summarize project costs for properties with positive project_value.
postdist project_value if project_value > 0, results(`results') row(51) ///
    section("Elevation cost") measure("Elevation project cost") ///
    universe("Properties reporting project cost") ///
    note("Positive nonmissing project_value; 2023 dollars, not necessarily the elevation permit")

postclose `results'

* -----------------------------------------------------------------------------
* 6. Save and export
* -----------------------------------------------------------------------------
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
        "`stat'" == "Mean" {
        putexcel F`row', nformat("$#,##0")
        putexcel G`row', nformat("$#,##0")
    }
    else if inlist("`sect'", "Premiums", "Claims", "Elevation cost") & ///
        "`stat'" == "Standard error" {
        putexcel F`row', nformat("$#,##0.00")
        putexcel G`row', nformat("$#,##0.00")
    }
    else {
        putexcel F`row', nformat("#,##0")
    }
}

di as result "Saved: `outfile'.dta"
di as result "Saved: `outfile'.xlsx"

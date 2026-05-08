* merge_npr_onto_state_panels.do
*
* Merge FEMA NPR property records onto:
*   - data/{state}_property_panel.dta  at state x ZIP x permit_year
*   - data/{state}_county_panel.dta    at county FIPS x year
*
* NPR source has State, Zip, Fiscal Year, Price Paid, and Status but no county
* FIPS. For the county panel, this script uses the state property panel's observed
* fips_county x zip_code x permit_year combinations as the ZIP-to-county bridge.

clear all
set more off

*------------------------------------------------------------------------------
* CONFIGURE: change this one line to run for a different state
*------------------------------------------------------------------------------
local state_l "va"

*------------------------------------------------------------------------------
* Derived from state_l
*------------------------------------------------------------------------------
local state_abbr = upper("`state_l'")

local root "/Users/anna/Desktop/Research/climate-investments"
local npr_csv "`root'/data/fema/fema_npr.csv"
capture confirm file "`npr_csv'"
if _rc != 0 {
    local npr_csv "`root'/data/fema/fema npr.csv"
}
capture confirm file "`npr_csv'"
if _rc != 0 {
    local npr_csv "`root'/data/fema/femanpr.csv"
}
local property_panel "`root'/data/`state_l'_property_panel.dta"
local county_panel "`root'/data/`state_l'_county_panel.dta"

local out_prop "`root'/data/`state_l'_property_panel_npr.dta"
local out_county "`root'/data/`state_l'_county_panel_npr.dta"

tempfile npr_local npr_zipyear npr_countyyear

*==============================================================================
* 1. NPR: aggregate to state x ZIP x fiscal year
*==============================================================================
* If this line times out, download/materialize data/fema/fema npr.csv locally.
copy "`npr_csv'" "`npr_local'", replace
import delimited using "`npr_local'", varnames(1) clear

rename *, lower
capture rename fiscal_year fiscalyear
capture rename price_paid pricepaid

destring fiscalyear zip pricepaid residence owner structure, replace force

gen str2 state_abbr = upper(strtrim(state))
keep if state_abbr == "`state_abbr'"

tostring zip, gen(zip_code) format("%05.0f") force
replace zip_code = substr(zip_code, 1, 5)
replace zip_code = "" if zip_code == "." | zip_code == "00000"

gen int permit_year = fiscalyear
drop if missing(permit_year) | zip_code == ""

gen byte npr_closed = upper(strtrim(status)) == "CLOSED"
gen byte npr_has_price = !missing(pricepaid)

collapse ///
    (count) npr_n_records      = id          ///
    (sum)   npr_total_paid     = pricepaid   ///
    (mean)  npr_avg_paid       = pricepaid   ///
    (sum)   npr_n_closed       = npr_closed  ///
    (sum)   npr_n_with_price   = npr_has_price ///
    (sum)   npr_residence_sum  = residence   ///
    (sum)   npr_owner_sum      = owner       ///
    (sum)   npr_structure_sum  = structure   ///
    , by(state_abbr zip_code permit_year)

gen byte npr_fema_any = npr_n_records > 0
gen double npr_log_total_paid = log(npr_total_paid + 1)

label var npr_fema_any       "=1 if any FEMA NPR record in state ZIP-year"
label var npr_n_records      "FEMA NPR records in state ZIP-year"
label var npr_total_paid     "FEMA NPR price paid total in state ZIP-year ($)"
label var npr_avg_paid       "Mean FEMA NPR price paid in state ZIP-year ($)"
label var npr_n_closed       "FEMA NPR closed records in state ZIP-year"
label var npr_n_with_price   "FEMA NPR records with nonmissing price paid in state ZIP-year"
label var npr_residence_sum  "Sum of NPR residence field in state ZIP-year"
label var npr_owner_sum      "Sum of NPR owner field in state ZIP-year"
label var npr_structure_sum  "Sum of NPR structure field in state ZIP-year"
label var npr_log_total_paid "Log(FEMA NPR total price paid + 1)"

sort state_abbr zip_code permit_year
save `npr_zipyear'

*==============================================================================
* 2. Property panel: merge NPR to each permit by state x ZIP x permit_year
*==============================================================================
use "`property_panel'", clear

gen str2 state_abbr = "`state_abbr'"

capture confirm numeric variable zip_code
if _rc == 0 {
    tostring zip_code, replace format("%05.0f") force
}
else {
    replace zip_code = strtrim(zip_code)
}
replace zip_code = substr(zip_code, 1, 5)
replace zip_code = substr("00000" + zip_code, length("00000" + zip_code) - 4, 5) ///
    if zip_code != "" & zip_code != "."
replace zip_code = "" if zip_code == "." | zip_code == "00000"

merge m:1 state_abbr zip_code permit_year using `npr_zipyear', ///
    keep(master match) gen(_merge_npr)

foreach v of varlist npr_n_records npr_total_paid npr_avg_paid npr_n_closed ///
    npr_n_with_price npr_residence_sum npr_owner_sum npr_structure_sum ///
    npr_fema_any npr_log_total_paid {
    replace `v' = 0 if missing(`v')
}

compress
save "`out_prop'", replace

di as result "Saved `state_abbr' property panel with NPR: `out_prop'"
tab _merge_npr
tab npr_fema_any

*==============================================================================
* 3. Build NPR county-year data through observed property ZIP/county bridge
*==============================================================================
preserve
    keep state_abbr fips_county county_name zip_code permit_year
    drop if missing(fips_county) | missing(permit_year) | zip_code == ""
    duplicates drop fips_county zip_code permit_year, force
    merge m:1 state_abbr zip_code permit_year using `npr_zipyear', ///
        keep(master match) nogen

    foreach v of varlist npr_n_records npr_total_paid npr_n_closed ///
        npr_n_with_price npr_residence_sum npr_owner_sum npr_structure_sum ///
        npr_fema_any {
        replace `v' = 0 if missing(`v')
    }

    collapse ///
        (sum)   npr_n_records_county      = npr_n_records      ///
        (sum)   npr_total_paid_county     = npr_total_paid     ///
        (sum)   npr_n_closed_county       = npr_n_closed       ///
        (sum)   npr_n_with_price_county   = npr_n_with_price   ///
        (sum)   npr_residence_sum_county  = npr_residence_sum  ///
        (sum)   npr_owner_sum_county      = npr_owner_sum      ///
        (sum)   npr_structure_sum_county  = npr_structure_sum  ///
        (max)   npr_fema_any_county       = npr_fema_any       ///
        , by(fips_county permit_year)

    rename permit_year year
    gen double npr_log_total_paid_county = log(npr_total_paid_county + 1)

    label var npr_fema_any_county       "=1 if any FEMA NPR record in observed county ZIP-years"
    label var npr_n_records_county      "FEMA NPR records summed over observed county ZIP-years"
    label var npr_total_paid_county     "FEMA NPR price paid summed over observed county ZIP-years ($)"
    label var npr_n_closed_county       "FEMA NPR closed records summed over observed county ZIP-years"
    label var npr_n_with_price_county   "FEMA NPR priced records summed over observed county ZIP-years"
    label var npr_log_total_paid_county "Log(county-year NPR total price paid + 1)"

    sort fips_county year
    save `npr_countyyear'
restore

*==============================================================================
* 4. County panel: merge county-year NPR variables
*==============================================================================
use "`county_panel'", clear

merge m:1 fips_county year using `npr_countyyear', ///
    keep(master match) gen(_merge_npr)

foreach v of varlist npr_n_records_county npr_total_paid_county ///
    npr_n_closed_county npr_n_with_price_county ///
    npr_residence_sum_county npr_owner_sum_county ///
    npr_structure_sum_county npr_fema_any_county ///
    npr_log_total_paid_county {
    replace `v' = 0 if missing(`v')
}

compress
save "`out_county'", replace

di as result "Saved `state_abbr' county panel with NPR: `out_county'"
tab _merge_npr
tab npr_fema_any_county

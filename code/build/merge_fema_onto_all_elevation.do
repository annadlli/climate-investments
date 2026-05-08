* merge_fema_onto_all_elevation.do
*
* Merge FEMA Hazard Mitigation Assistance Projects and FEMA NPR property
* records onto data/all_elevation_strict_filtered.dta.
*
* Output unit: one row per Builty strict-filtered elevation permit.
*
* Merge grains:
*   - HMA projects: county FIPS x event year
*   - FEMA NPR:     state x ZIP x fiscal year
*
* Permit event year follows the figure scripts:
*   DATE_ISSUED, falling back to DATE_SUBMITTED, then DATE_FINALED.

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments"
local permits "`root'/data/all_elevation_strict_filtered.dta"
local hma_csv "`root'/data/fema/HazardMitigationAssistanceProjects (1).csv"
local npr_csv "`root'/data/fema/fema npr.csv"
local out "`root'/data/all_elevation_strict_filtered_fema.dta"

tempfile hma_cy npr_zipyear
tempfile npr_local

*==============================================================================
* 1. FEMA HMA projects: aggregate to county x fiscal year
*==============================================================================
import delimited using "`hma_csv'", bindquote(strict) varnames(1) clear

rename *, lower

gen str proj_type = lower(projecttype)
gen str prog      = upper(programarea)

destring statenumbercode countycode programfy projectamount ///
    initialobligationamount federalshareobligated            ///
    subrecipientadmincostamt srmcobligatedamt                ///
    recipientadmincostamt costsharepercentage                ///
    benefitcostratio netvaluebenefits                        ///
    numberofproperties numberoffinalproperties, replace force

drop if missing(statenumbercode) | missing(countycode) | countycode == 0
drop if missing(programfy)

gen long countycode_int = int(countycode)
tostring statenumbercode, gen(st_fips) format("%02.0f")
tostring countycode_int, gen(co_fips) format("%03.0f")
gen str5 fips_county = st_fips + co_fips

gen int event_year = programfy

* Drop administrative non-starters, matching the existing HMA-cleaning scripts.
drop if inlist(status, "Withdrawn", "Void", "Pending")

gen byte hma_is_elev   = regexm(proj_type, "202\.") | regexm(proj_type, "elevat")
gen byte hma_is_buyout = regexm(proj_type, "200\.|201\.") ///
                       | regexm(proj_type, "acquisition") ///
                       | regexm(proj_type, "relocation")

gen byte hma_is_hmgp = prog == "HMGP"
gen byte hma_is_fma  = prog == "FMA"
gen byte hma_is_bric = prog == "BRIC"
gen byte hma_is_pdm  = prog == "PDM"

gen byte hma_elev_hmgp   = hma_is_elev   == 1 & hma_is_hmgp == 1
gen byte hma_elev_fma    = hma_is_elev   == 1 & hma_is_fma  == 1
gen byte hma_buyout_hmgp = hma_is_buyout == 1 & hma_is_hmgp == 1
gen byte hma_buyout_fma  = hma_is_buyout == 1 & hma_is_fma  == 1
gen byte hma_project_row = 1

collapse ///
    (count) hma_n_projects_total      = hma_project_row         ///
    (sum)   hma_n_elev_total          = hma_is_elev             ///
    (sum)   hma_n_buyout_total        = hma_is_buyout           ///
    (sum)   hma_n_elev_hmgp           = hma_elev_hmgp           ///
    (sum)   hma_n_elev_fma            = hma_elev_fma            ///
    (sum)   hma_n_buyout_hmgp         = hma_buyout_hmgp         ///
    (sum)   hma_n_buyout_fma          = hma_buyout_fma          ///
    (sum)   hma_n_hmgp                = hma_is_hmgp             ///
    (sum)   hma_n_fma                 = hma_is_fma              ///
    (sum)   hma_n_bric                = hma_is_bric             ///
    (sum)   hma_n_pdm                 = hma_is_pdm              ///
    (sum)   hma_project_amount        = projectamount           ///
    (sum)   hma_initial_obligation    = initialobligationamount ///
    (sum)   hma_fed_obligated         = federalshareobligated   ///
    (sum)   hma_subrecipient_admin    = subrecipientadmincostamt ///
    (sum)   hma_srmc_obligated        = srmcobligatedamt        ///
    (sum)   hma_recipient_admin       = recipientadmincostamt   ///
    (mean)  hma_avg_cost_share_pct    = costsharepercentage     ///
    (mean)  hma_avg_bca               = benefitcostratio        ///
    (sum)   hma_net_benefits          = netvaluebenefits        ///
    (sum)   hma_n_properties          = numberofproperties      ///
    (sum)   hma_n_final_properties    = numberoffinalproperties ///
    , by(fips_county event_year)

gen byte hma_fema_any       = hma_n_projects_total > 0
gen byte hma_fema_elev      = hma_n_elev_total > 0
gen byte hma_fema_buyout    = hma_n_buyout_total > 0
gen byte hma_fema_elev_hmgp = hma_n_elev_hmgp > 0
gen byte hma_fema_elev_fma  = hma_n_elev_fma > 0
gen double hma_log_fed_obligated = log(hma_fed_obligated + 1)

label var hma_fema_any       "=1 if any FEMA HMA project in county-year"
label var hma_fema_elev      "=1 if any FEMA HMA elevation project in county-year"
label var hma_fema_buyout    "=1 if any FEMA HMA buyout/acquisition/relocation in county-year"
label var hma_n_elev_total   "FEMA HMA elevation projects in county-year"
label var hma_fed_obligated  "FEMA HMA federal share obligated in county-year ($)"
label var hma_avg_bca        "Mean HMA benefit-cost ratio in county-year"

sort fips_county event_year
save `hma_cy'

*==============================================================================
* 2. FEMA NPR: aggregate to state x ZIP x fiscal year
*==============================================================================
* Copy to Stata's temp folder before import. This is faster and avoids some
* macOS file-provider/cloud timeouts once the source CSV is downloaded locally.
copy "`npr_csv'" "`npr_local'", replace
import delimited using "`npr_local'", varnames(1) clear

rename *, lower

* Stata usually imports headers with spaces as fiscalyear / pricepaid. These
* captures make the script tolerant if a different version preserves underscores.
capture rename fiscal_year fiscalyear
capture rename price_paid pricepaid

destring fiscalyear zip pricepaid residence owner structure, replace force

gen str2 state_abbr = upper(strtrim(state))
tostring zip, gen(zip_code) format("%05.0f")
replace zip_code = substr(zip_code, 1, 5)
gen int event_year = fiscalyear

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
    , by(state_abbr zip_code event_year)

gen byte npr_fema_any = npr_n_records > 0
gen double npr_log_total_paid = log(npr_total_paid + 1)

label var npr_fema_any       "=1 if any FEMA NPR record in state-ZIP-year"
label var npr_n_records      "FEMA NPR records in state-ZIP-year"
label var npr_total_paid     "FEMA NPR price paid total in state-ZIP-year ($)"
label var npr_avg_paid       "Mean FEMA NPR price paid in state-ZIP-year ($)"
label var npr_n_closed       "FEMA NPR closed records in state-ZIP-year"
label var npr_log_total_paid "Log(FEMA NPR total price paid + 1)"

sort state_abbr zip_code event_year
save `npr_zipyear'

*==============================================================================
* 3. Base permits: create merge keys
*==============================================================================
use "`permits'", clear

gen str2 state_abbr = upper(strtrim(STATE))

capture confirm numeric variable FIPS_STATE
if _rc == 0 {
    tostring FIPS_STATE, gen(st_fips_base) format("%02.0f") force
}
else {
    gen str20 st_fips_base = strtrim(FIPS_STATE)
}
replace st_fips_base = subinstr(st_fips_base, ".0", "", .)
replace st_fips_base = substr("00" + st_fips_base, length("00" + st_fips_base) - 1, 2)

capture confirm numeric variable FIPS_COUNTY
if _rc == 0 {
    tostring FIPS_COUNTY, gen(co_fips_base) format("%03.0f") force
}
else {
    gen str20 co_fips_base = strtrim(FIPS_COUNTY)
}
replace co_fips_base = subinstr(co_fips_base, ".0", "", .)
replace co_fips_base = substr("000" + co_fips_base, length("000" + co_fips_base) - 2, 3)

gen str5 fips_county = st_fips_base + co_fips_base

gen double date_issued_d    = date(substr(DATE_ISSUED,    1, 10), "YMD")
gen double date_submitted_d = date(substr(DATE_SUBMITTED, 1, 10), "YMD")
gen double date_finaled_d   = date(substr(DATE_FINALED,   1, 10), "YMD")
format date_issued_d date_submitted_d date_finaled_d %td

gen double event_date = date_issued_d
replace event_date = date_submitted_d if missing(event_date)
replace event_date = date_finaled_d   if missing(event_date)
format event_date %td

gen int event_year = year(event_date)

gen str14 permit_date_source = ""
replace permit_date_source = "DATE_ISSUED"    if !missing(date_issued_d)
replace permit_date_source = "DATE_SUBMITTED" if missing(date_issued_d) & !missing(date_submitted_d)
replace permit_date_source = "DATE_FINALED"   if missing(date_issued_d) & missing(date_submitted_d) & !missing(date_finaled_d)

capture confirm numeric variable ZIPCODE
if _rc == 0 {
    tostring ZIPCODE, gen(zip_code) format("%05.0f") force
}
else {
    gen str20 zip_code = strtrim(ZIPCODE)
}
replace zip_code = substr(zip_code, 1, 5)
replace zip_code = substr("00000" + zip_code, length("00000" + zip_code) - 4, 5) ///
    if zip_code != "" & zip_code != "."
replace zip_code = "" if zip_code == "." | zip_code == "00000"

label var fips_county        "5-digit county FIPS"
label var event_date         "Permit event date: issued, submitted, then finaled"
label var event_year         "Permit event year"
label var permit_date_source "Date field used for permit event year"
label var zip_code           "5-digit ZIP code"

*==============================================================================
* 4. Merge HMA and NPR
*==============================================================================
merge m:1 fips_county event_year using `hma_cy', keep(master match) gen(_merge_hma)

foreach v of varlist hma_n_projects_total hma_n_elev_total hma_n_buyout_total ///
    hma_n_elev_hmgp hma_n_elev_fma hma_n_buyout_hmgp hma_n_buyout_fma ///
    hma_n_hmgp hma_n_fma hma_n_bric hma_n_pdm hma_project_amount ///
    hma_initial_obligation hma_fed_obligated hma_subrecipient_admin ///
    hma_srmc_obligated hma_recipient_admin hma_net_benefits ///
    hma_n_properties hma_n_final_properties hma_fema_any ///
    hma_fema_elev hma_fema_buyout hma_fema_elev_hmgp ///
    hma_fema_elev_fma hma_log_fed_obligated {
    replace `v' = 0 if missing(`v')
}

merge m:1 state_abbr zip_code event_year using `npr_zipyear', keep(master match) gen(_merge_npr)

foreach v of varlist npr_n_records npr_total_paid npr_avg_paid npr_n_closed ///
    npr_n_with_price npr_residence_sum npr_owner_sum npr_structure_sum ///
    npr_fema_any npr_log_total_paid {
    replace `v' = 0 if missing(`v')
}

compress
save "`out'", replace

di as result "Saved FEMA-merged strict elevation file: `out'"
di as result "Rows: " _N

tab _merge_hma
tab _merge_npr
tab hma_fema_elev
tab npr_fema_any

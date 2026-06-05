/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-05-22

ARCHIVED: NPR buyout data was removed from the pipeline 2026-05-29. Kept for
    reference (structure retained to filter to single-family homes later); not
    called by master.do.

Description: Cleans the FEMA NPR buyout records -> clean/fema/fema_npr.dta.

Source: apps.npr.org/fema-table/assets/fema.csv
******************************************************************************/

clear all
set more off

* Paths from master.do; standalone fallback
if "$data" == "" global data "/Users/vendelasolvindnorman/Library/CloudStorage/Dropbox/Flooding/Data"
global draw   "$data/raw"
global dclean "$data/clean"
local raw   "$draw"
local clean "$dclean"

********************************************************************************
* 4. fema_npr -> fema_npr.dta
//2026-05-29: edit: removing buyout data from merge pipeline and clean files
********************************************************************************
//note: for future processing, structure is kept to filter to single family homes
local npr_raw ""
foreach f in ///
    "`raw'/fema_npr.csv"  {
    capture confirm file "`f'"
    if _rc == 0 & "`npr_raw'" == "" local npr_raw "`f'"
}

if "`npr_raw'" == "" {
    di as error "SKIP: fema_npr raw file not found in data/raw."
}
else {
    import delimited using "`npr_raw'", clear varnames(1) stringcols(_all)
    rename *, lower
    capture rename fiscal_year fiscalyear
    capture rename price_paid pricepaid

    keep id fiscalyear disasterdescription residence owner structure city state zip ///
         pricepaid status
    drop if missing(id) & missing(fiscalyear) & missing(state) & missing(zip)

    destring fiscalyear residence owner structure pricepaid zip, replace force
    tostring zip, gen(zip_code) format("%05.0f") force
    replace zip_code = "" if zip_code == "." | zip_code == "00000"

    label data "Clean source: FEMA NPR"
    label var id "FEMA NPR record ID"
    label var fiscalyear "Fiscal year"
    label var disasterdescription "Disaster description"
    label var state "State abbreviation"
    label var zip_code "ZIP code, 5-digit string"
    label var pricepaid "Price paid"
    label var status "Record status"
	drop id zip_code
	order state zip fiscalyear city
    compress
    save "`clean'/fema/fema_npr.dta", replace
}

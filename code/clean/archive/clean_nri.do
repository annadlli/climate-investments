/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-05-22

ARCHIVED: NRI was dropped from the pipeline 2026-05-29 (FMA-only focus). Kept
    for reference; not called by master.do.

Description: Cleans the FEMA National Risk Index county table -> clean/fema/nri_counties.dta.

Source: hazards.fema.gov/nri/data-resources  (All Counties table)
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
* 2. NRI -> nri_counties.dta
********************************************************************************
local nri_raw ""
foreach f in ///
    "`raw'/nri_counties.csv" {
    capture confirm file "`f'"
    if _rc == 0 & "`nri_raw'" == "" local nri_raw "`f'"
}

if "`nri_raw'" == "" {
    di as error "SKIP: NRI raw file not found in data/raw."
}
else {
    import delimited using "`nri_raw'", clear varnames(1) stringcols(_all)
    rename *, lower

    keep  statenameabbreviation statefipscode ///
         countyname  countyfipscode statecountyfipscode ///
            v81  v315 

    drop if missing(statecountyfipscode) & missing(countyname)
	rename v81 coastalfloodriskscore
	rename v315 inlandfloodriskscore
    foreach v in statefipscode countyfipscode statecountyfipscode ///
        coastalfloodriskscore inlandfloodriskscore {
        capture destring `v', replace force
    }
    label data "Clean source: National Risk Index counties"
	order statenameabbreviation statefipscode countyname countyfipscode statecountyfipscode
    compress
    save "`clean'/fema/nri_counties.dta", replace
}

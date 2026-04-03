//filter building permit
cd "/Users/anna/Desktop/climate-investments/"
local clean        "/Users/anna/Desktop/climate-investments/data/clean"
local atom"/Users/anna/Desktop/climate-investments/data/atom_process"
local in   "/Users/anna/Desktop/climate-investments/data/atom filtered"
local out "/Users/anna/Desktop/climate-investments/data/clean/chunk"

// use "/Users/anna/Desktop/climate-investments/data/clean/permits_filtered_all.dta", clear
// gen street2 = substr(street, 1, 244)
//  drop street 
//  rename street2 street
// keep street zipcode
//
// // * Rename to match atom file merge keys
// rename street  PROPERTYADDRESSFULL
// rename zipcode PROPERTYADDRESSZIP 
// tostring PROPERTYADDRESSFULL, replace force
//  tostring PROPERTYADDRESSZIP, replace force
// replace PROPERTYADDRESSFULL = lower(trim(PROPERTYADDRESSFULL))
// replace PROPERTYADDRESSFULL = subinstr(PROPERTYADDRESSFULL, ",", "", .)
//  replace PROPERTYADDRESSFULL = subinstr(PROPERTYADDRESSFULL, ".", "", .)
//
//  replace PROPERTYADDRESSZIP = upper(trim(PROPERTYADDRESSZIP))
// replace PROPERTYADDRESSZIP = subinstr(PROPERTYADDRESSZIP, " ", "", .)
//  drop if missing(PROPERTYADDRESSFULL) | missing(PROPERTYADDRESSZIP)
//  duplicates tag PROPERTYADDRESSFULL PROPERTYADDRESSZIP, gen(dup)
//  duplicates drop PROPERTYADDRESSFULL PROPERTYADDRESSZIP, force
//  save "`clean'/permits_unique.dta", replace



local files : dir "`in'" files "*.dta"

foreach f of local files {

    di "----------------------------------------------------------"
    di "Processing: `f'"
    di "----------------------------------------------------------"

    use "`in'/`f'", clear

* CLEAN ADDRESS FORMAT
replace PROPERTYADDRESSFULL = lower(trim(PROPERTYADDRESSFULL))
replace PROPERTYADDRESSFULL = subinstr(PROPERTYADDRESSFULL, ",", "", .)
replace PROPERTYADDRESSFULL = subinstr(PROPERTYADDRESSFULL, ".", "", .)

replace PROPERTYADDRESSZIP = upper(trim(PROPERTYADDRESSZIP))
replace PROPERTYADDRESSZIP = subinstr(PROPERTYADDRESSZIP, " ", "", .)

drop if missing(PROPERTYADDRESSFULL) | missing(PROPERTYADDRESSZIP)
duplicates drop PROPERTYADDRESSFULL PROPERTYADDRESSZIP, force
merge 1:1 PROPERTYADDRESSFULL PROPERTYADDRESSZIP using "`clean'/permits_unique.dta"
keep if _merge == 3
drop _merge

save "`out'/`f'", replace
}


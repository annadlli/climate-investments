*--------------------------------------------------*
* 1. Import FEMA HMA Subapplications file
*--------------------------------------------------*
import delimited "/Users/anna/Desktop/Research/climate-investments/data/fema/HazardMitigationAssistanceProjects (1).csv", clear varnames(1) stringcols(_all)

* Keep Texas only
keep if state == "Texas"

//keep if HMGP or FMA
keep if programarea == "HMGP" | programarea == "FMA"

//want pre2020 data specifically
destring programfy, replace
drop if programfy>=2020 //around half observationsa are dropped 

//has projectounties and countycode 
destring countycode, replace
gen got_grant = 0
replace got_grant = 1 if inlist(status, "Awarded", "Closed", "Completed", "Obligated") //pending, not approved, not selected, void, withdrawn are not awarded: 76% get grant

* Save raw Texas FMA subset
save "/Users/anna/Desktop/Research/climate-investments/data/fema/txsubapp.dta", replace

*-----------------------------*
* 2. Create county-year aggregates without collapse
*--------------------------------------------------*
bysort programfy countycode: egen any_grant = max(got_grant)
bysort programfy countycode: gen n_apps = _N
bysort programfy countycode: egen n_awards = total(got_grant)

* Keep one observation per county-year
bysort programfy countycode: gen countyyear_tag = (_n == 1)

* If you want to preserve representative FEMA variables from the first row,
* they will still be there after this keep
keep if countyyear_tag == 1
drop countyyear_tag

save "/Users/anna/Desktop/Research/climate-investments/data/fema/txsubapp_countyyear.dta", replace


*--------------------------------------------------*
* 2. Merge FEMA subapplications into filtered flood data
*--------------------------------------------------*
use "/Users/anna/Desktop/Research/climate-investments/data/flood_elevation_filters.dta", clear

* Standardize merge keys
gen countycode = COUNTY_FIPS
gen programfy  = TAXFISCALYEAR
destring programfy countycode,replace


merge m:1 programfy countycode using "/Users/anna/Desktop/Research/climate-investments/data/fema/txsubapp_countyyear.dta"

keep if _merge == 3

save "/Users/anna/Desktop/Research/climate-investments/data/hma_mergedin.dta", replace //26,368 obs kept out of 77181

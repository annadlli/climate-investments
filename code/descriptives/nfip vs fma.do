**********************/
*Author: Anna Li
*Date: July 16, 2026
*Description: Verification of why NFIP elevated property number differs (is less than) FMA
************************

local root "/Users/anna/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
//doublecheck NFIP
use "`root'/clean/nfip_policies_property.dta", clear


*for consistent order, convert to full name
gen str20 state_name = ""

replace state_name = "Alabama"        if state == "AL"
replace state_name = "Connecticut"    if state == "CT"
replace state_name = "Delaware"       if state == "DE"
replace state_name = "Florida"        if state == "FL"
replace state_name = "Georgia"        if state == "GA"
replace state_name = "Louisiana"      if state == "LA"
replace state_name = "Massachusetts"  if state == "MA"
replace state_name = "Maryland"       if state == "MD"
replace state_name = "Maine"          if state == "ME"
replace state_name = "Mississippi"    if state == "MS"
replace state_name = "North Carolina" if state == "NC"
replace state_name = "New Hampshire"  if state == "NH"
replace state_name = "New Jersey"     if state == "NJ"
replace state_name = "New York"       if state == "NY"
replace state_name = "Pennsylvania"   if state == "PA"
replace state_name = "Rhode Island"   if state == "RI"
replace state_name = "South Carolina" if state == "SC"
replace state_name = "Texas"          if state == "TX"
replace state_name = "Virginia"       if state == "VA"
replace state_name = "Vermont"        if state == "VT"

assert state_name != ""
sort state_name
tabstat got_elevated, by(state_name) statistics(sum) //counts all instances of elevation
tab elevation_year

//doublecheck FMA property numbers next
local sample_states ///
    "Alabama|Connecticut|Delaware|Florida|Georgia|Louisiana|Massachusetts|Maryland|Maine|Mississippi|North Carolina|New Hampshire|New Jersey|New York|Pennsylvania|Rhode Island|South Carolina|Texas|Virginia|Vermont"

	
//using properties data:
use "`root'/clean/fma_elevation_properties.dta", clear
destring numberofproperties, replace
tabstat numberofproperties, by(state) statistics(sum)

* limit to the same states as NFIP
keep if regexm(state, "^(`sample_states')$")
* limit to the same years as NFIP
destring programfy, replace
keep if programfy >= 2009 & programfy <= 2022

sort state
tabstat numberofproperties, by(state) statistics(sum)
tab programfy

//using projects data and apply same process:
use "`root'/clean/fma_elevation_projects.dta", clear
destring obligation_year, replace
destring n_properties, replace

keep if regexm(state, "^(`sample_states')$")
keep if obligation_year >= 2009 & obligation_year <=2022

sort state
tabstat n_properties, by(state) statistics(sum)
tab obligation_year
//output put in deck

//given discrepancy mainly comes from LA, inspect LA raw to see if its due to cleaning
*do similar cleaning process, without dorpping to see issue 

import delimited using ///
    "`root'/clean/nfip_policies_raw/la.csv", ///
    clear varnames(1) stringcols(_all)

* Keep variables needed for the property definition and diagnostics
keep id propertystate reportedzipcode countycode censusblockgroupfips ///
    nfipratedcommunitynumber ratedfloodzone occupancytype ///
    originalconstructiondate originalnbdate policyeffectivedate ///
    elevatedbuildingindicator primaryresidenceindicator ///
    rentalpropertyindicator latitude longitude buildingdescriptioncode agriculturestructureindicator stateownedindicator

* Standardize names
rename propertystate                state
rename reportedzipcode             zipcode
rename elevatedbuildingindicator   elevated
rename primaryresidenceindicator   primary_residence

* Create same variables
gen policy_year = real(substr(policyeffectivedate, 1, 4))
drop policyeffectivedate
gen construction_year = real(substr(originalconstructiondate, 1, 4))
gen str12 block = string(censusblockgroupfips, "%012.0f")
gen geo_key = cond( ///
    missing(censusblockgroupfips), ///
    "z" + zipcode, ///
    "b" + block ///
)
egen property_id = group(geo_key originalconstructiondate originalnbdate)

* Identify SFHA properties, but do not drop them
gen sfha = inlist(upper(substr(ratedfloodzone, 1, 1)), "A", "V") ///
    if !missing(ratedfloodzone)
	
drop if missing(property_id)

* Convert indicators to numeric
destring elevated primary_residence rentalpropertyindicator, replace

* Remove exact duplicate policy records
duplicates drop

* Apply the same monotonic-elevation correction
bysort property_id (policy_year): ///
    replace elevated = max(elevated, elevated[_n-1])

* Create property-level elevation measures
bysort property_id: egen ever_elevated = max(elevated)

bysort property_id (policy_year): ///
    gen got_elevated = elevated[_N] == 1 & elevated[1] == 0

bysort property_id (policy_year): ///
    egen elevation_year = min(cond(elevated == 1, policy_year, .))

replace elevation_year = . if got_elevated == 0

* Record the observation window
bysort property_id: egen first_policy_year = min(policy_year)
bysort property_id: egen last_policy_year  = max(policy_year)

* Retain one row per approximate property
bysort property_id (policy_year): keep if _n == _N
isid property_id

* Verify unrestricted Louisiana totals
count
count if ever_elevated == 1
count if got_elevated == 1 //1755 properties

tabstat ever_elevated got_elevated, ///
    by(sfha) statistics(count sum mean)

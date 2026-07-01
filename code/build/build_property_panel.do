/******************************************************************************
Authors: Anna Li 
Date: 2026-06-28

Description:
 Builds a property-level analysis dataset from NFIP policies, ATTOM value cells, and FMA elevation grants. NFIP policies are the eligible universe;
    ATTOM values merge by cell (no street address in NFIP); FMA merges at county level.
******************************************************************************/
args data state
local stl = lower("`state'")

* ============================================================
* 1. NFIP: one row per property
* ============================================================
*import data (name kept as it is in Dropbox)
use "`data'/clean/`stl'.dta", clear

*drop exact duplicates (on all variables)
duplicates drop

*convert zipcode and countycode to be numeric
rename zipcode zip_key
replace zip_key = "" if inlist(zip_key, ".", "00000")
replace zip_key = substr(zip_key, 1, 5) if strpos(zip_key, "-")
destring countycode zip_key, replace

*generate some extra vairables for analysis 
bysort property_id: egen first_policy_year = min(policy_year)
bysort property_id: egen last_policy_year  = max(policy_year)

*generated if we were to ever restrict sample to elevated only properties
bysort property_id: egen ever_elevated     = max(elevated)

*label variables created
label var first_policy_year "First NFIP appearance"
label var last_policy_year "Last NFIP appearance"

//drop if ever_elevated == 0

*keep year that is closest to when elevation occurred
bysort property_id (policy_year): keep if elevated == 1 & _n == 1
replace elevated = ever_elevated
drop ever_elevated

*note: first 2 digit of countycode represent state, generated to merge with hma later
gen state_fips = substr(string(countycode), 1, 2)
local state_fips = substr(string(countycode[1]), 1, 2)

*rename policy_year to be plain year
rename policy_year year

*save intermediate file
tempfile nfipbase matches zip county
save `nfipbase'

* ============================================================
* 2. ATTOM: ZIP x Year and county x year as fallback
* ============================================================
*done to clean up variable -> will move this upstream in a bit
use "`data'/build/attom_summary/`stl'_attom_value_zip_year.dta", clear
destring zip_key, replace
rename policy_year year
save `zip'
use "`data'/build/attom_summary/`stl'_attom_value_county_year.dta", clear
destring countycode, replace  
rename policy_year year
save `county'

* creating intermediary dataset: attom matches
* Tier 1: ZIP × policy year
use `nfipbase', clear
keep property_id countycode zip_key year
merge m:1 zip_key year using `zip', keep(match) nogen
*label where matches came from
gen attom_tier = "zip_year"
save `matches', replace

* Tier 2: county × policy year
use `nfipbase', clear
keep property_id countycode zip_key year
merge m:1 countycode year using `county', keep(match) nogen

*label where matches came from
gen attom_tier = "county_year"

* Only keep county matches for properties with no zip match
merge m:1 property_id using `matches', keepusing(attom_tier) keep(master) nogen
append using `matches'
save `matches', replace

*merge matches with nfipbase
use `nfipbase', clear
merge 1:1 property_id using `matches', keep(master match) nogen
replace attom_tier = "unmatched" if missing(attom_tier)

//tab attom_tier *done to get an idea of how many we need the fallback for
label var attom_tier         "Best ATTOM value-cell match tier"

* ============================================================
* 3. FMA: county-level merge
* ============================================================
*collapse FEMA HMA grant has been moved to collapse_hma_grants.do
merge m:1 countycode using `"`data'/clean/fma_grants_county.dta"', keep(master match) nogen

* ============================================================
* 4. Order, save
* ============================================================
*jorder variables by id and year 
order property_id state countycode zip_key year construction_year          ///
    first_policy_year last_policy_year elevated primary_residence     ///
    ratedfloodzone attom_tier attom_* fma_any fma_n_grants            ///
    fma_fed_obligated fma_*
sort property_id
compress
save "`data'/build/`stl'_nfip_attom_fma_property.dta", replace
di as result "Saved: `stl'_nfip_attom_fma_property.dta"

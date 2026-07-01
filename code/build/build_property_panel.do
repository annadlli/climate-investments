/******************************************************************************
Authors: Anna Li 
Date: 2026-06-28

Description:
    Interactive version of build_nfip_attom_fma_analysis.do.
    Set locals at the top, then run section by section.
    Do NOT commit hardcoded paths — this file is for local investigation only.
******************************************************************************/

* ============================================================
* SET THESE BEFORE RUNNING
* ============================================================
local data  "/Users/anna/Desktop/climate-investments"
local stl        "va"

capture mkdir "`data'/build"

* ============================================================
* 1. NFIP: one row per property
* ============================================================

use "`data'/`stl'.dta", clear
//convert from policy level to property level 
duplicates drop

rename zipcode zip_key
replace zip_key = "" if inlist(zip_key, ".", "00000")
replace zip_key = substr(zip_key, 1, 5) if strpos(zip_key, "-")
destring countycode zip_key, replace

bysort property_id: egen first_policy_year = min(policy_year)
bysort property_id: egen last_policy_year  = max(policy_year)
bysort property_id: egen ever_elevated     = max(elevated)

label var first_policy_year "First NFIP appearance"
label var last_policy_year "Last NFIP appearance"

drop if ever_elevated == 0

//keep year that is closest to when elevation occurred
bysort property_id (policy_year): keep if elevated == 1 & _n == 1
replace elevated = ever_elevated
drop ever_elevated

//note: first 2 digit of countycode represent state. 
gen state_fips = substr(string(countycode), 1, 2)
tab state_fips
tempfile nfipbase matches zip county
save `nfipbase'
* ============================================================
* 2. ATTOM: ZIP x Year and county x year as fallback
* ============================================================
use "`data'/data/`stl'_attom_value_zip_year.dta", clear
destring zip_key, replace
save `zip'
use "`data'/data/`stl'_attom_value_county_year.dta", clear
destring countycode, replace
save `county'

* Tier 1: ZIP × policy year
use `nfipbase', clear
keep property_id countycode zip_key policy_year
merge m:1 zip_key policy_year using `zip', keep(match) nogen

gen attom_tier = "zip_year"
save `matches', replace

* Tier 2: county × policy year
use `nfipbase', clear
keep property_id countycode zip_key policy_year
merge m:1 countycode policy_year using `county', keep(match) nogen

gen attom_tier = "county_year"

* Only keep county matches for properties with no zip match
merge m:1 property_id using `matches', keepusing(attom_tier) keep(master) nogen
append using `matches'
save `matches', replace

use `nfipbase', clear
merge 1:1 property_id using `matches', keep(master match) nogen
replace attom_tier = "unmatched" if missing(attom_tier)
tab attom_tier
local state_fips = substr(string(countycode[1]), 1, 2)
label var attom_tier         "Best ATTOM value-cell match tier"

* ============================================================
* 3. FMA: county-level merge
* ============================================================
preserve
    use "`data'/fma_elevation_grants.dta", clear
    keep if state_code == real("`state_fips'")
    gen countycode = string(state_code, "%02.0f") + string(county_code, "%03.0f")
	destring countycode, replace

	gen fma_one = 1

    collapse (count) fma_n_grants        = fma_one      ///
             (sum)   fma_n_properties    = number_of_properties    ///
                     fma_project_amount  = project_amount          ///
                     fma_fed_obligated   = federal_share_obligated ///
             (mean)  fma_avg_bcr         = bcr, by(countycode)
    gen fma_any               = fma_n_grants > 0
    gen fma_log_fed_obligated = log(fma_fed_obligated + 1)
	label var fma_any               "County has any FMA elevation grant"
	label var fma_n_grants          "FMA elevation grants in county"
    label var fma_fed_obligated     "FMA federal share obligated in county"
    tempfile fma
    save `fma'
restore
merge m:1 countycode using `fma', keep(master match) nogen

* ============================================================
* 4. Label, order, save
* ============================================================

order property_id state countycode zip_key construction_year          ///
    first_policy_year last_policy_year elevated primary_residence     ///
    ratedfloodzone attom_tier attom_* fma_any fma_n_grants            ///
    fma_fed_obligated fma_*
sort property_id
compress
save "`data'/build/`stl'_nfip_attom_fma_property.dta", replace
di as result "Saved: `stl'_nfip_attom_fma_property.dta"

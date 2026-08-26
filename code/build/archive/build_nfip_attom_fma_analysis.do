/******************************************************************************
Authors: Anna Li
Date: 2026-07-01

Description:
    Builds a property-level analysis dataset from NFIP policies, ATTOM value
    cells, and FMA elevation grants. NFIP policies are the eligible universe;
    ATTOM values merge by cell (no street address in NFIP); FMA merges at
    county level.

Inputs:
    `data'/clean/nfip_policies_{state}.dta
    `data'/build/{state}_attom_value_zip_year.dta
    `data'/build/{state}_attom_value_county_year.dta
    `data'/clean/fma_elevation_grants.dta

Output:
    `data'/build/{state}_nfip_attom_fma_property.dta
******************************************************************************/

args data states

capture mkdir "`data'/build"

foreach st of local states {

    local stl = strlower("`st'")

    * -------------------------------------------------------------------------
    * 1. NFIP: one row per property
    * -------------------------------------------------------------------------
    use "`data'/clean/nfip_policies_`stl'.dta", clear
    duplicates drop

    rename zipcode zip_key
    replace zip_key = "" if inlist(zip_key, ".", "00000")
    replace zip_key = substr(zip_key, 1, 5) if strpos(zip_key, "-")
    destring countycode zip_key, replace

    bysort property_id: egen first_policy_year = min(policy_year)
    bysort property_id: egen last_policy_year  = max(policy_year)
    bysort property_id: egen ever_elevated     = max(elevated)

    label variable first_policy_year "First NFIP appearance"
    label variable last_policy_year  "Last NFIP appearance"

    drop if ever_elevated == 0

    bysort property_id (policy_year): keep if elevated == 1 & _n == 1
    replace elevated = ever_elevated
    drop ever_elevated

    gen state_fips = substr(string(countycode), 1, 2)

    tempfile nfipbase matches zip county fma
    save `nfipbase'

    * -------------------------------------------------------------------------
    * 2. ATTOM: tiered value-cell merge (ZIP × year, county × year fallback)
    * -------------------------------------------------------------------------
    use "`data'/build/`stl'_attom_value_zip_year.dta", clear
    destring zip_key, replace
    save `zip'

    use "`data'/build/`stl'_attom_value_county_year.dta", clear
    destring countycode, replace
    save `county'

    * Tier 1: ZIP × policy year
    use `nfipbase', clear
    keep property_id countycode zip_key policy_year
    merge m:1 zip_key policy_year using `zip', keep(match) nogen

    gen attom_tier = "zip_year"
    save `matches', replace

    * Tier 2: county × policy year (only for properties not matched at ZIP tier)
    use `nfipbase', clear
    keep property_id countycode zip_key policy_year
    merge m:1 countycode policy_year using `county', keep(match) nogen

    gen attom_tier = "county_year"
    merge m:1 property_id using `matches', keepusing(attom_tier) keep(master) nogen
    append using `matches'
    save `matches', replace

    use `nfipbase', clear
    merge 1:1 property_id using `matches', keep(master match) nogen
    replace attom_tier = "unmatched" if missing(attom_tier)
    label variable attom_tier "Best ATTOM value-cell match tier"

    * -------------------------------------------------------------------------
    * 3. FMA: county-level merge
    * -------------------------------------------------------------------------
    preserve
        use "`data'/clean/fma_elevation_grants.dta", clear
        local state_fips = substr(string(countycode[1]), 1, 2)
        keep if state_code == real("`state_fips'")
        gen countycode = string(state_code, "%02.0f") + string(county_code, "%03.0f")
        destring countycode, replace

        gen fma_one = 1
        collapse (sum)  fma_n_grants        = fma_one                  ///
                        fma_n_properties    = number_of_properties     ///
                        fma_project_amount  = project_amount           ///
                        fma_fed_obligated   = federal_share_obligated  ///
                 (mean) fma_avg_bcr         = bcr, by(countycode)
        gen fma_any               = fma_n_grants > 0
        gen fma_log_fed_obligated = log(fma_fed_obligated + 1)
        label variable fma_any             "County has any FMA elevation grant"
        label variable fma_n_grants        "FMA elevation grants in county"
        label variable fma_fed_obligated   "FMA federal share obligated in county"
        save `fma'
    restore

    merge m:1 countycode using `fma', keep(master match) nogen
    foreach v of varlist fma_* {
        replace `v' = 0 if missing(`v')
    }

    * -------------------------------------------------------------------------
    * 4. Label, order, save
    * -------------------------------------------------------------------------
    order property_id state countycode zip_key construction_year         ///
        first_policy_year last_policy_year elevated primary_residence    ///
        ratedfloodzone attom_tier attom_* fma_any fma_n_grants           ///
        fma_fed_obligated fma_*
    sort property_id
    compress
    save "`data'/build/`stl'_nfip_attom_fma_property.dta", replace

    tab attom_tier
    di as result "Saved: `st'"
}

/******************************************************************************
Authors: Vendela Norman
Date: 2026-09-02

Description: Cleans the FEMA NFIP redacted policies data, restricting to 
    single-family residential policies.

Source: fema.gov/openfema-data-page/fima-nfip-redacted-policies-v2

TODO: Clean censustract countycode censusblockgroupfips
******************************************************************************/

args data states

* -----------------------------------------------------------------------------
* Section 1: Initial import and cleaning
* -----------------------------------------------------------------------------

* Loop over states
foreach st of local states {

    * Import data
    local stl = strlower("`st'")
    import delimited using "`data'/clean/nfip_policies_raw/`stl'.csv", ///
        clear varnames(1) stringcols(_all)

    * Restrict to single-family homes
    keep if inlist(occupancytype, "1", "11") // single-family residential
    drop if inlist("1", agriculturestructureindicator, stateownedindicator) // not agricultural structure or state-owned
    keep if inlist("1", buildingdescriptioncode) | mi(buildingdescriptioncode) // main house

    * Drop irrelevant variables 
    assert policycount == "1" 
    drop policycount houseofworshipindicator agriculturestructureindicator stateownedindicator ///
        occupancytype buildingdescriptioncode nonprofit smallbusinessindicatorbuilding ///
        floodproofedindicator nfipcommunityname latitude longitude id lowestadjacentgrade ///
        obstructiontype basementenclosurecrawlspacetype programtypeindicator

    * Rename
    ren (propertystate reportedzipcode elevatedbuildingindicator primaryresidenceindicator ///
        postfirmconstructionindicator ratedfloodzone totalinsurancepremiumofthepolicy ///
        policycost totalbuildinginsurancecoverage) ///
        (state zipcode elevated primary_residence post_firm flood_zone premium ///
        policy_cost coverage_building)

    * Create additional variables
    // i) Policy year 
    gen policy_year = real(substr(policyeffectivedate, 1, 4))
    drop policyeffectivedate 
    // ii) Construction year 
    gen construction_year = real(substr(originalconstructiondate, 1, 4))
    // iii) Approximate property id
    // egen property_id = group(zipcode construction_year flood_zone nfipratedcommunitynumber) // (following Wagner, 2021) 
    gen geo_key = cond(missing(censusblockgroupfips) | censusblockgroupfips == "", ///
        "z" + zipcode, "b" + censusblockgroupfips)
    egen property_id = group(geo_key originalconstructiondate originalnbdate)
    drop geo_key
    // iv) SFHA (Special Flood Hazard Area )
    gen sfha = inlist(substr(flood_zone, 1, 1), "A", "V") if !mi(flood_zone) 
    // v) Risk Rating 2.0 
    gen risk_rating_2 = ratemethod == "RatingEngine"

    * Convert merge variables to date format
    foreach v of varlist originalnbdate originalconstructiondate {
        gen _d = date(substr(`v',1,10), "YMD")
        drop `v'
        rename _d `v'
        format `v' %td
    }

    * Destring variables 
    destring elevated primary_residence post_firm premium policy_cost coverage_building, replace

    * Drop if property cannot be identified 
    drop if missing(property_id)
    replace construction_year = . if !inrange(construction_year, 1700, 2027)
    drop if mi(construction_year)

    * Clean variables 
    // i) Elevations must be monotonic within property over time
    bysort property_id (policy_year): replace elevated = max(elevated, elevated[_n-1])
    // ii) Zip codes 
    replace zipcode = substr(trim(zipcode), 1, 5)
    replace zipcode = string(real(zipcode), "%05.0f") if !mi(zipcode) & length(zipcode) < 5
    assert mi(zipcode) | length(zipcode) == 5
    // iii) Premiums must be positive (zeros and negatives are voids and refunds)
    foreach var in premium policy_cost {
        replace `var' = . if `var' <= 0
    }

    * Deflate nominal variables to 2023 dollars
    ren policy_year year
    merge m:1 year using "`data'/clean/cpi.dta", keep(1 3) keepusing(cpi) nogen
    ren year policy_year
    foreach var in premium policy_cost coverage_building {
        replace `var' = `var' / cpi
    }
    drop cpi

    * Drop property-year duplicates 
    gen _premium = cond(mi(premium), -1, premium)
    bysort property_id policy_year (_premium): keep if _n == _N
    drop _premium
    isid property_id policy_year

    * -----------------------------------------------------------------------------
    * Section 2: Save dataset
    * -----------------------------------------------------------------------------

    * Keep restricted variable set 
    keep property_id state countycode nfipratedcommunitynumber zipcode censustract ///
        censusblockgroupfips construction_year policy_year flood_zone elevated ///
        primary_residence originalnbdate originalconstructiondate sfha post_firm ///
        premium policy_cost coverage_building risk_rating_2

    * Label 
    label var property_id              "Property ID"
    label var state                    "State"
    label var countycode               "County FIPS"
    label var nfipratedcommunitynumber "NFIP rated community number"
    label var zipcode                  "ZIP code"
    label var censustract              "Census tract"
    label var censusblockgroupfips     "Census block group"
    label var originalnbdate           "Original date of flood policy"
    label var originalconstructiondate "Original construction date"
    label var construction_year        "Construction year"
    label var policy_year              "Policy effective year"
    label var flood_zone               "NFIP rated flood zone"
    label var sfha                     "In SFHA (rated zone A/V)"
    label var post_firm                "Post-FIRM construction"
    label var elevated                 "Elevated home"
    label var primary_residence        "Primary residence"
    label var premium                  "Total insurance premium (2023 $)"
    label var policy_cost              "Policy cost incl. fees and surcharges (2023 $)"
    label var coverage_building        "Building insurance coverage (2023 $)"
    label var risk_rating_2            "Priced under Risk Rating 2.0"

    * Save
    order state property_id policy_year construction_year post_firm sfha primary_residence ///
        elevated risk_rating_2 premium policy_cost coverage_building
    order originalconstructiondate censusblockgroupfips originalnbdate, last
    sort state property_id policy_year
    compress 
    sa "`data'/clean/nfip_policies_state/`stl'.dta", replace

}

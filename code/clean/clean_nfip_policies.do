/******************************************************************************
Authors: Vendela Norman
Date: 2026-06-25

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
    ren (propertystate reportedzipcode elevatedbuildingindicator primaryresidenceindicator) ///
        (state zipcode elevated primary_residence)

    * Clean zipcode
    // Note: A few arrive as ZIP+4 (dashed or not), with a trailing dash/space, or
    // with the leading zero already stripped by FEMA -- all silently fail the zip
    // merge onto FMA. 
    replace zipcode = substr(trim(zipcode), 1, 5)
    replace zipcode = string(real(zipcode), "%05.0f") if !mi(zipcode) & length(zipcode) < 5
    assert mi(zipcode) | length(zipcode) == 5

    * Create additional variables
    // i) Policy year 
    gen policy_year = real(substr(policyeffectivedate, 1, 4))
    // ii) Construction year 
    gen construction_year = real(substr(originalconstructiondate, 1, 4))
    // iii) Approximate property id
    // egen property_id = group(zipcode construction_year ratedfloodzone nfipratedcommunitynumber) // (following Wagner, 2021) 
    gen geo_key = cond(missing(censusblockgroupfips) | censusblockgroupfips == "", ///
        "z" + zipcode, "b" + censusblockgroupfips)
    egen property_id = group(geo_key originalconstructiondate originalnbdate)
    drop geo_key
    // iv) SFHA (Special Flood Hazard Area )
    // Note: Not sure about current vs. rated flood zone distinction
    gen sfha = inlist(substr(ratedfloodzone, 1, 1), "A", "V") if !mi(ratedfloodzone) 
    drop policyeffectivedate 

    * Convert merge variables to date format
    foreach v of varlist originalnbdate originalconstructiondate {
        gen _d = date(substr(`v',1,10), "YMD")
        drop `v'
        rename _d `v'
        format `v' %td
    }

    * Destring variables 
    destring elevated primary_residence, replace
    /* ds id state zipcode countycode censustract censusblockgroupfips ///
       nfipratedcommunitynumber nfipcommunitynumbercurrent ///
       ratedfloodzone floodzonecurrent policyeffectivedate ///
       policyterminationdate originalnbdate, not
    destring `r(varlist)', replace */

    * -----------------------------------------------------------------------------
    * Section 2: Additional cleaning
    * -----------------------------------------------------------------------------

    * Additional sample restrictions
    // i) Drop homes in SFHAs 
    // Note: These are subject to different BCR calculations (flat, pre-calculated benefits)
    drop if sfha == 1
    drop sfha
    // ii) Drop if missing key variables
    drop if missing(property_id) 

    * Fix data errors 
    // i) Elevations must be monotonic (once 1, stays 1) within property over time
    bysort property_id (policy_year): replace elevated = max(elevated, elevated[_n-1])
    // ii) Construction year must be in plausible range 
    replace construction_year = . if !inrange(construction_year, 1700, 2027)
    drop if mi(construction_year)

    * Keep restricted variable set 
    // Note: Temporary. Will expand variable set later on nfip
    keep property_id state countycode nfipratedcommunitynumber zipcode censustract ///
        censusblockgroupfips construction_year policy_year ratedfloodzone elevated ///
        primary_residence originalnbdate originalconstructiondate

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
    label var ratedfloodzone           "NFIP rated flood zone"
    label var elevated                 "Elevated home"
    label var primary_residence        "Primary residence"

    * Save
    order property_id state zipcode censusblockgroupfips policy_year originalnbdate ///
        originalconstructiondate construction_year ratedfloodzone elevated
    order countycode nfipratedcommunitynumber censustract, last
    sort property_id state countycode zipcode censustract censusblockgroupfips policy_year
    compress 
    sa "`data'/clean/nfip_policies_state/`stl'.dta", replace

}

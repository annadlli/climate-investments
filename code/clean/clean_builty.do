/******************************************************************************
Author: Vendela Norman
Date: 2026-07-16

Description: Cleans the per-state Builty elevation permit data, restricting to 
    true elevations. 

******************************************************************************/

args data states

* Loop over states
local states tx va 
foreach st of local states {

    * Import data
    local stl = strlower("`st'")
    import delimited using "`data'/clean/builty_raw/`stl'.csv", ///
        clear varnames(1) stringcols(_all) bindquote(strict) maxquotedrows(unlimited)

    * Split description into permit subtype and work description
    // Note: Builty packs a permit's line items into one newline-delimited description
    // (84% of raw rows), which is why the browser shows "Electrical PermitElevator".
    // Line 1 is reliably the permit subtype and carries information nothing else in the
    // schema has -- record_type is "Building Permit" for 98% and work_types is usually
    // empty. Lines 2+ have no consistent meaning (of rows with 3+ lines, 90,245 are free
    // text vs 204 holding a value), so they stay together as the description.
    // Note: The 9% with no newline are descriptions, not subtypes ("RAISE REAR CENTER OF
    // HOUSE"), so they keep an empty subtype.
    gen permit_subtype = ""
    gen desc = description
    replace permit_subtype = substr(description, 1, strpos(description, char(10)) - 1) ///
        if strpos(description, char(10)) > 0
    replace desc = substr(description, strpos(description, char(10)) + 1, .) ///
        if strpos(description, char(10)) > 0
    replace desc = subinstr(desc, char(10), "; ", .)
    drop description
    rename desc description

    * Replace lowercase 
    replace permit_subtype = lower(permit_subtype)
    replace description = lower(description)

    * Drop commerical properties
    drop if property_type == "Commercial"
    assert property_type == "Residential" | mi(property_type)

    * Drop obvious non-elevations
       drop if strpos(permit_subtype, "electrical permit") > 0 & strpos(description, "elevator") > 0 
    drop if strpos(description, "buyout") > 0 
    drop if permit_subtype == "sign" 
    drop if strpos(permit_subtype, "tree permit") > 0 | strpos(description, "tree pruning") > 0 | strpos(description, "tree trimming") > 0


    * Restrict to true elevations
    // Note: Keep-based, not drop-based -- 83% of candidates are carried by a bare flood
    // word (floodplain development permits, flood-damage repairs) and are not elevations.
    // A permit survives on (i) act language, an explicit description of lifting the
    // structure, or (ii) compliance language (elevation certificate, ICC, meet-BFE,
    // freeboard) -- but compliance language only counts alongside structural work on an
    // existing dwelling, because coastal jurisdictions stamp it as boilerplate on any
    // permit in a flood zone (pools, generators, new construction built to code).
    gen subtype = lower(permit_subtype)
    // i) Act language: lifting the structure itself
    gen act = 0
    foreach p in ///
        "elevat(e|ed|ing|ion).{0,30}(house|home|residence|dwelling|structure|building|sfr)" ///
        "(house|home|residence|dwelling|structure|building).{0,30}(elevat|rais|lift)" ///
        "rais(e|ed|ing).{0,30}(house|home|residence|dwelling|structure|building|slab|foundation|pier|piling)" ///
        "lift(ed|ing)?.{0,30}(house|home|residence|structure)" ///
        "house raising|home raising|house lifting|structure raising|raising for flood" ///
        "jack(ed|ing)? ?up" ///
        "elevat(e|ed|ing).{0,30}(out of|above).{0,15}flood" {
        replace act = 1 if ustrregexm(description, "`p'") | ustrregexm(subtype, "`p'")
    }
    // ii) Compliance language + evidence of structural work on an existing dwelling
    gen code = 0
    foreach p in ///
        "elevation certificate" ///
        "increased cost of compliance" ///
        "(above|to|meet|meets|per).{0,15}(base flood|bfe)" ///
        "freeboard" {
        replace code = 1 if ustrregexm(description, "`p'") | ustrregexm(subtype, "`p'")
    }
    gen structural = ustrregexm(description, ///
        "remodel|renovat|repair|alteration|addition|improvement|foundation|pier|piling|substantial") ///
        | ustrregexm(subtype, "alteration|addition|repair|remodel")
    gen elevation = act | (code & structural)
    // iii) Zero out non-elevation senses that get past the positives
    // Elevator work (incl. "ELEV RENEWAL" = elevator permit renewals)
    replace elevation = 0 if ustrregexm(description, "elevator") ///
        | ustrregexm(subtype, "elevator|elev renewal|elev cert renewal")
    // Architectural "elevation" = a facade or drawing: tract-home variants ("Elevation: C",
    // "elevation g"), directional facades ("south building elevation"), plan sheets
    replace elevation = 0 if ustrregexm(description, "elevation ?:? ?[a-z]{1,2}(,|;|$| )") ///
        | ustrregexm(description, "(north|south|east|west|front|rear|side|left|right).{0,15}elevation") ///
        | ustrregexm(description, "elevation (drawing|plan|view|sheet|detail)")
    // Signs, banners, logos permitted per building face
    replace elevation = 0 if ustrregexm(description, "\bsign\b|banner|logo|channel letter") ///
        | ustrregexm(subtype, "\bsign\b|banner")
    // Raising a component or the grade, not the structure
    replace elevation = 0 if ///
        ustrregexm(description, "(rais(e|ed|ing)|elevat(e|ed|ing)) (the )?(roof|ceiling|curb|deck|patio|porch|walkway|platform|driveway|grade|yard|equipment|ductwork)") ///
        | ustrregexm(description, "raised ranch")
    // Permit types that are never a house elevation, boilerplate notwithstanding
    replace elevation = 0 if ustrregexm(subtype, "pool|spa\b|generator|drainage|fence|irrigation|sprinkler")
    // Not a single-family home
    replace elevation = 0 if ustrregexm(description, ///
        "townhouse|town house|townhome|town home|condo|apartment|duplex|triplex|4.?plex|multi.?family") ///
        | ustrregexm(subtype, "townhouse|condo|apartment|multi.?family")
    // New construction built to BFE/freeboard is code compliance, not an elevation of an
    // existing home -- unless the text explicitly describes lifting the structure
    replace elevation = 0 if act == 0 & ( ///
        ustrregexm(subtype, "new single family|new sfr|\bnsfr|new residence|new construction|new townhouse|building.?new|new.{0,10}residential|certificate of occupancy|residential model|(inside|outside)( of)? the floodplain") ///
        | ustrregexm(description, "new (1|2|one|two).{0,10}stor(y|ies)|new (single family|sfr|home|house|residence|dwelling|construction)|model home"))
    // iv) Keep what survives
    keep if elevation == 1
    drop elevation act code structural subtype

    * Convert dates to years
    // Note: raw dates are ISO strings (YYYY-MM-DD), so the year is the first 4 characters
    foreach v of varlist date_issued date_finaled date_submitted {
        local yv : subinstr local v "date" "year"
        gen `yv' = real(substr(`v', 1, 4))
        drop `v'
    }

    * Destring 
    destring project_value total_fees, replace 

    * Drop extraneous variables 
    drop property_type builty_id 

    * Save 
    order state fips_state county fips_county zipcode locality cbsa fips_cbsa street_address 
    order status apn, last 


    stop 

    /*

    * Clean zipcode
    // Note: Unlike NFIP's, Builty's short zips are not recoverable -- "1"/"01"/"001" are
    // not leading-zero-stripped zips, they are corrupt. Blank them rather than pad.
    // Note: zipcode stays a string (CONVENTIONS section 5).
    replace zipcode = trim(zipcode)
    replace zipcode = "" if length(zipcode) != 5 | zipcode == "00000"
    assert mi(zipcode) | length(zipcode) == 5

    * Check permit records are unique
    isid builty_id

    * Label variables
    label var builty_id      "Builty permit ID"
    label var state          "State"
    label var county         "County name"
    label var fips_county    "County FIPS"
    label var fips_state     "State FIPS"
    label var cbsa           "CBSA"
    label var fips_cbsa      "CBSA FIPS"
    label var locality       "Permit-issuing locality"
    label var street_address "Street address (links to ATTOM)"
    label var zipcode        "ZIP code"
    label var apn            "Assessor parcel number"
    label var permit_year    "Year permit issued"
    label var date_issued    "Permit issue date"
    label var date_finaled   "Permit final date"
    label var date_submitted "Permit submission date"
    label var description    "Permit description (screened downstream)"
    label var property_type  "Property type (missing ~30%)"
    label var work_types     "Work types"
    label var project_value  "Project value (nominal $)"
    label var total_fees     "Permit fees (nominal $)"
    label var status         "Permit status"

    * Save
    order builty_id state fips_state county fips_county locality street_address ///
        zipcode apn permit_year date_issued date_finaled description property_type
    order cbsa fips_cbsa work_types project_value total_fees status date_submitted, last
    sort state county zipcode permit_year
    compress
    save "`data'/clean/builty_permits_`stl'.dta", replace
    */

}

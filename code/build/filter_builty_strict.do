/******************************************************************************
Authors: Anna Li 
Date: 2026-07-20
Description: Applies a strict home-elevation filter to the loose all-state
    Builty elevation-candidate file produced by the Python extraction step.
    Identifies structural home elevations and removes reviewed false positives.
	This is the file to change in future for further filters.
******************************************************************************/

args data

* Import the filtered all-state file produced by build_builty_filter.py
use "`data'/build/all_builty_elevations.dta", clear

* Standardize the raw uppercase Builty column names
rename *, lower

* Restrict the sample to permits identified as residential
gen residential = lower(ustrtrim(property_type)) == "residential"
keep if residential == 1


* Select the first available permit date, prioritizing the issue date
gen permit_date = date_issued
replace permit_date = date_submitted if mi(permit_date)
replace permit_date = date_finaled if mi(permit_date)

* Extract the permit year
gen permit_year = real(substr(permit_date, 1, 4))

* Create variables used by the downstream ATTOM merge
clonevar street = street_address
* Five-digit county FIPS (state + county) — the ATTOM merge keys on this
gen county_fips = fips_state + fips_county
clonevar job_value = project_value

*replace punctuation with spaces
replace desc_l = ustrregexra(desc_l, "[/()\.,-]", " ")

* Collapse repeated whitespace created during punctuation removal
replace desc_l = ustrregexra(desc_l, "\s+", " ")

//-------------------------------------------------------------------
//Apply filters
* Flag descriptions that explicitly describe lifting or raising a home
gen byte flood_elev_strict = 0
foreach pattern in ///
    "(^| )house elevation($| )" ///
    "(^| )home elevation($| )" ///
    "(^| )residential house elevation($| )" ///
    "flood damage house elevation" ///
    "flood damaged house elevation" ///
    "elevat(e|ing) (existing )?(house|home)( |$)" ///
    "rais(e|ed|ing) (the )?(house|home)( |$)" ///
    "(^| )(house|home)( |$).{0,40}rais(e|ed|ing)" ///
    "(^| )(house|home)( |$).{0,40}elevat" ///
    "lowest floor of the house.{0,50}elevat" ///
    "raise house to fema" ///
    "raised to meet elevation requirements" ///
    "elevation of existing home" ///
    "floodplain.{0,40}elevat" ///
    "elevat.{0,40}floodplain" ///
    "flood zone.{0,40}(elevat|rais)" ///
    "(elevat|rais).{0,40}flood zone" ///
    "(rais|elevat).{0,40}base flood elevation" ///
    "base flood elevation.{0,40}(rais|increas|meet|comply|above|compli)" ///
    "freeboard" ///
    "nfip.{0,40}(rais|elevat)" ///
    "(rais|elevat).{0,40}nfip" ///
    "hazard mitigation.{0,40}(rais|elevat| home | house |structur|residen)" ///
    "(rais|elevat).{0,40}hazard mitigation" ///
    "substantially (damaged|improved).{0,40}(rais|elevat)" ///
    "(rais|elevat).{0,40}substantially (damaged|improved)" ///
    "sfr.{0,30}(elevat|rais)" ///
    "(elevat|rais).{0,30}sfr" ///
    "residential.{0,30}(elevat|rais).{0,30}(flood|fema|bfe|mitigation)" ///
    "lift(ed|ing).{0,40}(house|home|structur|residen|dwelling|out of)" ///
    "(^| )(house|home)( |$).{0,40}lift(ed|ing)" ///
    "(structur|residen|dwelling).{0,40}lift(ed|ing)" ///
    "lift(ed|ing).{0,40}(floodplain|flood|fema|icc)" ///
    "out of (the )?floodplain" ///
    "jack(ed|ing) up.{0,40}(house|home|structur|residen)" {
    replace flood_elev_strict = 1 if ustrregexm(desc_l, `"`pattern'"')
}

//added as supplement: not used as actual restriction 
* Flag descriptions that mention flood, FEMA, BFE, or mitigation context
gen byte flood_adaptation_context = 0
foreach pattern in ///
    "flood" "fema" "bfe" "base flood elevation" "floodplain" ///
    "flood plain" "flood zone" "freeboard" "nfip" "icc" ///
    "hazard mitigation" "mitigation" "substantially damaged" ///
    "substantial damage" "substantially improved" ///
    "substantial improvement" "storm surge" {
    replace flood_adaptation_context = 1 if ustrregexm(desc_l, `"`pattern'"')
}


* Flag new construction, elevators, architectural elevations, and other
* reviewed non-elevation uses of the strict search terms
gen byte flood_elev_falsepos = 0
foreach pattern in ///
    "flood damage repair" ///
    "new construction" ///
    "elevator" ///
    "pool" ///
    "mobile.{0,20}home.{0,20}move.{0,20}in" ///
    "manufactured.{0,20}home.{0,20}move.{0,20}in" ///
    "mobile.{0,20}home.{0,20}set.{0,20}up" ///
    "elevation.{0,20}certificate|elevation.{0,20}assessor|elevation assessor's permit" ///
    "model house|elevatoions" ///
    "determination of substantial conformance.{0,120}elevations?" ///
    "site modifications.{0,80}elevations?" ///
    "townhouse|town house" ///
    "elevation *#? *[0-9]+" ///
    "(house type|model|master *file|masterfile).{0,100}elevation" ///
    "elevation.{0,100}(house type|model|master *file|masterfile)" ///
    "(build new|new).{0,80}(home|townhouse|town house|sfd|single family|custom home|residence).{0,100}elevation" ///
    "(unit|lot) +[a-z0-9]+.{0,40}elevation" ///
    "plan *#? *[0-9a-z-]+.{0,80}elevations?" ///
    "elevations?.{0,80}(std gate|standard plan|master|tract|lot [0-9]+|nsfr|new sfr|new house)" ///
    "(new sfr|new house|new single family|single family home|single family residential).{0,100}elevations?" ///
    "elevations?.{0,100}(new sfr|new house|new single family|single family home|single family residential)" ///
    "(front|rear|side|north|south|east|west|left|right).{0,30}elevations?" ///
    "elevations?.{0,30}(front|rear|side|north|south|east|west|left|right)" ///
    "(left|right) swing elevation" ///
    "plan[: ]+[0-9a-z ]+.*elevation[: ]+[a-z]" ///
    "elevation (drawing|plan|view|sheet|detail)" ///
    `"elevation ['"]?[a-z][0-9]?['"]?( |$|,|;|:|\.)"' ///
    `"elevations? ['"]?([a-z]|[ivx]+)[0-9]?['"]?( |$|,|;|:|\.)"' ///
    `"(^| )['"]?[a-z][0-9]?['"]? elevation"' ///
    "repeat.{0,40}elevation" ///
    "elevation[- ]+[a-z][0-9]?( |$|,|;|:|\.)" ///
    "(grade|grading|pad|site|curb|street|road|drain) elevation" ///
    "raised slab|elevated slab" ///
    "raised ranch" ///
    "rais(e|ed|ing).{0,40}roof|roof.{0,40}rais(e|ed|ing)" ///
    "rais(e|ed|ing).{0,40}ceiling|ceiling.{0,40}rais(e|ed|ing)" ///
    "rais(e|ed|ing).{0,40}(porch|deck|entry|garden|loop|meter|panel|wire|collar ties)" ///
    "(porch|deck|entry|garden|loop|meter|panel|wire|collar ties).{0,40}rais(e|ed|ing)" ///
    "service.{0,40}rais(e|ed|ing)|rais(e|ed|ing).{0,40}service" ///
    "shower faucet|shower plumbing" ///
    "fence permit|rock ?wall|privacy fence" ///
    "back yard|front yard|side yard|rear yard" ///
    "tree removal|tree pruning|prun(e|ing)|canopy|oak tree|laurel oak|roots.{0,60}(foundation|sidewalk|patio|deck)" ///
    "elevat(e|ed|ing).{0,60}(tree|canopy|limb|roof)|(tree|canopy|limb).{0,60}elevat(e|ed|ing)" ///
    "sign.{0,80}house raising|house raising.{0,80}sign" ///
    "code compliance.{0,100}house raising" ///
    "generator" ///
    "water heater" ///
    "signage|wall sign|channel letters" ///
    "illuminated.{0,30}(sign|letter|cabinet)" ///
    "finished floor" ///
    "minimum ffe|minimun ffe|min ffe" ///
    "elevation certificate" ///
    "flood plain determination" ///
    "raise (the )?roof" ///
    "raise (the )?bar" ///
    "patio addition rear elevation" ///
    "front elevation refacing" ///
    "new (home|sfr|single family|residence).{0,80}(plan |elevation [a-z]( |$))" ///
    "(plan |master plan ).{0,30}elevation [a-z]( |$)" ///
    "new (sfr|single family).{0,30}existing elevation" {
    replace flood_elev_falsepos = 1 if ustrregexm(desc_l, `"`pattern'"')
}

* Define the final indicator from the strict-text and false-positive flags
gen byte flood_elev_final = flood_elev_strict == 1 & ///
    flood_elev_falsepos == 0

* Restrict the output to final strict home-elevation permits
keep if flood_elev_final == 1

* Drop the temporary standardized description used by the text filters
drop desc_l

*organize data
order builty_id state fips_state county fips_county county_fips locality ///
    street_address street zipcode permit_date permit_year
sort state county_fips zipcode permit_year

* Save
compress
save "`data'/build/all_builty_elevations_strict.dta", replace

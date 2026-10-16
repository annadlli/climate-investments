/******************************************************************************
Authors: Vendela Norman and Anna Li
Date: 2026-10-02

Description: Cleans the per-state ATTOM building-permit elevation candidates,
    restricting to true home elevations, and collapses them to one row per address.

******************************************************************************/

args data states

* Set switches
local screen = 1 // screen by state for true elevations 
local clean  = 1 // clean and save final dataset

* -----------------------------------------------------------------------------
* Section 1: Screen elevation permits by state
* -----------------------------------------------------------------------------

* Loop over states
if `screen' == 1 {
    foreach st of local states {

        * Import data
        local stl = strlower("`st'")
        import delimited using "`data'/clean/permits_raw/`stl'.csv", ///
            clear varnames(1) stringcols(_all) bindquote(strict) maxquotedrows(unlimited)

        * Split description into permit subtype and work description
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

        * Drop obvious non-elevations
        drop if strpos(permit_subtype, "electrical permit") > 0 & strpos(description, "elevator") > 0 
        drop if strpos(description, "buyout") > 0 
        drop if permit_subtype == "sign" 
        drop if strpos(permit_subtype, "tree permit") > 0 | strpos(permit_subtype, "tree pruning") > 0 ///
            | strpos(permit_subtype, "tree trimming") > 0 | strpos(permit_subtype, "tree removal") > 0

        * Restrict to true elevations
        gen subtype = lower(permit_subtype)
        gen desc = lower(description)
        replace desc = subinstr(desc, char(10), " ; ", .)
        replace desc = subinstr(desc, char(13), " ",   .)

        local N "house|home|residence|dwelling|structure|building|sfr|s\.f\.r"

        // i) Act language: lifting the structure itself
        gen act = 0
        foreach p in ///
            "el[ae]vat(e|ed|ing|ion)[^.;]{0,30}(`N')" ///
            "(`N')[^.;]{0,30}(el[ae]vat|\brais(e|ed|ing)\b|\blift)" ///
            "\brais(e|ed|ing)\b[^.;]{0,30}(`N'|slab|foundation|pier|piling)" ///
            "\blift(ed|ing)?\b[^.;]{0,30}(`N')" ///
            "house raising|home raising|house lifting|structure raising|raising for flood" ///
            "jack(ed|ing)? ?up" ///
            "el[ae]vat(e|ed|ing)[^.;]{0,30}(out of|above)[^.;]{0,15}flood" {
            replace act = 1 if ustrregexm(desc, "`p'") | ustrregexm(subtype, "`p'")
        }

        // ii) Compliance language + structural work on an existing dwelling
        gen code = 0
        foreach p in ///
            "elevation certificate" ///
            "increased cost of compliance" ///
            "(above|to|meet|meets|per)[^.;]{0,15}(base flood|bfe)" ///
            "freeboard" {
            replace code = 1 if ustrregexm(desc, "`p'") | ustrregexm(subtype, "`p'")
        }
        gen structural = ustrregexm(desc, ///
            "remodel|renovat|repair|alteration|addition|improvement|foundation|pier|piling|substantial") ///
            | ustrregexm(subtype, "alteration|addition|repair|remodel")

        // iii) Strong retrofit language
        // elevation document or a survey not counted now (ie elevation certificate, boundary survey)
        gen strong = 0
        foreach p in ///
            "el[ae]vat(e|ed|ing|ion) [^.;]{0,10}(existing|the) (`N')" ///
            "(`N') (elevation|raising|lifting)\b(?!,? ?(certificate|cert\b|verification|boundary|survey|requirement|not\b|or\b))" ///
            "\brais(e|ing)\b [^.;]{0,10}(existing|the|a)? ?(`N'|double ?wide|mobile home)" ///
            "\braised\b (existing|the|a) (`N')" ///
            "\bel[ae]vate (`N')\b" ///
            "house raising|home raising|house lifting|jack(ed|ing)? ?up" ///
            "(rais(e|ed|ing)|el[ae]vat(e|ed|ing)) (`N'|foundation)[^.;]{0,15}above[^.;]{0,20}(flood|bfe|base flood)" ///
            "flood[^.;]{0,20}(house|home) elevation" ///
            "home elevation permit|(?<!meet )(?<!meets )(?<!match )elevation of (the )?(house|home|residence|dwelling|structure|existing)" ///
            "elevating (the )?(existing|house|home|residential)" ///
            "\brais(e|ed|ing)\b (`N') (out of|above)" ///
            "el[ae]vat(e|ed|ing) (`N') out of" {
            replace strong = 1 if ustrregexm(desc, "`p'") | ustrregexm(subtype, "`p'")
        }

        gen elevation = act | (code & structural)

        // iv) Negations
        // Elevator work
        replace elevation = 0 if ustrregexm(desc, "elevator") ///
            | ustrregexm(subtype, "elevator|elev renewal|elev cert renewal")
        // Cargo/boat/accessibility lifts (stilt-home installs quote elevation compliance)
        replace elevation = 0 if ustrregexm(desc, "(cargo|boat|chair|wheelchair|platform|vertical) lift")
        // Architectural "elevation": facade, drawing, tract-home plan variant
        replace elevation = 0 if ustrregexm(desc, "elevation ?:? ?(?!of\b|to\b|in\b|at\b|on\b|is\b|be\b|as\b|or\b|an\b|if\b|it\b|up\b|by\b|no\b|so\b|re\b)[a-z]{1,2}(,|;|$|\)| )") ///
            | ustrregexm(desc, "\b(block|lot|unit|plan|model|plex[a-z]*|swatch|farmhouse|craftsman)\b[^.]{0,30}elevation\b") ///
            | ustrregexm(desc, "elevation\b[^.]{0,25}\b(unit|block|lot)\b") ///
            | ustrregexm(desc, "elevation [a-z]{1,2}[0-9]+[a-z]*\b")
        replace elevation = 0 if ustrregexm(desc, "\b(north|south|east|west|front|rear|back|side|left|right)\b[^.;]{0,15}elevation") ///
            | ustrregexm(desc, "elevation (north|south|east|west|front|rear|drawing|plan|view|sheet|detail)") ///
            | ustrregexm(desc, "no change in elevation")
        // Signs, banners, logos
        replace elevation = 0 if ustrregexm(desc, "\bsign\b|banner|logo|channel letter|raceway|illum") ///
            | ustrregexm(subtype, "\bsign\b|banner")
        // Raising a component or the grade, not the structure (incl. "raise area")
        replace elevation = 0 if ustrregexm(desc, ///
            "(\brais(e|ed|ing)|elevat(e|ed|ing)|\blift(ed|ing)?)( \w+){0,2} (roof|ceil\w*|curb|deck|patio|porch|walkway|platform|driveway|grade|yard|area\b|land\b|equipment|ductwork|planter|floor\b|water ?heater|meter\b|panel\b|seating|door|header|lots?\b)") ///
            | ustrregexm(desc, "raised ranch")
        // Equipment installs: kW-rated/branded signature
        replace elevation = 0 if ustrregexm(desc, ///
            "\d+ ?kw\b|--kw|fueled generator|generator (pad|install)|install [^.;]{0,25}generator|portable generator|generac|kohler generator|cummins generator|briggs")
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            "\bgenerator\b|a/?c change ?out|changeout|condenser|mini.?split|heat pump|\bahu\b|hvac (equipment|change|unit)")
        // Timestamped review-comment logs "(3/21/2012 9:10 am skw);"
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            "\(\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2} ?(am|pm)? ?[a-z]{2,4}\)")
        // FIRM / flood-map determination stamps (incl. panel:/dfe:/bfe: signatures)
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            "effective (firm|flood) (panel|map)|firm panel|flood map dated|flood study|\bloma\b|\blomr\b|\bpanel ?: ?\d{4,}|\bdfe ?:|\bbfe ?: ?\d")
        // Text that says no elevation is involved (a determination, not work). Skipped when the permit has strong retrofit language
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            "not a substantial improvement|no elevation certificate|elevation (certificate|verification)( is)? not|(pre|post).?firm structure|in zone x\b")
        //review stamps some offices put on every permit 
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            "elevation (certificate|verification|cert\.?)[^.;]{0,30}not required|no (flood )?elevation (certificate|cert)|boundary survey|\biecc\b|\bno elevation\b|base flood elevation[^.;]{0,20}not established")
        // Tree work ("oak tree ... lifting house"; "trim tree elevating over home"; "raise canopy")
        replace elevation = 0 if ustrregexm(subtype, "\btrees?\b|\bprun|arborist")
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            "\b(oak|laurel|palm|pine|maple|magnolia) tree|tree removal|remov(e|al) [^.;]{0,15}tree|trim [^.;]{0,20}tree|arborist|\bstump\b" ///
            + "|\btrees?\b[^.;]{0,25}(lift|lean|fell|fall|damag)|\bprun(e|ed|ing)\b|raise (the |up )?canopy|(trim|cut)[^.;]{0,15}\b(live )?(oaks?|palms?|pines?|branches)\b")
        // Like-for-like storm repairs (FL convention: stamped repair permits)
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            "size for size|re-?roof|tear off|reshingle")
        // Pool permits
        replace elevation = 0 if ustrregexm(desc, "swimming pool/spa;|^ ?(swimming )?pool\b")
        replace elevation = 0 if strong == 0 & ustrregexm(subtype, "pool|spa\b|generator|drainage|fence|irrigation|sprinkler")
        // Not a single-family home
        replace elevation = 0 if ustrregexm(desc, ///
            "townhouse|town house|townhome|town home|condo|apartment|(?<!family[/ ])duplex|triplex|4.?plex|multi.?family") ///
            | ustrregexm(subtype, "townhouse|condo|apartment|multi.?family")
        // Mobile/manufactured home move-ins
        replace elevation = 0 if strong == 0 & (ustrregexm(desc, "mov(e|ing)[ -]?in\b") ///
            | ustrregexm(subtype, "move[ -]?in"))
        // Foundation repair / releveling; inch-scale scope caps
        // NB: compound quotes -- pattern contains a literal " for inch marks
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            `"re-?level|releveling|reshim|shimming|\bunderpin|remedial|stabiliz|pressed (concrete |steel )?pil|mud ?jack|slab ?jack|foam ?jack|concrete leveling|compaction grout|level(ing)? (the |existing )?(house|home|slab|foundation)|(raise|lift) (and|&) level|(no higher|not to exceed)[^.;]{0,15}\d{1,2} ?("|''|in\.|inch)|install (of )?\d+[^.;]{0,15}(piers?|pin piles)|\d+ (concrete|steel|pressed)[^.;]{0,20}piers?"')
        // Accessory structures via the code path
        replace elevation = 0 if act == 0 & (ustrregexm(desc, ///
            "storage (building|shed)|\bshed\b|detached garage|carport|accessory (structure|building|bldg)|gazebo|pergola|pool house|\bbarn\b") ///
            | ustrregexm(subtype, "accessory|storage"))
        // Stamped-notice boilerplate passing code & structural
        replace elevation = 0 if act == 0 & ustrregexm(desc, ///
            "construction must comply with|smoke detectors, if not existing|-{10,}|\*{6,}")
        // Demolition permits
        replace elevation = 0 if strong == 0 & (ustrregexm(desc, "\bdemo(lition|lish)?\b[^.;]{0,60}(rais|elevat)") ///
            | ustrregexm(subtype, "demolition"))
        // Lift stations; sewer/drain plumbing
        replace elevation = 0 if ustrregexm(desc, "lift station|lift-station")
        replace elevation = 0 if strong == 0 & ustrregexm(desc, "sewer (line|replacement)|drain (line|is)|septic")

        // v) Keep what survives; elevated new construction stays in the file with a flag
        keep if elevation == 1
        gen new_construction = strong == 0 & ( ///
            ustrregexm(subtype, "new single family|new sfr|\bnsfr|new residence|new construction|new townhouse|building.?new|new.{0,10}residential|certificate of occupancy|residential model|(inside|outside)( of)? the floodplain") ///
            | ustrregexm(desc, "new[- ]?(1|2|one|two)[^.;]{0,10}stor(y|ies)|new (single family|sfr|home|house|residence|dwelling|construction)|new [12]/s\b|new s/f\b|(new|modular) [^.;]{0,15}home elevated on|building a (new )?(single family|home|house|residence)|proposed (house|home|residence|dwelling)|model home"))
        gen retrofit = new_construction == 0
        drop elevation act code strong structural subtype desc

        * Save
        save "`data'/clean/permit_states/permit_elevations_`stl'.dta", replace
    } 
}

* -----------------------------------------------------------------------------
* Section 2: Append and clean final data
* -----------------------------------------------------------------------------

* Append all states
clear
foreach st of local states {
    local stl = strlower("`st'")
    append using "`data'/clean/permit_states/permit_elevations_`stl'.dta"
}

* Permit issue year (ATTOM carries the issue date on the permit record)
gen year_issued = real(substr(date_issued, 1, 4))
drop date_issued

* Status-history years (first date of each stage in the ATTOM status table)
foreach v in filed finaled expired {
    capture confirm variable date_`v'
    if _rc gen date_`v' = ""
    gen year_`v' = real(substr(date_`v', 1, 4))
    drop date_`v'
}
capture confirm variable status_last
if _rc gen status_last = ""
//year of the last status row, the fallback event year
capture confirm variable status_last_date
if _rc gen status_last_date = ""
gen year_last = real(substr(status_last_date, 1, 4))

foreach v in n_final_rows n_kill_rows n_reinstated_rows {
    capture confirm variable `v'
    if _rc gen `v' = .
    capture confirm string variable `v'
    if !_rc destring `v', replace force // the CSV import reads an all-empty column as string
}

* Destring
destring project_value total_fees, replace

* Clean up variables 
replace zipcode = substr(zipcode, 1, 5) if length(zipcode) > 5
replace zipcode = string(real(zipcode), "%05.0f") if !mi(zipcode) & length(zipcode) < 5

* Flag funding source from permit text (unknown for most)
local cue "grant|fund|award|assist|program|reimburs|financ"
gen fund_fema = ustrregexm(description, "\bfema\b[^.;]{0,40}(`cue')|(`cue')[^.;]{0,40}\bfema\b")
gen fund_hmgp = ustrregexm(description, "\bhmgp\b|hazard mitigation grant")
gen fund_fma  = ustrregexm(description, "\bfma\b|flood mitigation assistance")
gen fund_bbb  = ustrregexm(description, "build(ing)? back better")

* Collapse to address level
// Note: one project can carry several permits (building, electrical, plumbing); the  ATTOMID is the one ATTOM assigned to the permit's address (first non-missing)
gen one = 1
duplicates drop
sort state county street_address year_issued
collapse (firstnm) fips_state fips_county zipcode locality attomid ///
    (firstnm) permit_subtype description status status_last ///
    (min) year_issued year_filed year_finaled year_expired (max) year_last ///
    (max) n_final_rows n_kill_rows n_reinstated_rows ///
    (max) project_value total_fees ///
    (max) fund_fema fund_hmgp fund_fma fund_bbb ///
    (max) retrofit new_construction ///
    (sum) n_permits = one, by(state county street_address)

* Elevation event year: finaled year, else last status year, else issue year
gen year = year_finaled
replace year = year_last   if mi(year)
replace year = year_issued if mi(year)
quietly count if !mi(year_finaled)
di "event year = finaled year for " r(N) " addresses"
quietly count if mi(year_finaled) & !mi(year_last)
di "event year = last status year for " r(N) " addresses"
quietly count if mi(year_finaled) & mi(year_last)
di "event year = issue year for " r(N) " addresses (no status history)"

* Merge in CPI and deflate nominal variables to 2023 dollars on the issue year (when job value is declared)
rename year year_event
rename year_issued year
merge m:1 year using "`data'/clean/cpi.dta", keep(1 3) keepusing(cpi) nogen
rename year year_issued
rename year_event year
foreach var in project_value total_fees {
    replace `var' = `var' / cpi if !mi(`var') 
}
// a new build's declared value is the house, not an elevation; values under $1k are fees
replace project_value = . if new_construction
replace project_value = . if project_value < 1000

* Encode funding source: later lines win, so a named program beats the generic FEMA cue
gen funding_type = 0
replace funding_type = 1 if fund_fema
replace funding_type = 5 if fund_bbb
replace funding_type = 3 if fund_fma
replace funding_type = 2 if fund_hmgp
label define funding_type_lbl 0 "Unknown" 1 "FEMA" 2 "HMGP" 3 "FMA" 5 "Build Back Better"
label values funding_type funding_type_lbl

* Drop extraneous variables
drop cpi fund_*

* Completion: some permit at the address reached FINAL or COMPLETE in the status history
gen completed = n_final_rows > 0 if !mi(n_final_rows)
replace completed = !mi(year_finaled) if mi(completed)
quietly count if completed & mi(year_finaled)
di "completed with no dated completion row (undated FINAL / COMPLETE): " r(N) " addresses"
drop n_final_rows n_kill_rows n_reinstated_rows

* Label variables
label var year             "Elevation event year: finaled, else last status, else issue year" 
label var year_issued      "Permit issue year" 
label var year_last        "Year of the last status row (status history)" 
label var state            "State"
label var fips_state       "State FIPS"
label var county           "County name"
label var fips_county      "County FIPS"
label var zipcode          "ZIP code"
label var locality         "Site address city"
label var street_address   "Street address (links to ATTOM)"
label var attomid          "ATTOM property ID assigned to the permit (empty if unmatched)"
label var permit_subtype   "Permit type - subtype (line 1 of raw description)"
label var description      "Permit description (screened for elevation)"
label var project_value    "Declared job value (2023 $)"
label var total_fees       "Permit fees (2023 $)"
label var status           "Permit status (snapshot on the permit record)"
label var status_last      "Latest status in the ATTOM status history"
label var year_filed       "Year first filed or applied (status history)"
label var year_finaled     "Year first finaled or completed (status history)"
label var year_expired     "Year expired, cancelled, withdrawn, revoked or rejected, not reinstated (status history)" 
label var completed        "Some permit at the address has a FINAL or COMPLETE row, dated or not (status history)"
label var n_permits        "ATTOM permits at this address"
label var funding_type     "Funding source (from permit text)"
label var retrofit         "Elevation permit on an existing structure (any permit)"
label var new_construction "Elevated new construction (any permit)"

* Save final dataset
sort state county zipcode street_address year
order state fips_state county fips_county zipcode locality street_address attomid ///
    year year_issued year_last year_filed year_finaled year_expired completed status status_last ///
    funding_type retrofit new_construction
compress
save "`data'/clean/permit_elevations.dta", replace

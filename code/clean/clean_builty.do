/******************************************************************************
Author: Vendela Norman
Date: 2026-07-23

Description: Cleans the per-state Builty elevation permit data, restricting to 
    true elevations. 

******************************************************************************/

args data states

* Set switches
local screen = 0 // screen by state for true elevations
local clean  = 1 // clean and save final dataset

* -----------------------------------------------------------------------------
* Section 1: Screen elevation permits by state
* -----------------------------------------------------------------------------

* Loop over states
if `screen' == 1 {
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
        drop if strpos(permit_subtype, "tree permit") > 0 | strpos(permit_subtype, "tree pruning") > 0 ///
            | strpos(permit_subtype, "tree trimming") > 0 | strpos(permit_subtype, "tree removal") > 0

        * Restrict to true elevations
        // Keep-based. A permit survives on (i) act language, (ii) compliance
        // language + structural work on an existing dwelling. Negations then
        // remove non-elevation senses. "strong" = unambiguous retrofit
        // language that shields true elevations from context-based kills.
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
        gen strong = 0
        foreach p in ///
            "el[ae]vat(e|ed|ing|ion) [^.;]{0,10}(existing|the) (`N')" ///
            "(`N') (elevation|raising|lifting)\b" ///
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
        // Equipment installs: kW-rated/branded signature = UNCONDITIONAL kill
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
        // Negated determinations -- gated on STRONG (the stamp fakes act language)
        replace elevation = 0 if strong == 0 & ustrregexm(desc, ///
            "not a substantial improvement|no elevation certificate|elevation (certificate|verification)( is)? not|(pre|post).?firm structure|in zone x\b")
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
        // New construction built to code
        replace elevation = 0 if strong == 0 & ( ///
            ustrregexm(subtype, "new single family|new sfr|\bnsfr|new residence|new construction|new townhouse|building.?new|new.{0,10}residential|certificate of occupancy|residential model|(inside|outside)( of)? the floodplain") ///
            | ustrregexm(desc, "new[- ]?(1|2|one|two)[^.;]{0,10}stor(y|ies)|new (single family|sfr|home|house|residence|dwelling|construction)|new [12]/s\b|new s/f\b|(new|modular) [^.;]{0,15}home elevated on|building a (new )?(single family|home|house|residence)|proposed (house|home|residence|dwelling)|model home"))

        // v) Keep what survives
        keep if elevation == 1
        drop elevation act code strong structural subtype desc

        * Save
        save "`data'/clean/builty_states/builty_elevations_`stl'.dta", replace
    } 
}

* -----------------------------------------------------------------------------
* Section 2: Append and clean final data
* -----------------------------------------------------------------------------

* Append all states
clear
foreach st of local states {
    local stl = strlower("`st'")
    append using "`data'/clean/builty_states/builty_elevations_`stl'.dta"
}

* Drop some additional observations 
drop if strpos(description, "anthropology department building") > 0 // billion $ Yale project

* Convert dates to years
foreach v of varlist date_issued date_finaled date_submitted {
    local yv : subinstr local v "date" "year"
    gen `yv' = real(substr(`v', 1, 4))
    drop `v'
}

* Destring
destring project_value total_fees, replace

* Clean up variables 
replace project_value = . if project_value == 1
replace zipcode = string(real(zipcode), "%05.0f") if !mi(zipcode) & length(zipcode) < 5
egen year = rowmin(year_issued year_finaled year_submitted)
replace year = year_issued if year == 1900

* Flag funding source from permit text (unknown for most)
local cue "grant|fund|award|assist|program|reimburs|financ"
gen fund_fema = ustrregexm(description, "\bfema\b[^.;]{0,40}(`cue')|(`cue')[^.;]{0,40}\bfema\b")
gen fund_hmgp = ustrregexm(description, "\bhmgp\b|hazard mitigation grant")
gen fund_fma  = ustrregexm(description, "\bfma\b|flood mitigation assistance")
gen fund_sfha = ustrregexm(description, "\bsfha\b|special flood hazard area")
gen fund_bbb  = ustrregexm(description, "build(ing)? back better")

* Collapse to property level
// Note: Builty logs multiple transaction rows per project (inspection, construction, ...)
gen one = 1
duplicates drop
sort state county street_address year_issued          // so (firstnm) = earliest permit
collapse (firstnm) fips_state fips_county zipcode locality cbsa fips_cbsa ///
    (firstnm) permit_subtype description status ///
    (min) year year_issued year_finaled year_submitted ///
    (max) project_value total_fees ///
    (max) fund_fema fund_hmgp fund_fma fund_sfha fund_bbb ///
    (sum) n_permits = one, by(state county street_address)

* Merge in CPI and deflate nominal variables to 2023 dollars
merge m:1 year using "`data'/clean/cpi.dta", keep(1 3) keepusing(cpi) nogen
foreach var in project_value total_fees {
    replace `var' = `var' / cpi if !mi(`var') 
}

* Encode funding source 
gen byte funding_type = 0
replace funding_type = 4 if fund_sfha
replace funding_type = 1 if fund_fema
replace funding_type = 5 if fund_bbb
replace funding_type = 3 if fund_fma
replace funding_type = 2 if fund_hmgp
label define funding_type_lbl 0 "Unknown" 1 "FEMA" 2 "HMGP" 3 "FMA" 4 "SFHA" 5 "Build Back Better"
label values funding_type funding_type_lbl

* Drop extraneous variables
drop cpi year_issued year_finaled year_submitted fund_*

* Label variables
label var year           "Permit year (earliest of issued, finaled, submitted)"
label var state          "State"
label var fips_state     "State FIPS"
label var county         "County name"
label var fips_county    "County FIPS"
label var cbsa           "CBSA"
label var fips_cbsa      "CBSA FIPS"
label var zipcode        "ZIP code"
label var locality       "Permit-issuing locality"
label var street_address "Street address (links to ATTOM)"
label var permit_subtype "Permit subtype (line 1 of raw description)"
label var description    "Permit description (screened for elevation)"
label var project_value  "Project value (2023 $)"
label var total_fees     "Permit fees (2023 $)"
label var status         "Permit status"
label var n_permits      "Builty permits at this property"
label var funding_type   "Funding source (from permit text)"

* Save final dataset
sort state county zipcode fips_cbsa street_address year
order state fips_state county fips_county zipcode locality cbsa fips_cbsa street_address ///
    year status funding_type
compress
save "`data'/clean/builty_elevations.dta", replace

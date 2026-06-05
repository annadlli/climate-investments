/*
3_filter_nonnew.do
------------------
Input:  data/clean/temp/builty_loosefilter.dta  (744,977 obs)
Output: data/clean/temp/builty_nonnew_filtered.dta

Drops pure WORK_TYPES == "New" records (handled in 2_filter_new.do),
applies additional false-positive exclusions specific to non-New permit
types, then requires at least one genuine flood-adaptation signal.

Non-New WORK_TYPES breakdown in the loosefilter:
  464,779 total non-New, of which:
     89,602  Alteration+New (combo)
     61,687  Addition+Alteration+New
     50,051  Addition+New
     44,925  Alteration only
     16,697  Alteration+Demolition+New
     15,643  Demolition+New
     14,975  Addition+Alteration
     13,441  Addition only
      5,552  Alteration+Demolition
      5,397  Demolition only
        ...  (smaller combos)

FP problem for non-New: most of the 464,779 records passed the loose
filter on one of: elevat (plan elevation labels, finished floor elevation
boilerplate), flood (floodplain checklists, flood damage repairs, flood
lights), or rais/lift (raised decks, electrical mast raising). The
genuine flood-adaptation cases are ~9,800 — so the positive signal
requirement in Step 5 is the key filter.
*/

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments"

********************************************************************************
* 1. Load
********************************************************************************
use "`root'/data/clean/temp/builty_loosefilter.dta", clear

count
di as text "Total obs in builty_loosefilter (starting point): " as result `r(N)'

********************************************************************************
* 2. Drop pure-New records (those are handled in 2_filter_new.do)
********************************************************************************
drop if WORK_TYPES == "New"

count
di as text "Obs after dropping WORK_TYPES == New (exact): " as result `r(N)'
* expected: 464,779

********************************************************************************
* 3. Drop "flood plain: no" boilerplate-only records
*    Same exclusion as in 2_filter_new.do. Catches new-build boilerplate
*    that also appears on combo permits (Alteration+New, Addition+New, etc.).
********************************************************************************
gen byte boilerplate_fp = ///
    regexm(desc_l, "flood *plain *:? *(no|\(x\)no|x\)no)") & ///
    !regexm(desc_l, "rais|lift") & ///
    !regexm(desc_l, "freeboard") & ///
    !regexm(desc_l, "floodproof") & ///
    !regexm(desc_l, "bfe|base flood") & ///
    !regexm(desc_l, "fema.{0,40}elevat") & ///
    !regexm(desc_l, "flood zone ae|flood zone ve|ae flood|ve flood")

count if boilerplate_fp == 1
di as text "Obs dropped (flood-plain-no boilerplate only): " as result `r(N)'

drop if boilerplate_fp == 1
drop boilerplate_fp

count
di as text "Obs remaining after boilerplate drop: " as result `r(N)'

********************************************************************************
* 4. Additional false positive exclusions specific to non-New records
*    For each, we only drop if the record has NO genuine flood signal alongside.
********************************************************************************

* 4a. Raised deck / patio / porch / screen room / lanai
*     Common in FL: "raised screen room addition", "raised rear deck" etc.
*     These are structural but not flood adaptation.
gen byte fp_deck = ///
    regexm(desc_l, "rais.{0,30}(deck|patio|porch|screen room|lanai|screen porch|stoop)") & ///
    !regexm(desc_l, "flood|bfe|fema")

count if fp_deck == 1
di as text "Obs dropped (raised deck / patio / porch / screen room, no flood signal): " as result `r(N)'

drop if fp_deck == 1
drop fp_deck

count
di as text "Obs remaining: " as result `r(N)'

* 4b. Raising electrical mast, meter, service, riser — utility work, not structural
gen byte fp_elec = ///
    regexm(desc_l, "rais.{0,30}(electrical mast|mast|meter|service|riser|ats|weather.?head)") & ///
    !regexm(desc_l, "flood|bfe|fema|flood zone")

count if fp_elec == 1
di as text "Obs dropped (raising electrical mast / meter / service): " as result `r(N)'

drop if fp_elec == 1
drop fp_elec

count
di as text "Obs remaining: " as result `r(N)'

* 4c. Flood lights — lighting fixture, not flood adaptation
gen byte fp_floodlight = ///
    regexm(desc_l, "flood *light") & ///
    !regexm(desc_l, "flood zone|flood plain|bfe|base flood|rais|elevat|fema")

count if fp_floodlight == 1
di as text "Obs dropped (flood light — lighting fixture): " as result `r(N)'

drop if fp_floodlight == 1
drop fp_floodlight

count
di as text "Obs remaining: " as result `r(N)'

* 4d. Flood damage repair — restoration of damage, not elevation/adaptation
*     Only drop when NOT paired with elevation language (those are genuine upgrades).
gen byte fp_flooddmg = ///
    regexm(desc_l, "flood damage") & ///
    !regexm(desc_l, "elevat|rais|lift|bfe|freeboard|floodproof|above flood|fema")

count if fp_flooddmg == 1
di as text "Obs dropped (flood damage repair only, no elevation signal): " as result `r(N)'

drop if fp_flooddmg == 1
drop fp_flooddmg

count
di as text "Obs remaining: " as result `r(N)'

* 4e. "No elevation certificate required" / "elevation certificate not required"
*     Confirms the property is outside the flood zone.
gen byte fp_nocert = ///
    regexm(desc_l, "no elevation certificate") | ///
    regexm(desc_l, "elevation certificate.{0,20}(not required|not needed|waived|n.?a)") | ///
    regexm(desc_l, "elevation (verification|cert).{0,20}not required")

count if fp_nocert == 1
di as text "Obs dropped (no elevation certificate required — outside flood zone): " as result `r(N)'

drop if fp_nocert == 1
drop fp_nocert

count
di as text "Obs remaining: " as result `r(N)'

* 4f. Flood Zone X only — Zone X is outside the SFHA (minimal / no flood hazard).
*     Only drop if there is no AE/VE zone or BFE language alongside.
gen byte fp_zonex = ///
    regexm(desc_l, "flood zone x") & ///
    !regexm(desc_l, "flood zone v|bfe|base flood elevation|ae flood|ve flood") & ///
    !regexm(desc_l, "flood zone a[^x]")

count if fp_zonex == 1
di as text "Obs dropped (flood zone X only, outside SFHA): " as result `r(N)'

drop if fp_zonex == 1
drop fp_zonex

count
di as text "Obs remaining: " as result `r(N)'

* 4g. HVAC replacement "same location / same elevation" — mechanical swap, not structural
gen byte fp_hvac = ///
    regexm(desc_l, "same.{0,20}(location|position).{0,20}elevation") & ///
    regexm(desc_l, "(hvac|condenser|air.?condition|furnace|boiler|chiller|compressor|ahu|rtu|heat pump)")

count if fp_hvac == 1
di as text "Obs dropped (HVAC replacement same location/elevation): " as result `r(N)'

drop if fp_hvac == 1
drop fp_hvac

count
di as text "Obs remaining after all FP exclusions: " as result `r(N)'

********************************************************************************
* 5. Genuine flood-adaptation signal requirement
*    The FP exclusions above handle specific named patterns but leave a large
*    residual (~450K) of records that matched the loose filter keyword but
*    contain no actual flood-elevation activity. The positive signal requirement
*    below is the main filter — only ~9,800 non-New records carry a genuine
*    signal.
********************************************************************************
gen byte genuine = 0

* 5a. Structure raised / foundation elevated because of flood
replace genuine = 1 if regexm(desc_l, "rais.{0,50}(flood plain|flood zone|bfe|base flood|out of flood|floodplain|100.yr)")
replace genuine = 1 if regexm(desc_l, "(flood plain|flood zone|bfe|base flood|floodplain|100.yr).{0,80}rais")
replace genuine = 1 if regexm(desc_l, "raise.{0,30}fema")

* 5b. Lift / elevate existing structure explicitly for flood (retrofit-specific)
replace genuine = 1 if regexm(desc_l, "(lift|elevat).{0,40}(existing|house|home|dwelling|structure|building|foundation).{0,40}flood")
replace genuine = 1 if regexm(desc_l, "flood.{0,40}(lift|elevat).{0,40}(existing|house|home|dwelling|structure|building|foundation)")

* 5c. Above BFE / above base flood elevation
replace genuine = 1 if regexm(desc_l, "above (base flood elevation|bfe)")
replace genuine = 1 if regexm(desc_l, "[0-9].{0,5}(ft|feet|foot).{0,20}above (flood|bfe)")
replace genuine = 1 if regexm(desc_l, "must meet.{0,20}base flood elevation")
replace genuine = 1 if regexm(desc_l, "must meet.{0,20}bfe")

* 5d. Freeboard
replace genuine = 1 if regexm(desc_l, "freeboard")

* 5e. Floodproofing (dry or wet)
replace genuine = 1 if regexm(desc_l, "floodproof")

* 5f. FEMA substantial damage rule (50% rule triggers required elevation upgrade)
replace genuine = 1 if regexm(desc_l, "substantial.?damage") & ///
    !regexm(desc_l, "fire damage") & ///
    !regexm(desc_l, "not a substantial")

* 5g. Elevate to conform with FEMA AE/VE flood zone designation
replace genuine = 1 if regexm(desc_l, "elevat.{0,60}(bfe|base flood elevation|flood zone ae|flood zone ve|ae flood|ve flood)")
replace genuine = 1 if regexm(desc_l, "elevat.{0,60}conform.{0,30}fema")
replace genuine = 1 if regexm(desc_l, "elevat.{0,60}fema flood")
replace genuine = 1 if regexm(desc_l, "(flood zone ae|flood zone ve|ae flood zone|ve flood zone|base flood elevation).{0,80}elevat")
replace genuine = 1 if regexm(desc_l, "fema.{0,40}elevat")

* 5h. Pile / pier foundation in VE flood zone
replace genuine = 1 if regexm(desc_l, "pile (foundation|fdn|found|cap).{0,60}flood")
replace genuine = 1 if regexm(desc_l, "flood.{0,60}pile (foundation|fdn|found|cap)")

* 5i. Raise / lift / elevate explicitly out of or above flood zone
replace genuine = 1 if regexm(desc_l, "(rais|lift|elevat).{0,40}out of.{0,20}flood")
replace genuine = 1 if regexm(desc_l, "(rais|lift|elevat).{0,40}above.{0,20}(flood plain|flood zone|floodplain)")

* 5j. Elevation certificate required in confirmed AE/VE zone (post-firm)
replace genuine = 1 if regexm(desc_l, "elevation certificate.{0,30}(required|reqrd|req).{0,100}(flood zone ae|flood zone ve|ae flood|ve flood|base flood elevation|bfe)")
replace genuine = 1 if regexm(desc_l, "(flood zone ae|flood zone ve|base flood elevation|bfe).{0,100}elevation certificate.{0,30}(required|reqrd|req)")

* 5k. Finished floor / bottom of frame must meet flood elevation
replace genuine = 1 if regexm(desc_l, "(bottom of frame|finished floor).{0,30}must meet elevation")
replace genuine = 1 if regexm(desc_l, "must meet.{0,30}(design )?flood elevation")
replace genuine = 1 if regexm(desc_l, "flood zone ae.{0,80}(bottom of frame|finished floor).{0,30}meet elevation")
replace genuine = 1 if regexm(desc_l, "(design )?flood elevation.{0,30}[0-9][0-9.]*.{0,10}(navd|ngvd|ft|feet)")

* 5l. Flood vents (required in AE zone for enclosures below BFE)
replace genuine = 1 if regexm(desc_l, "flood vent")

* 5m. Flood-damage-resistant materials below flood elevation
replace genuine = 1 if regexm(desc_l, "flood damage resistant.{0,60}elevation")
replace genuine = 1 if regexm(desc_l, "materials below.{0,30}elevation.{0,30}flood")

count if genuine == 0
di as text "Obs dropped (no genuine flood-adaptation signal): " as result `r(N)'

keep if genuine == 1
drop genuine

count
di as text "Non-New genuine flood-adaptation records kept: " as result `r(N)'
* expected: ~9,800

********************************************************************************
* 6. Save
********************************************************************************
save "`root'/data/clean/temp/builty_nonnew_filtered.dta", replace
di as text "Saved: data/clean/temp/builty_nonnew_filtered.dta"

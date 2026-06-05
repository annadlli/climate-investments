/*
2_filter_new.do
---------------
Input:  data/clean/temp/builty_loosefilter.dta  (744,977 obs)
Output: data/clean/temp/builty_new_genuine.dta

Keeps only records with WORK_TYPES == "New" (pure new construction)
and applies a tight genuine flood-adaptation filter to them.
Everything that passes is a true flood-elevation / flood-adaptation case.
The rest of the New records (~96%) are false positives driven by:
  - "plan elevation: A/B/C" (builder façade-style labels)
  - "flood plain: no" checklist boilerplate on new-build permits

Pipeline context (obs from Python notebook):
  163,332,180  builty_all.parquet (raw)
   60,211,670  builty_residential.parquet (residential only)
      744,977  builty_loosefilter.dta (loose keyword filter)
          ...  -> this file handles the "New" slice
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
* 2. Keep WORK_TYPES == "New" (exact match)
********************************************************************************
keep if WORK_TYPES == "New"

count
di as text "Obs with WORK_TYPES == New (exact): " as result `r(N)'
* expected: 280,198

********************************************************************************
* 3. Drop "flood plain: no" boilerplate-only records
*    These are new-build permits with a standard checkbox noting the property
*    is NOT in a flood zone. We drop them unless a genuine flood signal is
*    also present (raise, BFE, freeboard, floodproof, etc.).
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
* 4. Genuine flood-adaptation filter
*    Require at least one positive signal of genuine flood elevation.
*    Each replace covers a distinct semantic category.
********************************************************************************
gen byte genuine = 0

* 4a. Structure raised / foundation elevated because of flood
replace genuine = 1 if regexm(desc_l, "rais.{0,50}(flood plain|flood zone|bfe|base flood|out of flood|floodplain|100.yr)")
replace genuine = 1 if regexm(desc_l, "(flood plain|flood zone|bfe|base flood|floodplain|100.yr).{0,80}rais")
replace genuine = 1 if regexm(desc_l, "raise.{0,30}fema")

* 4b. Explicit "above BFE" / "above base flood elevation"
replace genuine = 1 if regexm(desc_l, "above (base flood elevation|bfe)")
replace genuine = 1 if regexm(desc_l, "[0-9].{0,5}(ft|feet|foot).{0,20}above (flood|bfe)")
replace genuine = 1 if regexm(desc_l, "above.{0,20}(100.yr|100-year) flood")
replace genuine = 1 if regexm(desc_l, "must meet.{0,20}base flood elevation")
replace genuine = 1 if regexm(desc_l, "must meet.{0,20}bfe")
replace genuine = 1 if regexm(desc_l, "housing pad.{0,30}above bfe")
replace genuine = 1 if regexm(desc_l, "placed.{0,20}(at |above ).{0,20}bfe")

* 4c. Freeboard (by definition a flood-zone construction requirement)
replace genuine = 1 if regexm(desc_l, "freeboard")

* 4d. Floodproofing (dry or wet)
replace genuine = 1 if regexm(desc_l, "floodproof")

* 4e. FEMA substantial damage rule (50% rule triggers required elevation upgrade)
*     Exclude: fire damage context, "not a substantial improvement"
replace genuine = 1 if regexm(desc_l, "substantial.?damage") & ///
    !regexm(desc_l, "fire damage") & ///

* 4f. Elevate to conform with FEMA AE/VE flood zone designation
replace genuine = 1 if regexm(desc_l, "elevat.{0,60}(bfe|base flood elevation|flood zone ae|flood zone ve|ae flood|ve flood)")
replace genuine = 1 if regexm(desc_l, "elevat.{0,60}conform.{0,30}fema")
replace genuine = 1 if regexm(desc_l, "elevat.{0,60}fema flood")
replace genuine = 1 if regexm(desc_l, "(flood zone ae|flood zone ve|ae flood zone|ve flood zone|base flood elevation).{0,80}elevat")
replace genuine = 1 if regexm(desc_l, "fema.{0,40}elevat")

* 4g. Pile / pier foundation in VE flood zone
replace genuine = 1 if regexm(desc_l, "pile (foundation|fdn|found|cap).{0,60}flood")
replace genuine = 1 if regexm(desc_l, "flood.{0,60}pile (foundation|fdn|found|cap)")

* 4h. Raise / lift / elevate explicitly out of or above flood zone
replace genuine = 1 if regexm(desc_l, "(rais|lift|elevat).{0,40}out of.{0,20}flood")
replace genuine = 1 if regexm(desc_l, "(rais|lift|elevat).{0,40}above.{0,20}(flood plain|flood zone|floodplain)")

* 4i. Elevation certificate required in confirmed AE/VE zone (post-firm structure)
replace genuine = 1 if regexm(desc_l, "elevation certificate.{0,30}(required|reqrd|req).{0,100}(flood zone ae|flood zone ve|ae flood|ve flood|base flood elevation|bfe)")
replace genuine = 1 if regexm(desc_l, "(flood zone ae|flood zone ve|base flood elevation|bfe).{0,100}elevation certificate.{0,30}(required|reqrd|req)")

* 4j. Finished floor / bottom of frame must meet flood elevation (incl. mobile homes)
replace genuine = 1 if regexm(desc_l, "(bottom of frame|finished floor).{0,30}must meet elevation")
replace genuine = 1 if regexm(desc_l, "must meet.{0,30}(design )?flood elevation")
replace genuine = 1 if regexm(desc_l, "flood zone ae.{0,80}(bottom of frame|finished floor).{0,30}meet elevation")
replace genuine = 1 if regexm(desc_l, "(design )?flood elevation.{0,30}[0-9][0-9.]*.{0,10}(navd|ngvd|ft|feet)")

* 4k. Flood-damage-resistant materials below a specified flood elevation
replace genuine = 1 if regexm(desc_l, "flood damage resistant.{0,60}elevation")
replace genuine = 1 if regexm(desc_l, "materials below.{0,30}elevation.{0,30}flood")

count if genuine == 0
di as text "Obs dropped (no genuine flood-adaptation signal): " as result `r(N)'

keep if genuine == 1
drop genuine

count
di as text "Genuine new-construction flood-adaptation records kept: " as result `r(N)'
* expected: ~9,671

********************************************************************************
* 5. Save
********************************************************************************
save "`root'/data/clean/temp/builty_new_genuine.dta", replace
di as text "Saved: data/clean/temp/builty_new_genuine.dta"

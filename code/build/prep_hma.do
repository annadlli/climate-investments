/******************************************************************************
Authors: Vendela Norman
Date: 2026-09-15

Description: Collapses the FMA home-elevation grants to ZIP and county, 
    so FMA funding can be merged onto the property universe.

******************************************************************************/

args data

* -----------------------------------------------------------------------------
* Section 1: ZIP level
* -----------------------------------------------------------------------------

* Import HMA home-elevation grants (all programs)
use "`data'/clean/hma_elevation.dta", clear

* Drop observations with missing zip codes 
drop if mi(zip)

* Apportion project spend across the project's logged properties
// Note: hma_spend is project-level, replicated onto every property record by the m:1
// merge in clean_hma -- summing it over rows inflates spend ~9x. Weighting each row by
// its share of the project's logged properties preserves the project total.
// Note: n_properties_rec needs no apportioning -- it already counts the structures in
// this record, so it sums to a whole number rather than a fraction.
bysort project_identifier: egen n_logged = total(n_properties_rec)
gen spend = hma_spend * (n_properties_rec / n_logged)

* Collapse to zip level (pooling projects and years)
// Note: Should probably try to do something more refined than pooling across all years
// Note: bcr is a record-weighted mean, so projects count in proportion to rows
// Note: county is firstnm, so it is arbitrary for the 33 zips that cross counties
egen proj_tag = tag(zip project_identifier)
collapse (sum) hma_n_grants = proj_tag ///
               hma_n_properties = n_properties_rec hma_spend = spend ///
         (min) hma_year_min = obligation_year ///
         (max) hma_year_max = year_closed ///
         (firstnm) state state_code county county_code, by(zip)

* Create merge variables
ren zip zipcode 

* Label
label var hma_n_grants     "HMA elevation grants (projects) in ZIP"
label var hma_n_properties "Properties elevated under HMA grants in ZIP"
label var hma_spend        "HMA elevation spending in ZIP (2023 $)"
label var hma_year_min     "First HMA obligation year in ZIP"
label var hma_year_max     "Last HMA grant closure year in ZIP"

* Save
order state state_code county county_code zipcode
sort zipcode
compress
save "`data'/clean/hma_zip.dta", replace

* -----------------------------------------------------------------------------
* Section 2: County level 
* -----------------------------------------------------------------------------

* Import HMA home-elevation grants (all programs; programarea is the flag)
use "`data'/clean/hma_elevation.dta", clear

* Drop observations with missing geographic identifiers
drop if mi(county_code) | county_code == 0 // fill in county w/ subrecipient info?

* Apportion project spend across the project's logged properties
// Note: Project-only rows have no MitProps record, so n_properties_rec is missing.
// FEMA logged no structures for them, so the project sits whole in its own county and
// contributes its project-level property count.
bysort project_identifier: egen n_logged = total(n_properties_rec)
gen spend = cond(project_merge == 2, hma_spend, hma_spend * (n_properties_rec / n_logged))
gen n_props = cond(project_merge == 2, n_properties, n_properties_rec)

* Collapse to county level (pooling projects and years)
egen proj_tag = tag(state_code county_code project_identifier)
collapse (sum) hma_n_grants = proj_tag ///
               hma_n_properties = n_props hma_spend = spend ///
         (min) hma_year_min = obligation_year ///
         (max) hma_year_max = year_closed ///
         (firstnm) state county, by(state_code county_code)

* Create merge variables
gen countycode = string(state_code, "%02.0f") + string(county_code, "%03.0f")
drop county_code

* Label
label var hma_n_grants     "HMA elevation grants (projects) in county"
label var hma_n_properties "Properties elevated under HMA grants in county"
label var hma_spend        "HMA elevation spending in county (2023 $)"
label var hma_year_min     "First HMA obligation year in county"
label var hma_year_max     "Last HMA grant closure year in county"

* Save
order state state_code county countycode
sort countycode
compress
save "`data'/clean/hma_county.dta", replace

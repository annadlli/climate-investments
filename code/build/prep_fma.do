/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-16

Description: Collapses the FMA home-elevation grants to ZIP (primary) and county
    (fallback), so FMA funding can be merged onto the property universe.

Note: This should be a placeholder until the FOIA requests come through and we
    can merge FMA grants ar the property level.

******************************************************************************/

args data

* -----------------------------------------------------------------------------
* Section 1: ZIP level (primary)
* -----------------------------------------------------------------------------

* Import FMA home-elevation grants
use "`data'/clean/fma_elevation.dta", clear

* Drop observations with missing zip codes 
drop if mi(zip)

* Apportion project spend across the project's logged properties
// Note: fma_spend is project-level, replicated onto every property record by the m:1
// merge in clean_fma -- summing it over rows inflates spend ~9x. Weighting each row by
// its share of the project's logged properties preserves the project total.
// Note: n_properties_rec needs no apportioning -- it already counts the structures in
// this record, so it sums to a whole number rather than a fraction.
bysort project_identifier: egen n_logged = total(n_properties_rec)
gen spend = fma_spend * (n_properties_rec / n_logged)

* Collapse to zip level (pooling projects and years)
// Note: Should probably try to do something more refined than pooling across all years
// Note: bcr is a record-weighted mean, so projects count in proportion to rows
// Note: county is firstnm, so it is arbitrary for the 33 zips that cross counties
egen proj_tag = tag(zip project_identifier)
collapse (sum) n_grants = proj_tag ///
               n_properties = n_properties_rec fma_spend = spend ///
         (mean) bcr = bcr ///
         (min) year_min = obligation_year ///
         (max) year_max = year_closed ///
         (firstnm) state state_code county county_code, by(zip)

* Create merge variables
ren zip zipcode 

* Save
order state state_code county county_code zipcode
sort zipcode
compress
save "`data'/clean/fma_zip.dta", replace

* -----------------------------------------------------------------------------
* Section 2: County level (fallback)
* -----------------------------------------------------------------------------

* Import FMA home-elevation grants
use "`data'/clean/fma_elevation.dta", clear

* Drop observations with missing geographic identifiers
drop if mi(county_code) | county_code == 0 // fill in county w/ subrecipient info?

* Apportion project spend across the project's logged properties
// Note: Project-only rows have no MitProps record, so n_properties_rec is missing.
// FEMA logged no structures for them, so the project sits whole in its own county and
// contributes its project-level property count.
bysort project_identifier: egen n_logged = total(n_properties_rec)
gen spend = cond(project_merge == 2, fma_spend, fma_spend * (n_properties_rec / n_logged))
gen n_props = cond(project_merge == 2, n_properties, n_properties_rec)

* Collapse to county level (pooling projects and years)
egen proj_tag = tag(state_code county_code project_identifier)
collapse (sum) n_grants = proj_tag ///
               n_properties = n_props fma_spend = spend ///
         (mean) bcr = bcr ///
         (min) year_min = obligation_year ///
         (max) year_max = year_closed ///
         (firstnm) state county, by(state_code county_code)

* Create merge variables
gen countycode = string(state_code, "%02.0f") + string(county_code, "%03.0f")
drop county_code

* Save
order state state_code county countycode
sort countycode
compress
save "`data'/clean/fma_county.dta", replace

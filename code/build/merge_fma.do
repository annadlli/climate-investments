/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-01

Description: Collapses the FMA home-elevation grants to the county level, so FMA
    funding can be merged onto the property universe by county.

Note: This should be a placeholder until the FOIA requests come through and we
    can merge FMA grants ar the property level.

******************************************************************************/

args data

* Import FMA home-elevation grants
use "`data'/clean/fma_elevation_properties.dta", clear




/* 

* Restrict to grains w/ a non-missing county
// Note: Focus states (TX and VA are never missing the county)
// Note: Could use the more granular subrecipient info...
drop if mi(county) | county == "Statewide"

// TEMP 
drop if year_elev_max < 2009

* Collapse to county level (pooling projects and years)
// Note: Should probably try to do something more refined than pooling across all years 
gen n = _n
collapse (count) n_grants = n ///
         (sum) n_properties = n_properties fma_spend = fma_spend ///
         (mean) bcr = bcr ///
         (min) year_min = year_elev_min ///
         (max) year_max = year_elev_max ///
         (firstnm) state county, by(state_code county_code)

* Create merge variables 
gen countycode = string(state_code, "%02.0f") + string(county_code, "%03.0f")
drop county_code
gen elevated = 1 

* Save
order state state_code county countycode
save "`data'/clean/fma_county.dta", replace

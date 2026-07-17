/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-16

Description: Creates geographic crosswalks. 

******************************************************************************/

args data

* Import FMA data
import delimited using "`data'/raw/HazardMitigationAssistanceProjects.csv", clear ///
        varnames(1) stringcols(_all) bindquote(strict)

* Collapse into crosswalk 
keep state statenumbercode county countycode 
duplicates drop 
destring *, replace 
drop if mi(countycode)

* Save 
order statenumbercode countycode, last
sort statenumbercode countycode
save "`data'/clean/crosswalks/county_xwalk.dta", replace
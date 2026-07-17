/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-07-10

Description: Cleans FEMA Hazard Mitigation Assistance (HMA) projects data, 
    restricting to FMA projects. 

Source: fema.gov/openfema-data-page/hazard-mitigation-assistance-projects-v4

TODO: Figure out if this property-level dataset it worth incorporating: 
    https://catalog.data.gov/dataset/hazard-mitigation-assistance-mitigated-properties
******************************************************************************/

args data

* -----------------------------------------------------------------------------
* Section 1: Basic cleaning and sample restrictions
* -----------------------------------------------------------------------------

* Import data 
import delimited using "`data'/raw/HazardMitigationAssistanceProjects.csv", clear ///
    varnames(1) stringcols(_all) bindquote(strict)

* Drop irrelevant variables 
drop disasternumber recipientadmincostamt recipienttribalindicator ///
    subrecipientadmincostamt subrecipienttribalindicator id datasource ///
    recipient srmcobligatedamt

* Filter to relevant projects 
// Note: This keeps some compound projects (e.g., elevations plus buyouts)
// i) Keep FMA home elevation projects 
keep if programarea == "FMA" | programarea == "SRL" // FMA projects 
keep if strpos(projecttype, "202.1") > 0 | strpos(projecttype, "202.2") > 0 // Home elevations 
drop if strpos(projecttype, "106.2") > 0 // some "other non-construction" project 
// ii) Approved and completed projects only
drop if inlist(status, "Not Approved / Denied", "Not Selected", "Withdrawn", ///
    "Void", "Revision Requested", "Pending")
drop if mi(status) // drops 1 obs that doesn't look like it recieved funding 
drop if mi(initialobligationamount) & federalshareobligated == "0.00" & numberoffinalproperties == "0" 

* Destring
qui: destring *, replace 

* -----------------------------------------------------------------------------
* Section 2: Create analysis variables & additional cleaning
* -----------------------------------------------------------------------------

* Create elevation year window
// i) Convert date strings to numeric calendar years
foreach var in dateinitiallyapproved dateapproved dateclosed initialobligationdate {
    gen `var'_year = real(substr(`var', 1, 4))
    drop `var'
}
// ii) Create minimum elevation year variable 
egen year_elev_min = rowmin(dateinitiallyapproved_year dateapproved_year)
replace year_elev_min = programfy if missing(year_elev_min)
replace year_elev_min = initialobligationdate_year if !mi(initialobligationdate_year)
drop dateinitiallyapproved_year programfy dateapproved_year
// iii) Create maximum elevation year variable
// Note: This is missing for non-closed projects 
ren dateclosed_year year_elev_max
// iv) Checks 
assert !mi(year_elev_min) 
assert year_elev_min <= year_elev_max

* Create funding year variable 
gen year = initialobligationdate_year
replace year = year_elev_min if mi(year) 
drop initialobligationdate_year

* Set 0's to missing for some variables 
// Note: These variables should not be 0 so set to missing 
// Note: I think some of these 0s are actually redactions when #properties = 1 
foreach var in numberofproperties numberoffinalproperties benefitcostratio netvaluebenefits {
    replace `var' = . if `var' == 0
}

* Create additional analysis variables 
// i) FMA spend 
gen fma_spend = federalshareobligated 
replace fma_spend = initialobligationamount if (fma_spend == 0 | mi(fma_spend)) ///
    & !mi(initialobligationamount) & initialobligationamount > 0
replace fma_spend = projectamount * costsharepercentage if fma_spend == 0 | mi(fma_spend) 
drop initialobligationamount federalshareobligated projectamount costsharepercentage
// ii) Number of properties
gen n_properties = numberoffinalproperties
replace n_properties = numberofproperties if mi(n_properties) & !mi(numberofproperties)
drop numberoffinalproperties numberofproperties

* Merge in CPI 
merge m:1 year using "`data'/clean/cpi.dta", assert(2 3) keep(1 3) keepusing(cpi) nogen

* Deflate nominal variables 
foreach var in fma_spend netvaluebenefits {
    replace `var' = `var' / cpi if !mi(`var') & !mi(cpi)
}
drop cpi

* Rename
rename (benefitcostratio projecttype statenumbercode countycode ///
    netvaluebenefits projectidentifier projectcounties year) ///
    (bcr project_type state_code county_code ///
    net_value_benefits project_identifier project_counties obligation_year)

* -----------------------------------------------------------------------------
* Section 3: Save
* -----------------------------------------------------------------------------

* Label variables
label var state                      "State name"
label var state_code                 "State FIPS code"
label var county                     "County name"
label var county_code                "County FIPS code (3-digit)"
label var programarea                "FEMA program"
label var year_elev_min              "Minimum elevation year"
label var year_elev_max              "Maximum elevation year"
label var region                     "FEMA region"
label var project_counties           "Project counties"
label var project_type               "Project type"
label var status                     "Project status"
label var obligation_year             "Year of initial obligation"
label var subrecipient               "Subrecipient"
label var fma_spend                  "Federal dollars obligated"
label var bcr                        "Benefit-cost ratio"
label var net_value_benefits         "Net value of benefits"
label var n_properties               "Number of properties"
label var project_identifier         "Project identifier"

* Save data
order state state_code county county_code subrecipient programarea year_elev_min ///
    year_elev_max status n_properties fma_spend bcr net_value_benefits obligation_year
order region project_counties project_type project_identifier, last 
sort state county year_elev_min
compress
save "`data'/clean/fma_elevation_projects.dta", replace

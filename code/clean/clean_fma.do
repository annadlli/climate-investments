/******************************************************************************
Author: Vendela Norman
Date: 2026-07-16

Description: Cleans FEMA HMA mitigated properties and project files, restricting
    to FMA single-family elevation homes. 

Sources: Properties -- https://catalog.data.gov/dataset/hazard-mitigation-assistance-mitigated-properties
        Projects -- fema.gov/openfema-data-page/hazard-mitigation-assistance-projects-v4
******************************************************************************/

args data

* -----------------------------------------------------------------------------
* Section 1: Merge datasets 
* -----------------------------------------------------------------------------

* Import property-level data
import delimited "`data'/raw/hma_mitigated_properties.csv", clear stringcols(_all)

* Restrict to FMA single-family elevations
keep if programarea == "FMA" | programarea == "SRL" 
keep if propertyaction == "Elevation"
keep if structuretype == "Single Family"

* Merge in project-level data 
// Note: This provides more granular geographic data for a subset of projects. 
preserve
    import delimited using "`data'/raw/HazardMitigationAssistanceProjects.csv", clear ///
        varnames(1) stringcols(_all) bindquote(strict)
    keep if programarea == "FMA" | programarea == "SRL"
    // Note: Both files carry numberofproperties -- MitProps' counts the structures in
    // a record, Projects' counts them in the project. Rename so the merge keeps both
    // grains instead of the master silently winning.
    ren numberofproperties n_properties_proj
    tempfile fma_projects
    save "`fma_projects'", replace
restore

merge m:1 projectidentifier using "`fma_projects'", assert(2 3) gen(project_merge)

* Drop irrelevant variables 
drop disasternumber recipientadmincostamt recipienttribalindicator ///
    subrecipientadmincostamt subrecipienttribalindicator id datasource ///
    recipient srmcobligatedamt

* -----------------------------------------------------------------------------
* Section 2: Additional sample selection & cleaning 
* -----------------------------------------------------------------------------

* Filter to relevant projects 
// Note: This keeps some compound projects (e.g., elevations plus buyouts)
// i) Keep FMA home elevation projects 
keep if propertyaction == "Elevation" | strpos(projecttype, "202.1") > 0 | strpos(projecttype, "202.2") > 0 // Home elevations 
drop if propertyaction != "Elevation" & strpos(projecttype, "106.2") > 0 // some "other non-construction" project 
// ii) Approved and completed projects only
drop if inlist(status, "Not Approved / Denied", "Not Selected", "Withdrawn", ///
    "Void", "Revision Requested", "Pending")
drop if mi(status) // drops 1 obs that doesn't look like it recieved funding 
drop if mi(initialobligationamount) & federalshareobligated == "0.00" & numberoffinalproperties == "0" 

* Destring
// Note: zip is excluded -- destring strips leading zeros (NJ/MA/CT/RI ZIPs)
qui: ds zip, not
qui: destring `r(varlist)', replace
replace zip = "" if zip == "00000"

* Merge in clean county names/codes 
// i) Names 
ren county county_name
merge m:1 state countycode using "`data'/clean/crosswalks/county_xwalk.dta", ///
    keep(1 3) keepusing(county) nogen
replace county_name = county if mi(county_name)
drop county countycode
ren county_name county
replace county = "Norfolk (city)" if county == "Norton (city)" & state == "Virginia" // fix typo
// ii) Codes
merge m:1 state county using "`data'/clean/crosswalks/county_xwalk.dta", ///
    keep(1 3) keepusing(countycode) nogen
replace countycode = 109 if strpos(subrecipient, "Terrebonne") > 0

* Clean city names
replace city = proper(trim(city))

* -----------------------------------------------------------------------------
* Section 3: Create analysis variables 
* -----------------------------------------------------------------------------

* Create elevation year window
// i) Convert date strings to numeric calendar years
foreach var in dateinitiallyapproved dateapproved dateclosed initialobligationdate {
    gen `var'_year = real(substr(`var', 1, 4))
    drop `var'
}
// ii) Create minimum elevation year variable 
// Note: Initial obligation year unless missing
egen year_elev_min = rowmin(dateinitiallyapproved_year dateapproved_year)
replace year_elev_min = programfy if missing(year_elev_min)
replace year_elev_min = initialobligationdate_year if !mi(initialobligationdate_year)
// iii) Create maximum elevation year variable
ren dateclosed_year year_closed // missing for non-closed projects 
// iv) Checks 
assert !mi(year_elev_min) 
assert year_elev_min <= year_closed

* Set 0's to missing for some variables 
// Note: These variables should not be 0 so set to missing 
// Note: I think some of these 0s are actually redactions when #properties = 1 
foreach var in numberofproperties n_properties_proj numberoffinalproperties benefitcostratio netvaluebenefits {
    replace `var' = . if `var' == 0
}

* Create additional analysis variables 
// i) FMA spend 
gen fma_spend = federalshareobligated 
replace fma_spend = initialobligationamount if (fma_spend == 0 | mi(fma_spend)) ///
    & !mi(initialobligationamount) & initialobligationamount > 0
replace fma_spend = projectamount * costsharepercentage if fma_spend == 0 | mi(fma_spend) 
// ii) Number of properties
// Note: n_properties is the project total; n_properties_rec is the structures in this
// MitProps record, and is missing on project-only rows, which have no record. Keeping
// them apart lets the build sum properties instead of apportioning a project total.
gen n_properties = numberoffinalproperties
replace n_properties = n_properties_proj if mi(n_properties) & !mi(n_properties_proj)
ren numberofproperties n_properties_rec

* Merge in CPI 
ren year_elev_min year
merge m:1 year using "`data'/clean/cpi.dta", assert(2 3) keep(1 3) keepusing(cpi) nogen

* Deflate nominal variables 
foreach var in fma_spend netvaluebenefits {
    replace `var' = `var' / cpi if !mi(`var') & !mi(cpi)
}

* Drop additional variables 
drop propertyaction structuretype status cpi numberoffinalproperties n_properties_proj ///
    initialobligationamount federalshareobligated projectamount costsharepercentage ///
    initialobligationdate_year dateinitiallyapproved_year programfy dateapproved_year

* Rename
rename (benefitcostratio projecttype statenumbercode countycode ///
    netvaluebenefits projectcounties year projectidentifier) ///
    (bcr project_type state_code county_code net_value_benefits ///
        project_counties obligation_year project_identifier)

* -----------------------------------------------------------------------------
* Section 4: Save
* -----------------------------------------------------------------------------

* Label variables
label var state                      "State name"
label var state_code                 "State FIPS code"
label var county                     "County name"
label var county_code                "County FIPS code (3-digit)"
label var subrecipient               "Subrecipient"
label var city                       "City"
label var zip                        "ZIP (5-digit; finest FMA geography)"
label var obligation_year            "Year of initial obligation"
label var year_closed              "Maximum elevation year"
label var n_properties               "Number of properties (project)"
label var n_properties_rec           "Number of properties (this record)"
label var fma_spend                  "Federal dollars obligated"
label var bcr                        "Benefit-cost ratio"
label var net_value_benefits         "Net value of benefits"
label var region                     "FEMA region"
label var project_counties           "Project counties"
label var programarea                "FEMA program"
label var project_type               "Project type"
label var project_identifier         "Project identifier"

* Save 
order state state_code county county_code subrecipient city zip programarea ///
    obligation_year year_closed n_properties n_properties_rec fma_spend bcr ///
    net_value_benefits 
order propertypartofproject typeofresidency damagecategory foundationtype ///
    actualamountpaid region project_counties programarea project_type project_identifier, last 
sort state county city obligation_year
compress
save "`data'/clean/fma_elevation.dta", replace


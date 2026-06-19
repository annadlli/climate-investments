/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-06-11

Description: Cleans FEMA Hazard Mitigation Assistance (HMA) projects data, 
    restricting to FMA projects. 

Source: fema.gov/openfema-data-page/hazard-mitigation-assistance-projects-v4
******************************************************************************/

args data

* Import data 
import delimited using "`data'/raw/HazardMitigationAssistanceProjects.csv", clear ///
    varnames(1) stringcols(_all) bindquote(strict)

* Drop irrelevant variables 
drop disasternumber recipientadmincostamt recipienttribalindicator ///
    subrecipientadmincostamt subrecipienttribalindicator ///
    initialobligationdate srmcobligatedamt region id datasource 

* Destring
qui: destring *, replace 

* Filter to relevant projects 
// i) Keep FMA projects 
keep if programarea == "FMA" // FMA projects 
keep if strpos(projecttype, "202.1") > 0 | strpos(projecttype, "202.2") > 0 // Home elevations 
// ii) Drop buyouts 
// Note: The vast majority of projects are elevation or buyout only. This drops 
// a small number of projects (~75) that bundle both. 
drop if strpos(projecttype, "Acquisition") > 0 // drop buyouts
// iii) Drop remaining compound projects: strip the private-elevation entries, drop if any other activity code is left (keeps 202.1+202.2 combos)
drop if ustrregexm(ustrregexra(projecttype, "202\.[12]A?:[^;]*", ""), "[0-9]+\.[0-9]+[A-Z]?:")
// iv) Drop projects where no properties were elevated
drop if numberofproperties == 0
drop programarea

* Convert date strings to numeric calendar years
gen year_approved = real(substr(dateapproved, 1, 4))
gen year_closed = real(substr(dateclosed, 1, 4))
drop dateapproved dateclosed

* Variable cleanup 
replace benefitcostratio = . if benefitcostratio == 0

* Rename
rename (programfy benefitcostratio projecttype statenumbercode countycode ///
    projectamount initialobligationamount federalshareobligated netvaluebenefits ///
    numberofproperties numberoffinalproperties costsharepercentage ///
    projectidentifier projectcounties dateinitiallyapproved) ///
    (year bcr project_type state_code county_code ///
    project_amount initial_obligation_amount federal_share_obligated net_value_benefits ///
    number_of_properties number_of_final_properties cost_share_percentage ///
    project_identifier project_counties date_initially_approved)

* Label variables
label var year                       "Program fiscal year"
label var year_approved              "Year project approved"
label var year_closed                "Year project closed"
label var date_initially_approved    "Date initially approved"
label var project_identifier         "Project identifier"
label var state                      "State name"
label var state_code                 "State FIPS code"
label var county                     "County name"
label var county_code                "County FIPS code (3-digit)"
label var project_counties           "Project counties"
label var project_type               "Project type"
label var status                     "Project status"
label var recipient                  "Recipient"
label var subrecipient               "Subrecipient"
label var project_amount             "Total project amount"
label var initial_obligation_amount  "Initial obligation amount"
label var federal_share_obligated    "Federal share obligated"
label var bcr                        "Benefit-cost ratio"
label var net_value_benefits         "Net value of benefits"
label var number_of_properties       "Number of properties"
label var number_of_final_properties "Final number of properties"
label var cost_share_percentage      "Cost share percentage"

* Save data
order project_identifier year* state state_code county county_code status
sort year state county
compress
save "`data'/clean/fma_elevation_grants.dta", replace

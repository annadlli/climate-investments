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
    subrecipientadmincostamt subrecipienttribalindicator dateinitiallyapproved ///
    initialobligationdate srmcobligatedamt region id projectidentifier ///
    datasource projectcounties

* Destring
qui: destring *, replace 

* Filter to relevant projects 
// i) Keep FMA projects 
keep if programarea == "FMA" // FMA projects 
keep if strpos(projecttype, "202.1") > 0 | strpos(projecttype, "202.2") > 0 // Home elevations 
// ii) Drop buyouts 
// Note: The vast marjority of projects are elevation or buyout only. This drops 
// a small number of projects (~75) that bundle both. 
drop if strpos(projecttype, "Acquisition") > 0 // drop buyouts
// iii) Drop remaining compound projects: strip the private-elevation entries, drop if any other activity code is left (keeps 202.1+202.2 combos)
drop if ustrregexm(ustrregexra(projecttype, "202\.[12]A?:[^;]*", ""), "[0-9]+\.[0-9]+[A-Z]?:")
drop programarea

* Convert date strings to numeric calendar years
gen year_approved = real(substr(dateapproved, 1, 4))
gen year_closed = real(substr(dateclosed, 1, 4))
drop dateapproved dateclosed

* Rename 
rename programfy year

* Label variables
label var year                      "Program fiscal year"
label var state                     "State name"
label var statenumbercode           "State FIPS code"
label var county                    "County name"
label var countycode                "County FIPS code"
label var projecttype               "Project type"
label var status                    "Project status"
label var recipient                 "Recipient"
label var subrecipient              "Subrecipient"
label var projectamount             "Total project amount"
label var federalshareobligated     "Federal share obligated"
label var benefitcostratio          "Benefit-cost ratio"
label var netvaluebenefits          "Net value of benefits"
label var numberofproperties        "Number of properties"
label var numberoffinalproperties   "Final number of properties"
label var year_approved             "Year project approved"
label var year_closed               "Year project closed"

* Save data 
order year* state statenumbercode county countycode status 
sort year state county
compress
save "`data'/clean/hma_projects.dta", replace

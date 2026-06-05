/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-05-29

Description: Cleans FEMA Hazard Mitigation Assistance (HMA) projects ->
    clean/hma_projects.dta. Restricted to FMA (2026-05-29).

Source: fema.gov/openfema-data-page/hazard-mitigation-assistance-projects-v4
******************************************************************************/

* Data root passed from master.do as the first argument
args data
local raw   "`data'/raw"
local clean "`data'/clean"

local hma_raw ""

foreach f in ///
    "`raw'/HazardMitigationAssistanceProjects (1).csv" ///
    "`raw'/HazardMitigationAssistanceProjects.csv" ///
    "`raw'/hma_projects.csv" ///
    "`raw'/fema/HazardMitigationAssistanceProjects.csv" ///
    "`raw'/fema/hma_projects.csv" {

    capture confirm file "`f'"

    if _rc == 0 & "`hma_raw'" == "" {
        local hma_raw "`f'"
    }
}

if "`hma_raw'" == "" {
    di as error "SKIP: HazardMitigationAssistanceProjects raw file not found in data/raw."
}
else {

    import delimited using "`hma_raw'", clear varnames(1) stringcols(_all) bindquote(strict)
     rename *, lower

    keep projectidentifier programarea programfy region state statenumbercode ///
         county countycode disasternumber projectcounties projecttype ///
         status recipient recipienttribalindicator subrecipient ///
         subrecipienttribalindicator datasource dateapproved ///
         dateclosed dateinitiallyapproved projectamount ///
         initialobligationdate initialobligationamount ///
         federalshareobligated subrecipientadmincostamt ///
         srmcobligatedamt recipientadmincostamt ///
         costsharepercentage benefitcostratio netvaluebenefits ///
         numberoffinalproperties numberofproperties id

    drop if missing(id) & missing(projectidentifier) & missing(programfy)

    foreach v in ///
        programfy region statenumbercode countycode disasternumber ///
        projectamount initialobligationamount federalshareobligated ///
        subrecipientadmincostamt srmcobligatedamt ///
        recipientadmincostamt costsharepercentage ///
        benefitcostratio netvaluebenefits ///
        numberoffinalproperties numberofproperties {

        capture destring `v', replace force
    }

    * Labels
    label data "Clean source: FEMA Hazard Mitigation Assistance Projects"

    label var projectidentifier      "HMA project identifier"
    label var programarea            "HMA program area"
    label var programfy              "Program fiscal year"
    label var region                 "FEMA region"
    label var state                  "State name"
    label var statenumbercode        "State FIPS code"
    label var county                 "County name"
    label var countycode             "County FIPS code"
    label var disasternumber         "FEMA disaster number"
    label var projecttype            "Project type"
    label var status                 "Project status"
    label var recipient              "Recipient"
    label var subrecipient           "Subrecipient"
    label var projectamount          "Total project amount"
    label var federalshareobligated  "Federal share obligated"
    label var benefitcostratio       "Benefit-cost ratio"
    label var netvaluebenefits       "Net value of benefits"
    label var numberofproperties     "Number of properties"
    label var numberoffinalproperties "Final number of properties"

    * Drop unused variables
    drop disasternumber ///
         recipientadmincostamt ///
         recipienttribalindicator ///
         subrecipientadmincostamt ///
         subrecipienttribalindicator ///
         dateinitiallyapproved ///
         initialobligationdate ///
         srmcobligatedamt ///
         region ///
         id ///
         projectidentifier

    order programfy state statenumbercode county countycode
	keep if programarea == "FMA" //2026-05-29 update: keep only FMA -> 4409 obs total
    compress

    save "`clean'/hma_projects.dta", replace
}

********************************************************************************
* Author: Anna
* Date: 2026-05-22
*Update DateA: 2026-05-31
*Update Content:
//2026-05-29: restrict to FMA only, remove nri and npr
//2026-05-31: add clean nfip policies content

* Description: This file reads in raw FEMA/NPR data sources and converts them
* into cleaned source-level files for the climate investments project.
*
* Codebase organization:
*   raw source files:
*       data/raw/HazardMitigationAssistanceProjects (1).csv
*       data/raw//nri_counties.csv
*       data/raw/FimaNfipClaimsV2.csv
*       data/raw/fema/fema_npr.csv
*
*   cleaned source files:
*       data/clean/fema/hma_projects.dta
*       data/clean/fema/nri_counties.dta
*       data/clean/fema/nfip_claims.dta
*       data/clean/fema/fema_npr.dta
*
* Notes:
*   i) FEMA Hazard Mitigation Assistance Projects:
*      https://www.fema.gov/openfema-data-page/hazard-mitigation-assistance-projects-v4
*      API: https://www.fema.gov/api/open/v4/HazardMitigationAssistanceProjects
*
*   ii) FEMA National Risk Index county-level table:
*      https://hazards.fema.gov/nri/data-resources
*      Download: All Counties - County-level detail (Table)
*   iii) FEMA NFIP Redacted Claims:
*        https://www.fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2
*        API: https://www.fema.gov/api/open/v2/FimaNfipClaims
*
*   iv) NPR/FEMA buyout database from FOIA records:
*       https://www.npr.org/2019/03/05/696995788/search-the-thousands-of-disaster-buyouts-fema-didnt-want-you-to-see
*       CSV: https://apps.npr.org/fema-table/assets/fema.csv
* 
*	v)	FEMA NFIP Redacted Policies: 
*		https://www.fema.gov/openfema-data-page/fima-nfip-redacted-policies-v2
*		API: https://www.fema.gov/api/open/v2/FimaNfipPolicies
********************************************************************************

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments"
local raw  "`root'/data/raw"
local clean "`root'/data/clean"

********************************************************************************
* 1. HazardMitigationAssistanceProjects -> hma_projects.dta
//May 29: restrict to FMA only
********************************************************************************
********************************************************************************
* FEMA Hazard Mitigation Assistance (HMA) Projects
********************************************************************************

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


//2026-05-29 update: remove NRI
/*
********************************************************************************
* 2. NRI -> nri_counties.dta
********************************************************************************
local nri_raw ""
foreach f in ///
    "`raw'/nri_counties.csv" {
    capture confirm file "`f'"
    if _rc == 0 & "`nri_raw'" == "" local nri_raw "`f'"
}

if "`nri_raw'" == "" {
    di as error "SKIP: NRI raw file not found in data/raw."
}
else {
    import delimited using "`nri_raw'", clear varnames(1) stringcols(_all)
    rename *, lower

    keep  statenameabbreviation statefipscode ///
         countyname  countyfipscode statecountyfipscode ///
            v81  v315 

    drop if missing(statecountyfipscode) & missing(countyname)
	rename v81 coastalfloodriskscore
	rename v315 inlandfloodriskscore
    foreach v in statefipscode countyfipscode statecountyfipscode ///
        coastalfloodriskscore inlandfloodriskscore {
        capture destring `v', replace force
    }
    label data "Clean source: National Risk Index counties"
	order statenameabbreviation statefipscode countyname countyfipscode statecountyfipscode
    compress
    save "`clean'/fema/nri_counties.dta", replace
}

*/
********************************************************************************
* 3. FIMaNFIPClaimsV2 -> nfip_claims.dta
********************************************************************************

local nfip_raw ""

foreach f in ///
    "`raw'/FIMaNFIPClaimsV2.csv" {

    capture confirm file "`f'"
    if !_rc & "`nfip_raw'" == "" local nfip_raw "`f'"
}

if "`nfip_raw'" == "" {
    di as error "SKIP: NFIP claims raw file not found."
}
else {
    import delimited using "`nfip_raw'", clear varnames(1) stringcols(_all)
    rename *, lower

    keep asofdate dateofloss yearofloss state reportedcity reportedzipcode countycode ///
         nfipcommunityname censustract censusblockgroupfips latitude longitude ///
         policycount occupancytype primaryresidenceindicator originalconstructiondate ///
         elevatedbuildingindicator elevationcertificateindicator elevationdifference ///
         basefloodelevation ratedfloodzone floodzonecurrent waterdepth floodevent ///
         eventdesignationnumber causeofdamage buildingdamageamount contentsdamageamount ///
         amountpaidonbuildingclaim amountpaidoncontentsclaim ///
         amountpaidonincreasedcostofcompl netbuildingpaymentamount ///
         netcontentspaymentamount neticcpaymentamount totalbuildinginsurancecoverage ///
         totalcontentsinsurancecoverage buildingpropertyvalue contentspropertyvalue ///
         buildingreplacementcost contentsreplacementcost numberofunits id originalnbdate originalconstructiondate  primaryresidenceindicator postfirmconstructionindicator  rentalpropertyindicator floodproofedindicator nfipratedcommunitynumber  //2026-05-31: added variables to match what was kept in policy

    drop if missing(id) & missing(yearofloss) & missing(state) & missing(countycode)

    foreach v in yearofloss countycode policycount occupancytype latitude longitude ///
        primaryresidenceindicator elevatedbuildingindicator ///
        elevationcertificateindicator elevationdifference basefloodelevation waterdepth ///
        buildingdamageamount contentsdamageamount amountpaidonbuildingclaim ///
        amountpaidoncontentsclaim amountpaidonincreasedcostofcompl ///
        netbuildingpaymentamount netcontentspaymentamount neticcpaymentamount ///
        totalbuildinginsurancecoverage totalcontentsinsurancecoverage ///
        buildingpropertyvalue contentspropertyvalue buildingreplacementcost ///
        contentsreplacementcost numberofunits postfirmconstructionindicator floodproofedindicator nfipratedcommunitynumber rentalpropertyindicator {

        capture confirm variable `v'
        if !_rc destring `v', replace force
    }
	rename buildingpropertyvalue nfipbuildingpropertyvalue

    label data "Clean source: NFIP Claims"

    label var yearofloss "Year of loss"
    label var state "State abbreviation"
    label var reportedcity "Reported city"
    label var reportedzipcode "Reported ZIP code"
    label var countycode "County FIPS code"
    label var buildingdamageamount "Building damage amount"
    label var amountpaidonbuildingclaim "Amount paid on building claim"
    label var netbuildingpaymentamount "Net building payment amount"
    label var nfipbuildingpropertyvalue "Building property value from NFIP"
	
	keep state countycode yearofloss dateofloss ///
     buildingdamageamount nfipbuildingpropertyvalue ///
     amountpaidonbuildingclaim ///
     occupancytype primaryresidenceindicator ///
     elevatedbuildingindicator  ///
	 originalnbdate originalconstructiondate  primaryresidenceindicator postfirmconstructionindicator  /// 2026-05-31
	 rentalpropertyindicator floodproofedindicator nfipratedcommunitynumber /// 2026-05-31
	 ratedfloodzone floodzonecurrent id latitude longitude //2026-05-31: kept id for matching to nfip policies potentially, same latitude and longitude
     //basefloodelevation elevationdifference waterdepth amountpaidonincreasedcostofcompl /// 2026-05-31
      
	 

	//2026-05-31 edits
foreach v in dateofloss originalconstructiondate originalnbdate {
    gen `v'_d = daily(substr(`v',1,10), "YMD")
    format `v'_d %td
	drop `v'
	rename `v'_d `v'
}

	//
	order yearofloss dateofloss state countycode ratedfloodzone floodzonecurrent

    compress
    save "`clean'/nfip_claims.dta", replace
}

//2026-05-29 note: kept occupancytype for future more filters to be applied (ie single house, primary residence)
//occupancytype: 1 = single family residence, 11: single-family reidential building, 16: single residential unit within a multi-unit building
/*
********************************************************************************
* 4. fema_npr -> fema_npr.dta
//2026-05-29: edit: removing buyout data from merge pipeline and clean files
********************************************************************************
//note: for future processing, structure is kept to filter to single family homes
local npr_raw ""
foreach f in ///
    "`raw'/fema_npr.csv"  {
    capture confirm file "`f'"
    if _rc == 0 & "`npr_raw'" == "" local npr_raw "`f'"
}

if "`npr_raw'" == "" {
    di as error "SKIP: fema_npr raw file not found in data/raw."
}
else {
    import delimited using "`npr_raw'", clear varnames(1) stringcols(_all)
    rename *, lower
    capture rename fiscal_year fiscalyear
    capture rename price_paid pricepaid

    keep id fiscalyear disasterdescription residence owner structure city state zip ///
         pricepaid status
    drop if missing(id) & missing(fiscalyear) & missing(state) & missing(zip)

    destring fiscalyear residence owner structure pricepaid zip, replace force
    tostring zip, gen(zip_code) format("%05.0f") force
    replace zip_code = "" if zip_code == "." | zip_code == "00000"

    label data "Clean source: FEMA NPR"
    label var id "FEMA NPR record ID"
    label var fiscalyear "Fiscal year"
    label var disasterdescription "Disaster description"
    label var state "State abbreviation"
    label var zip_code "ZIP code, 5-digit string"
    label var pricepaid "Price paid"
    label var status "Record status"
	drop id zip_code
	order state zip fiscalyear city
    compress
    save "`clean'/fema/fema_npr.dta", replace
}

********************************************************************************
* 5. attom_tx / attom_va 
********************************************************************************
// note: this is 44 GB and 20 GB respectively, so they will be left as is until needed for future processing. 

********************************************************************************
* 6. builty_all 
********************************************************************************
//note: this is 20.43 GB so i will keep it as is and not put it in clean. clean will still be the builty_all_elevation filter applied.

di as result "Done creating clean raw-source files."
*/
********************************************************************************
* 7. nfip_policies clean up -> nfip_policies_state.dta
********************************************************************************
// note:initial processing is done in python already,including splitting the 30+GB file and extracting relevant state only. this is cleaning up further
foreach st in tx va {

    use "`clean'/nfip_policies_`st'.dta", clear

    keep id property_state_clean latitude longitude ///
        policy_effective_date policy_termination_date policy_effective_year ///
        original_nb_date original_construction_year ///
        rated_flood_zone flood_zone_current  ///
        policy_count primary_residence single_family_policy is_elevated post_firm ///
        rental_property_ind floodproofed_ind mandatory_purchase_flag ///
        fips_county zip5 total_policy_premium total_building_coverage total_contents_coverage  nfip_rated_community_number
		//base_flood_elevation elevation_difference


foreach v in policy_effective_date policy_termination_date original_nb_date {
    capture confirm string variable `v'
    if !_rc {
        gen double `v'_num = clock(`v', "YMDhms")
        replace `v'_num = clock(substr(`v', 1, 19), "YMDhms") if missing(`v'_num)
        gen `v'_d = dofc(`v'_num)
        format `v'_d %td
        drop `v'_num `v'
        rename `v'_d `v'
    }
}

foreach v in latitude longitude ///
    policy_count total_policy_premium total_building_coverage total_contents_coverage ///
    original_construction_year policy_effective_year ///
    rental_property_ind floodproofed_ind mandatory_purchase_flag nfip_rated_community_number fips_county zip5{

    capture confirm string variable `v'
    if !_rc {
        destring `v', replace ignore(", $") force
    }
}
recast double latitude longitude total_policy_premium total_building_coverage total_contents_coverage, force

recast int policy_effective_year original_construction_year, force
recast byte policy_count primary_residence single_family_policy is_elevated post_firm, force

label variable total_contents_coverage "Total contents coverage amount"

gen double total_coverage = total_building_coverage + total_contents_coverage
label variable total_coverage "Total building plus contents coverage"

gen double premium_per_1000_coverage = total_policy_premium / (total_coverage / 1000) if total_coverage > 0
label variable premium_per_1000_coverage "Premium per $1,000 of total coverage"

gen has_building_policy = total_building_coverage > 0 if !missing(total_building_coverage)
label variable has_building_policy "Policy has building coverage"

gen has_contents_policy = total_contents_coverage > 0 if !missing(total_contents_coverage)
label variable has_contents_policy "Policy has contents coverage"

capture label define yesno 0 "No" 1 "Yes", replace
label values primary_residence yesno
label values single_family_policy yesno
label values is_elevated yesno
label values post_firm yesno
label values has_building_policy yesno
label values has_contents_policy yesno
    label variable id "FEMA NFIP policy record ID"
    label variable property_state_clean "Property state"
    label variable latitude "Property latitude"
    label variable longitude "Property longitude"
    label variable fips_county "County FIPS code"
    label variable zip5 "5-digit ZIP code"

    label variable policy_effective_date "Policy effective date"
    label variable policy_effective_year "Policy effective year"
    label variable policy_termination_date "Policy termination date"
    label variable original_nb_date "Original new business policy date"
    label variable original_construction_year "Original construction year"

    label variable primary_residence "Primary residence policy"
    label variable single_family_policy "Single-family residential policy"
    label variable rental_property_ind "Rental property indicator"
    label variable is_elevated "Elevated building indicator"
    label variable floodproofed_ind "Floodproofed building indicator"
    label variable mandatory_purchase_flag "Mandatory purchase requirement flag"
    label variable post_firm "Post-FIRM construction indicator"

    label variable policy_count "Policy count"
    label variable total_policy_premium "Total policy premium"
    label variable total_building_coverage "Total building coverage amount"
    label variable total_contents_coverage "Total contents coverage amount"

    label variable rated_flood_zone "NFIP rated flood zone"
    label variable flood_zone_current "Current flood zone"

    label variable nfip_rated_community_number "NFIP rated community number"

    capture label define yesno 0 "No" 1 "Yes", replace
    label values primary_residence yesno
    label values single_family_policy yesno
    label values is_elevated yesno
    label values post_firm yesno

    save "`clean'/nfip_policies_`st'.dta", replace
}

/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-05-31

Description: Cleans the FEMA NFIP redacted claims (FimaNfipClaimsV2) ->
    clean/nfip_claims.dta.

Source: fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2
******************************************************************************/

* Data root passed from master.do as the first argument
args data
local raw   "`data'/raw"
local clean "`data'/clean"

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

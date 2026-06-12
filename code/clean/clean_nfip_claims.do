/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-06-11

Description: Cleans the FEMA NFIP redacted claims data. 

Source: fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2
******************************************************************************/

args data

* Import data
import delimited using "`data'/raw/FIMaNFIPClaimsV2.csv", clear varnames(1) stringcols(_all)

* Keep relevant variables 
// Note: This list will have to be revisited
keep asofdate dateofloss yearofloss state reportedzipcode countycode ///
     nfipcommunityname censustract censusblockgroupfips latitude longitude ///
     policycount occupancytype primaryresidenceindicator originalconstructiondate ///
     elevatedbuildingindicator elevationcertificateindicator elevationdifference ///
     basefloodelevation ratedfloodzone floodzonecurrent waterdepth floodevent ///
     eventdesignationnumber causeofdamage buildingdamageamount contentsdamageamount ///
     amountpaidonbuildingclaim amountpaidoncontentsclaim ///
     amountpaidonincreasedcostofcompl netbuildingpaymentamount ///
     netcontentspaymentamount neticcpaymentamount totalbuildinginsurancecoverage ///
     totalcontentsinsurancecoverage buildingpropertyvalue contentspropertyvalue ///
     buildingreplacementcost contentsreplacementcost numberofunits id originalnbdate ///
     originalconstructiondate  primaryresidenceindicator postfirmconstructionindicator ///
     rentalpropertyindicator floodproofedindicator nfipratedcommunitynumber  


* Destring 
destring *, replace 

stop 

	rename buildingpropertyvalue nfipbuildingpropertyvalue
	
	keep state countycode yearofloss dateofloss ///
     buildingdamageamount nfipbuildingpropertyvalue ///
     amountpaidonbuildingclaim ///
     occupancytype primaryresidenceindicator ///
     elevatedbuildingindicator  ///
	 originalnbdate originalconstructiondate  primaryresidenceindicator postfirmconstructionindicator  /// 2026-05-31
	 rentalpropertyindicator floodproofedindicator nfipratedcommunitynumber /// 2026-05-31
	 ratedfloodzone floodzonecurrent id latitude longitude //2026-05-31: kept id for matching to nfip policies potentially, same latitude and longitude
     basefloodelevation elevationdifference waterdepth amountpaidonincreasedcostofcompl /// 2026-05-31
      
	 

	//2026-05-31 edits
foreach v in dateofloss originalconstructiondate originalnbdate {
    gen `v'_d = daily(substr(`v',1,10), "YMD")
    format `v'_d %td
	drop `v'
	rename `v'_d `v'
}

* Label variables 
label var yearofloss                    "Year of loss"
label var state                         "State abbreviation"
label var reportedzipcode               "ZIP code"
label var countycode                    "County FIPS code"
label var buildingdamageamount          "Building damage amount"
label var amountpaidonbuildingclaim     "Amount paid on building claim"
label var netbuildingpaymentamount      "Net building payment amount"
label var nfipbuildingpropertyvalue     "Building property value from NFIP"

* Save data 
order yearofloss dateofloss state countycode ratedfloodzone floodzonecurrent
save "`data'/clean/nfip_claims.dta", replace

//2026-05-29 note: kept occupancytype for future more filters to be applied (ie single house, primary residence)
//occupancytype: 1 = single family residence, 11: single-family reidential building, 16: single residential unit within a multi-unit building

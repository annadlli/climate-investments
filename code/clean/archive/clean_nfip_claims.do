/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-06-11

Description: Cleans the FEMA NFIP redacted claims data. 

Source: fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2

NOTE: Unclear what we might use this data for, so pausing data cleanup here. 
******************************************************************************/

args data

* Import data
import delimited using "`data'/raw/FIMaNFIPClaimsV2.csv", clear varnames(1) stringcols(_all)

* Restrict to single-family homes
keep if inlist(occupancytype, "1", "11")
drop if inlist("1", agriculturestructureindicator, stateownedindicator)
keep if inlist("1", buildingdescriptioncode) | mi(buildingdescriptioncode)

* Drop irrelevant variables 
// Note: Should expand on this list later. 
drop reportedcity floodevent eventdesignationnumber fico floodcharacteristicsindicator ///
     houseworship numberoffloorsintheinsuredbuildi causeofdamage waterdepth ///
     floodwaterduration nfipcommunityname nonprofitindicator nonpaymentreasonbuilding /// 
     nonpaymentreasoncontents stateownedindicator agriculturestructureindicator ///
     policycount occupancytype buildingdescriptioncode basementenclosurecrawlspacetype ///
     floodproofedindicator obstructiontype dateofloss 

// nfipcommunitynumbercurrent <- adds more current information?

* Destring everything except the string match keys
ds reportedzipcode countycode censustract censusblockgroupfips nfipratedcommunitynumber, not
destring `r(varlist)', replace 

* Rename 
ren (elevatedbuildingindicator reportedzipcode) (elevated zipcode) 

* Label 
label var elevated       "Elevated building"

* Save data 
order id yearofloss state zipcode countycode censustract latitude longitude
sort yearofloss state zipcode
save "`data'/clean/nfip_claims.dta", replace


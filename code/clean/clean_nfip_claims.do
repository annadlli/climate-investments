/******************************************************************************
Authors: Vendela Norman
Date: 2026-08-27

Description: Cleans the FEMA NFIP redacted claims data. 

Source: fema.gov/openfema-data-page/fima-nfip-redacted-claims-v2

******************************************************************************/

args data

* -----------------------------------------------------------------------------
* Section 1: Initial import and cleaning
* -----------------------------------------------------------------------------

* Import data
import delimited using "`data'/raw/FIMaNFIPClaimsV2.csv", clear varnames(1) stringcols(_all)

* Impose sample restrictions, following NFIP policies data cleaning
// i) Restrict to single-family homes
keep if inlist(occupancytype, "1", "11") // single-family residential
drop if inlist("1", agriculturestructureindicator, stateownedindicator) // not agricultural structure or state-owned
keep if inlist("1", buildingdescriptioncode) | mi(buildingdescriptioncode) // main house
// ii) Eliminate homes with unrealistic construction year 
gen construction_year = real(substr(originalconstructiondate, 1, 4))
replace construction_year = . if !inrange(construction_year, 1700, 2027)
drop if mi(construction_year)
// iii) Drop homes w/ missing merge variables
drop if mi(censusblockgroupfips) | mi(originalnbdate) 

 * Create approximate property id
// Note: This helps identify unique properties (but cannot be used for merging with NFIP policies)
egen property_id = group(originalconstructiondate censusblockgroupfips originalnbdate)

* Keep relevant variables 
keep property_id originalconstructiondate censusblockgroupfips originalnbdate state reportedzipcode ///
     dateofloss yearofloss amountpaidonbuildingclaim amountpaidoncontentsclaim buildingpropertyvalue ///
     ratedfloodzone numberoffloors totalbuildinginsurancecoverage totalcontentsinsurancecoverage ///
     buildingdamageamount buildingdeductiblecode netbuildingpaymentamount netcontentspaymentamount ///
     buildingreplacementcost

 * Rename
ren (reportedzipcode yearofloss netbuildingpaymentamount netcontentspaymentamount ///
     totalbuildinginsurancecoverage totalcontentsinsurancecoverage buildingpropertyvalue ///
     ratedfloodzone) ///
    (zipcode year_loss claim_building claim_contents coverage_building coverage_contents ///
     property_val_nfip flood_zone)

* Destring 
ds state zipcode censusblockgroupfips dateofloss originalconstructiondate originalnbdate ///
    flood_zone buildingdeductiblecode, not
destring `r(varlist)', replace

* Convert merge variables to date format
foreach v of varlist originalnbdate originalconstructiondate {
    gen _d = date(substr(`v',1,10), "YMD")
    drop `v'
    rename _d `v'
    format `v' %td
}

* -----------------------------------------------------------------------------
* Section 2: Create analysis variables & save
* -----------------------------------------------------------------------------

* Merge in CPI
ren year_loss year
merge m:1 year using "`data'/clean/cpi.dta", assert(2 3) keep(1 3) keepusing(cpi) nogen
ren year year_loss

* Use "net" payments
// Note: Claims are largely the same but these correct for oddities like uncashed checks
replace claim_building = amountpaidonbuildingclaim if mi(claim_building) | claim_building < 0
replace claim_contents = amountpaidoncontentsclaim if mi(claim_contents) | claim_contents < 0
drop amountpaidonbuildingclaim amountpaidoncontentsclaim

* Drop claims above the cap 
// TODO:
stop 
drop if claim_cb > 250000 // NFIP claims cap

* Deflate nominal variables
foreach var in claim_building claim_contents property_val_nfip {
    replace `var' = `var' / cpi if !mi(`var') & !mi(cpi)
}
drop cpi

* Create additional variables
egen claim_cb = rowtotal(claim_building claim_contents) // total claims across building + contents

* Drop rejected and uncashed claims 
drop if mi(claim_cb) | claim_cb <= 0 // negative = uncashed 

* Drop duplicates
duplicates drop

* Collapse to the property-year level
// Note: One raw record is one payment transaction, so dollar amounts are summed
// within property-year
bys property_id year_loss: gen n_records = _N // number of transactions per property-year
foreach var in claim_building claim_contents claim_cb {
    bys property_id year_loss: egen _t = total(`var')
    replace `var' = _t
    drop _t
}
bys property_id year_loss (dateofloss): keep if _n == 1
isid property_id year_loss

* Drop additional variables 
drop dateofloss n_records numberoffloors buildingdeductiblecode buildingdamageamount buildingreplacementcost

* Label variables
label var state                    "State"
label var zipcode                  "ZIP code"
label var censusblockgroupfips     "Census block group"
label var property_id              "Approximate property ID"
label var flood_zone               "NFIP rated flood zone"
label var year_loss                "Year of loss"
label var claim_building           "Net amount paid on building claim (2023 $)"
label var claim_contents           "Net amount paid on contents claim (2023 $)"
label var claim_cb                 "Total net amount paid, building + contents (2023 $)"
label var property_val_nfip        "Building property value (NFIP, 2023 $)"
label var coverage_building        "Total building insurance coverage"
label var coverage_contents        "Total contents insurance coverage"
label var originalconstructiondate "Original construction date"
label var originalnbdate           "Original new-business policy date"

* Save data
order state zipcode censusblockgroupfips property_id flood_zone year_loss claim*
order originalconstructiondate originalnbdate, last
sort state zipcode censusblockgroupfips property_id year_loss 
compress
save "`data'/clean/nfip_claims_panel.dta", replace

* -----------------------------------------------------------------------------
* Section 3: Collapse to property-level data & save
* -----------------------------------------------------------------------------

* Collapse to the property-level
collapse (sum) claim_cb ///
     (first) originalconstructiondate censusblockgroupfips originalnbdate state zipcode, by(property_id)

* Label variables
label var property_id              "Approximate property ID"
label var claim_cb                 "Total net amount paid, all loss years (2023 $)"
label var originalconstructiondate "Original construction date"
label var censusblockgroupfips     "Census block group"
label var originalnbdate           "Original new-business policy date"
label var state                    "State"
label var zipcode                  "ZIP code"

* Save data
save "`data'/clean/nfip_claims_property.dta", replace
/******************************************************************************
Author: Vendela Norman
Date: 2026-07-10

Description: Cleans the FEMA HMA Mitigated Properties file (property-level FMA
    mitigation records) and restricts to the FMA single-family elevation universe. 

Source: https://catalog.data.gov/dataset/hazard-mitigation-assistance-mitigated-properties
******************************************************************************/

args data

* Import data
import delimited "`data'/raw/hma_mitigated_properties.csv", clear stringcols(_all)

* Drop irrelevant variables 
drop disasternumber 

* Restrict to FMA single-family elevations
keep if programarea == "FMA" | programarea == "SRL" 
keep if propertyaction == "Elevation"
keep if structuretype == "Single Family"

* Destring 
destring numberofproperties, replace 

/* * Label
label var id                 "FEMA record id"
label var projectidentifier  "HMA project id (links to Projects file)"
label var programfy          "Program fiscal year"
label var propertyaction     "Mitigation action (Elevation)"
label var structuretype      "Structure type (Single Family)"
label var foundationtype     "Foundation type"
label var state              "State"
label var county             "County name"
label var city               "City"
label var zip                "ZIP (5-digit; finest FMA geography)"
label var damagecategory     "Damage category"
label var actualamountpaid   "Actual amount paid (nominal \$; ~2% filled)"
label var numberofproperties "Number of properties in record"
label var disasternumber     "Disaster number"

* Order, sort, save
order id projectidentifier state county city zip programfy propertyaction ///
      structuretype foundationtype damagecategory actualamountpaid numberofproperties disasternumber
sort state county zip programfy
compress */

* Save 
save "`data'/clean/fma_elevation_properties.dta", replace

/******************************************************************************
Authors: Vendela Norman
Date: 2026-06-19

Description: Cleans the FEMA NFIP Multiple Loss Properties data. Source of
    RL/SRL status for FMA prioritization.

Source: fema.gov/openfema-data-page/nfip-multiple-loss-properties-v1
******************************************************************************/

args data

* Import data
import delimited using "`data'/raw/NfipMultipleLossProperties.csv", clear ///
    varnames(1) stringcols(_all)

* Drop irrelevant variables
drop latitude longitude primaryresidenceindicator id 
stop 

* Rename (align geo keys to the policies names for the build merge)
ren (stateabbreviation fipscountycode communityidnumber censusblockgroup) ///
    (state countycode community censusblockgroupfips)

* Create additional variables
// i) Construction year  
gen construction_year = real(substr(originalconstructiondate, 1, 4))
drop originalconstructiondate 

* Destring everything except the string match keys (protect zero-padded codes)
ds id state countycode zipcode community censusblockgroupfips originalnbdate, not
destring `r(varlist)', replace

* Label variables
label var id                   "FEMA NFIP MLP record ID"
label var state                "State"
label var countycode           "County FIPS"
label var zipcode              "ZIP code"
label var community            "NFIP community ID number"
label var censusblockgroupfips "Census block group"
label var originalnbdate       "Original new-business policy date"
label var nfiprl               "NFIP repetitive loss (insurance defn)"
label var nfipsrl              "NFIP severe repetitive loss (insurance defn)"
label var fmarl                "FMA repetitive loss (grant defn)"
label var fmasrl               "FMA severe repetitive loss (grant defn)"

* Save
order id state countycode zipcode community censusblockgroupfips ///
    originalnbdate construction_year fmarl fmasrl nfiprl nfipsrl ///
    mitigatedindicator totallosses 
sort state countycode zipcode
compress
save "`data'/clean/nfip_multiple_loss.dta", replace

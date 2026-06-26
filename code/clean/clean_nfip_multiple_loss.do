/******************************************************************************
Authors: Vendela Norman
Date: 2026-06-25

Description: Cleans the FEMA NFIP Multiple Loss Properties data. Source of
    RL/SRL status for FMA prioritization.

Source: fema.gov/openfema-data-page/nfip-multiple-loss-properties-v1
******************************************************************************/

args data

* Import data
import delimited using "`data'/raw/NfipMultipleLossProperties.csv", clear ///
    varnames(1) stringcols(_all)

* Drop irrelevant variables
drop state primaryresidenceindicator communityname latitude longitude ///
    postfirmconstructionindicator asofdate

* Rename 
ren (stateabbreviation fipscountycode communityidnumber censusblockgroup ///
    nfiprl nfipsrl fmarl fmasrl reportedcity) ///
    (state countycode community censusblockgroupfips nfip_rl nfip_srl ///
    fma_rl fma_srl city)

* Destring 
ds countycode zipcode community censusblockgroupfips, not
destring `r(varlist)', replace

* Convert merge variables to date format
foreach v of varlist originalnbdate originalconstructiondate {
    gen _d = date(substr(`v',1,10), "YMD")
    drop `v'
    rename _d `v'
    format `v' %td
}

* Restrict to single-family homes
keep if inlist(occupancytype, 1, 11) 
drop occupancytype

* Drop missing data 
drop if mi(censusblockgroupfips) | mi(originalnbdate) | mi(originalconstructiondate) 

* Drop duplicates 
duplicates drop

* Create approximate property id
// Note: This helps identify unique properties, but cannot be used for merging with NFIP policies
egen property_id = group(originalconstructiondate censusblockgroupfips originalnbdate)

* Drop remaining duplicates 
bys property_id (id): drop if _n > 1
isid property_id
drop property_id id 

* Label variables
label var state                    "State"
label var countycode               "County FIPS"
label var zipcode                  "ZIP code"
label var community                "NFIP community ID number"
label var censusblockgroupfips     "Census block group"
label var county                   "County name"
label var city                     "City"
label var originalnbdate           "Original new-business policy date"
label var originalconstructiondate "Original construction date"
label var mostrecentdateofloss     "Most recent date of loss"
label var floodzone                "FEMA flood zone"
label var insuredindicator         "Currently insured (NFIP)"
label var mitigatedindicator       "Property mitigated indicator"
label var totallosses              "Total number of losses"
label var fma_rl                   "FMA repetitive loss (grant)"
label var fma_srl                  "FMA severe repetitive loss (grant)"
label var nfip_rl                  "NFIP repetitive loss (insurance)"
label var nfip_srl                 "NFIP severe repetitive loss (insurance)"

* Save
order state countycode zipcode community censusblockgroupfips originalnbdate ///
    originalconstruction floodzone insured fma_rl fma_srl nfip_rl nfip_srl ///
    mitigatedindicator totallosses 
order county city, last
sort state zipcode censusblockgroupfips 
compress
save "`data'/clean/nfip_multiple_loss.dta", replace

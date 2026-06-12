/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-06-12

Description: Cleans the FEMA NFIP Multiple Loss Properties data -- the property-
    level repetitive-loss (RL) / severe-repetitive-loss (SRL) roster. Source of
    RL/SRL status for FMA prioritization; cell-matched onto the eligible-homes
    universe. (RL/SRL is NOT derivable from redacted claims -- no property key.)

    Download (full 240k-row CSV; data page is browser-only):
    curl -sL "https://www.fema.gov/api/open/v1/NfipMultipleLossProperties.csv" \
      -o "<data>/raw/NfipMultipleLossProperties.csv"

Source: fema.gov/openfema-data-page/nfip-multiple-loss-properties-v1

NOTE: SKETCH -- verify against the data once downloaded (occupancyType values;
    whether yes/no flags are stored as 'true'/'false' vs '1'/'0').
******************************************************************************/

args data

* Import
import delimited using "`data'/raw/NfipMultipleLossProperties.csv", clear varnames(1) stringcols(_all)

* Restrict to single-family homes
* NOTE: MLP uses 1-digit occupancy codes only -- verify values like we did for claims.
keep if occupancytype == "1"

* Drop irrelevant variables (keep broad otherwise)
drop state county reportedcity communityname asofdate   // full names/timestamp; keep abbrev + fips + community number

* Destring everything except the zero-padded string match keys
ds fipscountycode zipcode communityidnumber censusblockgroup, not
destring `r(varlist)', replace

* Yes/No flags -> byte 0/1  (codebook stores these as 'true'/'1')
foreach v in nfiprl nfipsrl fmarl fmasrl mitigatedindicator ///
             insuredindicator postfirmconstructionindicator primaryresidenceindicator {
    gen byte `v'_b = inlist(lower(`v'), "1", "true", "yes", "y")
    drop `v'
    rename `v'_b `v'
}

* Dates -> years (ISO "YYYY-MM-DD..." strings)
gen int construction_year   = real(substr(originalconstructiondate, 1, 4))
replace construction_year = . if !inrange(construction_year, 1700, 2027)
gen int most_recent_loss_yr = real(substr(mostrecentdateofloss, 1, 4))
drop originalconstructiondate mostrecentdateofloss

* Rename for brevity
ren stateabbreviation state
ren communityidnumber  community

* Label key variables
label var fipscountycode      "County FIPS (5-digit)"
label var community           "NFIP community ID number"
label var fmarl               "FMA repetitive loss (grant defn)"
label var fmasrl              "FMA severe repetitive loss (grant defn)"
label var nfiprl              "NFIP repetitive loss (insurance defn)"
label var nfipsrl             "NFIP severe repetitive loss (insurance defn)"
label var mitigatedindicator  "Structure mitigated as of data date"
label var totallosses         "Paid NFIP claims >\$1k since 1978"
label var construction_year   "Original construction year"

* Order, sort, save
order id state fipscountycode zipcode community censusblockgroup floodzone ///
      construction_year fmarl fmasrl nfiprl nfipsrl mitigatedindicator totallosses
sort state fipscountycode zipcode
save "`data'/clean/nfip_multiple_loss.dta", replace

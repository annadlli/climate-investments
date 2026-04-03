*******************************************************
* merge_panel.do
* Build county × year analysis panel by merging:
*   1. flood_elevation_filters.dta  (permit-level → county×year)
*   2. hma_tx_countyyear.dta        (county×year×program → county×year)
*   3. nri_clean.dta                (county cross-section)
*
* Output: merged_panel.dta
* Unit: county × year
*******************************************************

clear all
set more off

* -------------------------------------------------------
* 0. Paths
* -------------------------------------------------------
local root "/Users/anna/Desktop/Research/climate-investments/data"
local permits "`root'/flood_elevation_filters.dta"
local hma     "`root'/fema/hma_tx_countyyear.dta"
local nri     "`root'/fema/nri_clean.dta"
local out     "`root'/merged_panel.dta"

*******************************************************
* 1. Permit data: clean county FIPS, extract year, collapse
*******************************************************
use "`permits'", clear

* Clean malformed county FIPS
* Example malformed pattern: "4885" should become "48085"
gen str5 fips_county = trim(SITUSSTATECOUNTYFIPS)
replace fips_county = substr(fips_county, 1, 2) + "0" + substr(fips_county, 3, .) ///
    if length(fips_county) == 4

assert length(fips_county) == 5 if !missing(fips_county)
label var fips_county "5-digit county FIPS (cleaned)"

* Permit year
gen year = year(dofc(PERMIT_DATE))
label var year "Permit year"

* Keep flood-elevation permits only (broad)
keep if flood_elev_broad == 1
keep if year >= 2000 & !missing(year)

* Indicators for collapse
gen one_permit = 1
gen residential_flag = RESIDENTIAL == 1 if !missing(RESIDENTIAL)

collapse ///
    (count)  n_elev_permits  = one_permit ///
    (sum)    total_job_value = JOB_VALUE ///
    (mean)   avg_job_value   = JOB_VALUE ///
    (median) med_job_value   = JOB_VALUE ///
    (sum)    n_residential   = residential_flag ///
    (mean)   avg_prop_value  = TAXMARKETVALUETOTAL ///
    , by(fips_county year)

label var n_elev_permits  "Number of flood elevation permits"
label var total_job_value "Total job value of elevation permits ($)"
label var avg_job_value   "Mean job value of elevation permits ($)"
label var med_job_value   "Median job value of elevation permits ($)"
label var n_residential   "Number of residential elevation permits"
label var avg_prop_value  "Mean property market value ($)"

sort fips_county year
isid fips_county year

tempfile permits_cy
save `permits_cy'

*******************************************************
* 2. HMA data: collapse county×year×program → county×year
*******************************************************
use "`hma'", clear

* Make sure keys are clean
capture confirm string variable fips_county
if _rc == 0 {
    replace fips_county = trim(fips_county)
}
else {
    tostring fips_county, replace format(%05.0f)
}

* Program flags
gen byte has_hmgp = (programarea == "HMGP")
gen byte has_fma  = (programarea == "FMA")
gen byte has_bric = (programarea == "BRIC")
gen byte has_srl  = (programarea == "SRL")

collapse ///
    (sum) n_projects             ///
    (sum) n_elevation            ///
    (sum) n_buyout               ///
    (sum) total_project_amt      ///
    (sum) total_fed_obligated    ///
    (sum) total_funding          ///
    (sum) total_net_benefits     ///
    (sum) total_properties       ///
    (sum) total_final_properties ///
    (mean) avg_bca               ///
    (mean) avg_cost_share_pct    ///
    (max) has_hmgp               ///
    (max) has_fma                ///
    (max) has_bric               ///
    (max) has_srl                ///
    , by(fips_county county year)

label var n_projects              "HMA: total mitigation projects"
label var n_elevation             "HMA: elevation projects"
label var n_buyout                "HMA: buyout/acquisition/relocation projects"
label var total_project_amt       "HMA: total estimated project cost ($)"
label var total_fed_obligated     "HMA: total federal share obligated ($)"
label var total_funding           "HMA: primary funding measure ($)"
label var total_net_benefits      "HMA: total net present value of benefits ($)"
label var total_properties        "HMA: total properties at application"
label var total_final_properties  "HMA: total properties completed"
label var avg_bca                 "HMA: mean benefit-cost ratio"
label var avg_cost_share_pct      "HMA: mean federal cost-share %"
label var has_hmgp                "1 if county-year has any HMGP project"
label var has_fma                 "1 if county-year has any FMA project"
label var has_bric                "1 if county-year has any BRIC project"
label var has_srl                 "1 if county-year has any SRL project"

sort fips_county year
isid fips_county year

tempfile hma_cy
save `hma_cy'

*******************************************************
* 3. NRI data: ensure one row per county
*******************************************************
use "`nri'", clear

capture confirm string variable fips_county
if _rc == 0 {
    replace fips_county = trim(fips_county)
}
else {
    tostring fips_county, replace format(%05.0f)
}

duplicates tag fips_county, gen(dup)
count if dup > 0

* If duplicates exist, collapse numeric vars and keep first nonmissing string vars
* Adjust this if you know nri_clean.dta is already unique
collapse ///
    (firstnm) state_name county_name nri_rating svi_rating ifl_risk_rating ///
              cfl_risk_rating hur_risk_rating community_risk_factor ///
    (mean) population building_value area_sqmi ///
           nri_score nri_state_pctile ///
           eal_total eal_rate_adj_pctile ///
           svi_score resilience_score ///
           ifl_eal_total ifl_eal_building ifl_eal_score ///
           ifl_eal_rate_building ifl_eal_rate_pctile ///
           ifl_risk_score ifl_freq ifl_exp_building ///
           cfl_eal_total cfl_eal_building cfl_risk_score ///
           hur_eal_total hur_eal_building hur_risk_score ///
    , by(fips_county)

sort fips_county
isid fips_county

tempfile nri_xs
save `nri_xs'

*******************************************************
* 4. Build balanced county × year skeleton
*******************************************************
use `permits_cy', clear
keep fips_county
duplicates drop

append using `hma_cy'
keep fips_county
duplicates drop

* Cross with years 2000–2023
expand 24
bysort fips_county: gen year = 1999 + _n

sort fips_county year
isid fips_county year

tempfile skeleton
save `skeleton'

*******************************************************
* 5. Merge permits onto skeleton
*******************************************************
use `skeleton', clear

merge 1:1 fips_county year using `permits_cy', keep(master match) nogen

replace n_elev_permits = 0 if missing(n_elev_permits)
replace n_residential  = 0 if missing(n_residential)

label var n_elev_permits "Flood elevation permits (0 if none filed)"

*******************************************************
* 6. Merge HMA onto panel
*******************************************************
merge 1:1 fips_county year using `hma_cy', keep(master match) nogen

replace n_projects            = 0 if missing(n_projects)
replace n_elevation           = 0 if missing(n_elevation)
replace n_buyout              = 0 if missing(n_buyout)
replace has_hmgp              = 0 if missing(has_hmgp)
replace has_fma               = 0 if missing(has_fma)
replace has_bric              = 0 if missing(has_bric)
replace has_srl               = 0 if missing(has_srl)

replace total_project_amt     = 0 if missing(total_project_amt)
replace total_fed_obligated   = 0 if missing(total_fed_obligated)
replace total_funding         = 0 if missing(total_funding)
replace total_net_benefits    = 0 if missing(total_net_benefits)
replace total_properties      = 0 if missing(total_properties)
replace total_final_properties= 0 if missing(total_final_properties)

*******************************************************
* 7. Merge NRI cross-section onto panel
*******************************************************
merge m:1 fips_county using `nri_xs', keep(master match) nogen

*******************************************************
* 8. Derived variables
*******************************************************
gen elev_per_1000pop = (n_elev_permits / population) * 1000 if population > 0
gen hma_per_1000pop  = (n_projects / population) * 1000 if population > 0
gen fed_oblg_per_cap = total_fed_obligated / population if population > 0
gen elev_permit_rate = n_elev_permits / (building_value / 1000000) if building_value > 0

label var elev_per_1000pop "Flood elevation permits per 1,000 residents"
label var hma_per_1000pop  "HMA projects per 1,000 residents"
label var fed_oblg_per_cap "Federal HMA dollars obligated per capita ($)"
label var elev_permit_rate "Elevation permits per $1M building value"

gen has_hma = n_projects > 0
label var has_hma "1 if county-year has any HMA elevation/buyout project"
*******************************************************
* 9. Checks
*******************************************************
describe

tab ifl_risk_rating if year == 2020
tabstat n_elev_permits n_projects total_fed_obligated ifl_eal_total, ///
    stat(n mean sd p25 p50 p75 p99) col(stat)

count if missing(nri_score)
count if n_elev_permits == 0
count if n_projects == 0

*******************************************************
* 10. Save
*******************************************************
sort fips_county year
save "`out'", replace

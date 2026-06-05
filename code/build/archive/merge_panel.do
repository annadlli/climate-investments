*******************************************************
* merge_panel.do
* Build county × year analysis panel by merging:
*   1. flood_elevation_filters.dta  (permit-level → county×year)
*   2. hma_tx_countyyear.dta        (county×year×program → county×year)
*   3. nri_clean.dta                (county cross-section)
*
* Output: merged_panel.dta
* Unit: county × year (restricted to county-years with at least one
*       elevation permit, FEMA elevation grant, or FEMA buyout)
*
* NOTE on avg_prop_value:
*   This is the mean ATTOM property value across Builty elevation permits
*   in that county-year. It is missing when FEMA made elevation grants in
*   counties/years where Builty has no permit data — a known coverage gap
*   in the Builty database, not a merge error.
*******************************************************

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments/data"
local permits "`root'/flood_elevation_filters.dta"
local hma     "`root'/fema/hma_tx_countyyear.dta"
local nri     "`root'/fema/nri_clean.dta"
local out     "`root'/merged_panel.dta"

*******************************************************
* 1. Permit data: clean FIPS, extract year, collapse to county×year
*******************************************************
use "`permits'", clear

* Fix malformed FIPS (e.g. "4885 " → "48085")
gen str5 fips_county = trim(SITUSSTATECOUNTYFIPS)
replace fips_county = substr(fips_county,1,2) + "0" + substr(fips_county,3,.) ///
    if length(fips_county) == 4
assert length(fips_county) == 5 if !missing(fips_county)

* Permit year (PERMIT_DATE is %tc — must use dofc())
gen year = year(dofc(PERMIT_DATE))

* Keep elevation permits only, 2000+
keep if flood_elev_final == 1 //revise April 9: keep only strict filter
keep if year >= 2000 & !missing(year)

gen byte one = 1
gen byte resid = (RESIDENTIAL == 1) if !missing(RESIDENTIAL)

//only want residential properties
drop if resid == 0 //27 obs dropped
collapse ///
    (sum)    n_permits_builty  = one              ///
    (sum)    n_resid_permits   = resid            ///
    (sum)    total_job_value   = JOB_VALUE        ///
    (mean)   avg_job_value     = JOB_VALUE        ///
    (mean)   avg_prop_value    = TAXMARKETVALUETOTAL ///
    , by(fips_county year)

label var n_permits_builty  "Builty: flood elevation permits filed"
label var n_resid_permits   "Builty: residential elevation permits"
label var total_job_value   "Builty: total elevation job value ($)"
label var avg_job_value     "Builty: mean elevation job value ($)"
label var avg_prop_value    "Builty: mean property market value ($, from ATTOM — missing if no Builty coverage)"

sort fips_county year
isid fips_county year
tempfile permits_cy
save `permits_cy'

//72 obs

*******************************************************
* 2. HMA data: collapse county×year×program → county×year
*    Create indicators for each intervention type × program
*******************************************************
use "`hma'", clear

replace fips_county = trim(fips_county)

* Program flags 
gen byte hmgp = (programarea == "HMGP")
gen byte fma  = (programarea == "FMA")

* Elevation × program
gen byte fema_elev_hmgp = (n_elevation > 0) & (programarea == "HMGP")
gen byte fema_elev_fma  = (n_elevation > 0) & (programarea == "FMA")
gen byte fema_elev_any  = (n_elevation > 0)

* Buyout × program
gen byte fema_buyout_hmgp = (n_buyout > 0) & (programarea == "HMGP")
gen byte fema_buyout_fma  = (n_buyout > 0) & (programarea == "FMA")
gen byte fema_buyout_any  = (n_buyout > 0)

* Elevation project count × program (for sums across programs)
gen n_elev_hmgp   = n_elevation if programarea == "HMGP"
gen n_elev_fma    = n_elevation if programarea == "FMA"
gen n_buyout_hmgp = n_buyout    if programarea == "HMGP"
gen n_buyout_fma  = n_buyout    if programarea == "FMA"
foreach v of varlist n_elev_hmgp n_elev_fma n_buyout_hmgp n_buyout_fma {
    replace `v' = 0 if missing(`v')
}


collapse ///
    (sum)  n_elev_total_fema      = n_elevation        ///
    (sum)  n_buyout_total_fema    = n_buyout           ///
    (sum)  n_elev_hmgp       = n_elev_hmgp        ///
    (sum)  n_elev_fma        = n_elev_fma         ///
    (sum)  n_buyout_hmgp     = n_buyout_hmgp      ///
    (sum)  n_buyout_fma      = n_buyout_fma       ///
    (sum)  n_properties      = total_properties   ///
    (sum)  n_props_completed = total_final_properties ///
    (mean) avg_bca           = avg_bca            ///
    (max)  fema_elev         = fema_elev_any           ///  any FEMA elevation project
    (max)  fema_elev_hmgp    = fema_elev_hmgp          ///  FEMA elevation via HMGP
    (max)  fema_elev_fma     = fema_elev_fma           ///  FEMA elevation via FMA
    (max)  fema_buyout       = fema_buyout_any         ///  any FEMA buyout
    (max)  fema_buyout_hmgp  = fema_buyout_hmgp        ///  FEMA buyout via HMGP
    (max)  fema_buyout_fma   = fema_buyout_fma         ///  FEMA buyout via FMA
    (max)  has_hmgp          = hmgp               ///  any HMGP activity
    (max)  has_fma           = fma                ///  any FMA activity
    , by(fips_county county year)

* Labels
label var n_elev_total      "FEMA: elevation projects (all programs)"
label var n_buyout_total    "FEMA: buyout/acquisition projects (all programs)"
label var n_elev_hmgp       "FEMA: elevation projects via HMGP"
label var n_elev_fma        "FEMA: elevation projects via FMA"
label var n_buyout_hmgp     "FEMA: buyout projects via HMGP"
label var n_buyout_fma      "FEMA: buyout projects via FMA"
label var n_properties      "FEMA: properties in project applications"
label var n_props_completed "FEMA: properties with completed mitigation"
label var avg_bca           "FEMA: mean benefit-cost ratio"
//flags
label var fema_elev         "=1 if any FEMA-funded elevation project this county-year"
label var fema_elev_hmgp    "=1 if FEMA elevation via HMGP this county-year"
label var fema_elev_fma     "=1 if FEMA elevation via FMA this county-year"
label var fema_buyout       "=1 if any FEMA-funded buyout this county-year"
label var fema_buyout_hmgp  "=1 if FEMA buyout via HMGP this county-year"
label var fema_buyout_fma   "=1 if FEMA buyout via FMA this county-year"
label var has_hmgp          "=1 if any HMGP project (any type) this county-year"
label var has_fma           "=1 if any FMA project (any type) this county-year"

sort fips_county year
isid fips_county year
tempfile hma_cy
save `hma_cy'
//189 obs

*******************************************************
* 3. NRI: one row per county
*    Also used to fill in county_name for all obs
*******************************************************
use "`nri'", clear
replace fips_county = trim(fips_county)

* NRI is already unique by fips_county — assert and proceed
isid fips_county

keep fips_county county_name state_name                 ///
     population building_value                ///
     ifl_risk_score ifl_risk_rating cfl_risk_score cfl_risk_rating                    ///
     svi_score svi_rating              
    //nri_score nri_rating             
label var county_name    "County name"
label var population     "County population (NRI)"
label var building_value "Total building replacement value ($, NRI)"
label var ifl_risk_score "Inland flood risk score (0–100)"
label var ifl_risk_rating "Inland flood risk rating"
label var svi_score      "Social vulnerability index score"
label var svi_rating     "Social vulnerability rating"
label var cfl_risk_score "Coastal flood risk score (0–100)"
label var cfl_risk_rating "Coastal flood risk rating"
sort fips_county
tempfile nri_xs
save `nri_xs'

//254 obs
*******************************************************
* 4. Build skeleton: union of counties, 2000–2023
*    Only counties that appear in permits OR HMA data
*    (restricts to counties with meaningful mitigation activity)
*******************************************************
use `permits_cy', clear
keep fips_county
append using `hma_cy', keep(fips_county)
duplicates drop fips_county, force

expand 24
bysort fips_county: gen year = 1999 + _n

sort fips_county year
isid fips_county year
tempfile skeleton
save `skeleton'
//1656 obs
*******************************************************
* 5. Merge all data onto skeleton
*******************************************************
use `skeleton', clear

* 5a. Permits
merge 1:1 fips_county year using `permits_cy', keep(master match) nogen
replace n_permits_builty = 0 if missing(n_permits_builty)
replace n_resid_permits  = 0 if missing(n_resid_permits)

* 5b. HMA
merge 1:1 fips_county year using `hma_cy', keep(master match) nogen

foreach v of varlist n_elev_total n_buyout_total n_elev_hmgp n_elev_fma ///
    n_buyout_hmgp n_buyout_fma   ///
    n_properties n_props_completed ///
    fema_elev fema_elev_hmgp fema_elev_fma ///
    fema_buyout fema_buyout_hmgp fema_buyout_fma ///
    has_hmgp has_fma  {
    replace `v' = 0 if missing(`v')
}

* 5c. NRI (cross-section)
merge m:1 fips_county using `nri_xs', keep(master match) nogen

* Fill county name from HMA data where NRI didn't match
replace county_name = county if missing(county_name) & !missing(county)
drop county  // was from HMA, now redundant

*******************************************************
* 6. Restrict to active county-years
*    Keep only rows with at least one elevation permit,
*    FEMA elevation grant, or FEMA buyout
*    (drops ~1,438 uninformative zero rows)
*******************************************************
keep if n_permits_builty > 0 | fema_elev == 1 | fema_buyout == 1

*******************************************************
* 7. Derived variables
*******************************************************
gen log_n_permits    = log(n_permits_builty + 1)
gen permits_per_1000 = (n_permits_builty / population) * 1000 if population > 0

label var log_n_permits    "Log(Builty elevation permits + 1)"
label var permits_per_1000 "Builty elevation permits per 1,000 residents"

*******************************************************
* 8. Variable order and sort
*    county identifiers first, then outcomes, then controls
*******************************************************
order fips_county county_name year                          

sort fips_county year

*******************************************************
* 9. Checks
*******************************************************
di "Observations: " _N " (should be ~218 after restriction)"
di "Counties: " `: di _N/24'  // approx

tab ifl_risk_rating

tabstat n_permits_builty fema_elev fema_buyout avg_prop_value, ///
    stat(n mean p25 p50 p75) col(stat)

* Any remaining missing county names?
count if missing(county_name)
di "Missing avg_prop_value among county-years with Builty permits"
count if n_permits_builty > 0 & missing(avg_prop_value)
count if missing(county_name)
count if n_permits_builty == 0 & fema_elev == 0 & fema_buyout == 0
isid fips_county year
//all missing average_prop_value are because there are no builty records for that county-year

*******************************************************
* 10. Save
*******************************************************
describe
local k = r(k)

di "Saved: merged_panel.dta (" _N " obs, " `k' " variables)"
save "`out'", replace
//218 obs

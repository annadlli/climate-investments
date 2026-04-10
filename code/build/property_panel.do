* property_panel.do
* Build a property-level analysis dataset.
*
* Base: flood_elevation_filters.dta — restricted to flood elevation
*       permits (flood_elev_broad == 1), 2000 onward
*
* Merges (all m:1 — permit is finest grain):
*   1. ClimateRisk   → m:1 zip_code       (ZIP cross-section)
*   2. NRI           → m:1 fips_county    (county cross-section)
*   3. NFIP claims   → m:1 fips_county × permit_year
*   4. HMA projects  → m:1 fips_county × permit_year
*
* Output: data/property_panel.dta
*
* Notes on known data gaps:
*   avg_prop_value comes from ATTOM (via Builty). It is missing for
*   permits where ATTOM has no property record — not a merge error.
*   FEMA buyouts have no equivalent in the Builty permit data; the
*   Builty DEMOLITION flag is demolition permits, not acquisitions.

clear all
set more off

local root "/Users/anna/Desktop/Research/climate-investments/data"

********************************************************************************
* 1. Load and restrict base permit data
********************************************************************************
use "`root'/flood_elevation_filters.dta", clear

* --- 1a. Keep elevation permits only, 2000+ ---
keep if flood_elev_final == 1 //strictest
gen permit_year = year(dofc(PERMIT_DATE))
keep if permit_year >= 2000 & !missing(permit_year)

di "Elevation permits 2000+: " _N   // 3466 obs

* --- 1b. County FIPS merge key ---
gen str5 fips_county = trim(SITUSSTATECOUNTYFIPS)
replace fips_county = substr(fips_county,1,2) + "0" + substr(fips_county,3,.) ///
    if length(fips_county) == 4
assert length(fips_county) == 5 if !missing(fips_county)

* --- 1c. ZIP merge key ---
gen str5 zip_code = substr(trim(ZIPCODE), 1, 5)
replace zip_code = "0" + zip_code if length(zip_code) == 4

* --- 1d. Property and job value ---
replace prop_value = TAXASSESSEDVALUETOTAL if missing(prop_value)
br if mi(prop_value) //all 3466 obs have property value


gen log_prop_value = log(prop_value)  if prop_value > 0
gen log_job_value  = log(JOB_VALUE)   if JOB_VALUE  > 0
gen val_cost_ratio     = prop_value /JOB_VALUE  if prop_value > 0 & JOB_VALUE > 0

label var permit_year    "Year permit filed"
label var fips_county    "5-digit county FIPS"
label var zip_code       "5-digit ZIP code"
label var prop_value     "Property value ($, ATTOM market or assessed)"
label var log_prop_value "Log property value"
label var log_job_value  "Log elevation job value"
label var val_cost_ratio     "Property value/ elevation job cost"

gen byte sample_resid  = (RESIDENTIAL == 1)
label var sample_resid  "Residential property (ATTOM)"
tab sample_resid
drop if sample_resid == 0 //only want residential properties
tab OWNERTYPEDESCRIPTION1
//want only individual owned properties
drop if OWNERTYPEDESCRIPTION1 != "INDIVIDUAL"
//keep key variables only
keep log* fips_county zip_code prop_value val_cost_ratio permit_year desc_l TAXMARKETVALUEYEAR PERMIT_NUMBER	 TAXASSESSEDVALUEIMPROVEMENTS addr_clean zip_clean CITY COUNTY JOB_VALUE FILE_DATE FINAL_DATE FOUNDATION* PERMIT_DATE YEARBUILT* 
summarize prop_value
//3141 obs
********************************************************************************
* 2. ClimateRisk (ZIP cross-section → m:1 zip_code)
********************************************************************************
preserve
    use "`root'/disasters/climaterisk.dta", clear
    keep if state == "Texas"
    tostring zip, gen(zip_code) format("%05.0f")
	keep state zip zip_code flood_fluvial_rating flood_pluvial_rating
    drop zip state
    tempfile cr_tx
    save `cr_tx'
restore

merge m:1 zip_code using `cr_tx', keep(master match) nogen

label var flood_fluvial_rating   "ClimateRisk: riverine flood rating (ZIP)"
label var flood_pluvial_rating   "ClimateRisk: pluvial flood rating (ZIP)"

********************************************************************************
* 3. NRI (county cross-section → m:1 fips_county)
*    Also provides county_name for all observations
********************************************************************************
preserve
    use "`root'/fema/nri_clean.dta", clear
    keep fips_county county_name state_name population building_value  ///
          nri_rating ifl_risk_rating svi_rating cfl_risk_score cfl_risk_rating ifl_risk_score
    tempfile nri
    save `nri'
restore

merge m:1 fips_county using `nri', keep(master match) nogen

label var county_name    "County name"
label var population     "County population (NRI)"
label var building_value "County total building replacement value ($, NRI)"
label var nri_rating     "NRI composite risk rating"
label var ifl_risk_rating "Inland flood risk rating"
label var ifl_risk_rating "Inland flood risk score"
label var cfl_risk_score "Coastal flood risk score"
label var cfl_risk_rating "Coastal flood risk rating"
label var svi_rating     "Social vulnerability rating"

********************************************************************************
* 4. NFIP county×year (m:1 fips_county × permit_year)
********************************************************************************
preserve
    use "`root'/fema/nfip_tx_countyyear.dta", clear
    rename year             nfip_year
    rename n_claims_total   nfip_n_claims
    rename n_substantially_dmgd nfip_n_subst_dmgd
    rename total_damage     nfip_total_damage
    rename total_payout     nfip_total_payout
    rename avg_damage_ratio nfip_avg_dmg_ratio
    rename avg_water_depth  nfip_avg_water_depth
    keep fips_county nfip_year nfip_n_claims nfip_n_subst_dmgd  ///
         nfip_total_damage nfip_total_payout nfip_avg_dmg_ratio            ///
         nfip_avg_water_depth 
    tempfile nfip_cy
    save `nfip_cy'
restore

gen int nfip_year = permit_year
merge m:1 fips_county nfip_year using `nfip_cy', keep(master match) nogen
drop nfip_year

foreach v of varlist nfip_n_claims nfip_n_subst_dmgd  ///
    nfip_total_damage nfip_total_payout  {
    replace `v' = 0 if missing(`v')
}

label var nfip_n_claims       "NFIP residential claims in county-year"
label var nfip_n_subst_dmgd   "NFIP claims with damage >= 50% of value in county-year"
label var nfip_total_damage   "Total NFIP building damage in county-year ($)"
label var nfip_total_payout   "Total NFIP building payout in county-year ($)"
label var nfip_avg_dmg_ratio  "Mean damage/value ratio in county-year"
label var nfip_avg_water_depth "Mean flood water depth in county-year (ft)"

********************************************************************************
* 5. HMA county×year (m:1 fips_county × permit_year)
*    Indicator variables: fema_elev, fema_buyout (overall and by program)
********************************************************************************
preserve
    use "`root'/fema/hma_tx_countyyear.dta", clear

    gen byte elev_hmgp   = (n_elevation > 0) & (programarea == "HMGP")
    gen byte elev_fma    = (n_elevation > 0) & (programarea == "FMA")
    gen byte elev_any    = (n_elevation > 0)
    gen byte buyout_hmgp = (n_buyout > 0)    & (programarea == "HMGP")
    gen byte buyout_fma  = (n_buyout > 0)    & (programarea == "FMA")
    gen byte buyout_any  = (n_buyout > 0)

    gen fed_hmgp = total_fed_obligated if programarea == "HMGP"
    gen fed_fma  = total_fed_obligated if programarea == "FMA"
    replace fed_hmgp = 0 if missing(fed_hmgp)
    replace fed_fma  = 0 if missing(fed_fma)

    collapse ///
        (max)  fema_elev         = elev_any      ///
        (max)  fema_elev_hmgp    = elev_hmgp     ///
        (max)  fema_elev_fma     = elev_fma      ///
        (max)  fema_buyout       = buyout_any    ///
        (max)  fema_buyout_hmgp  = buyout_hmgp   ///
        (max)  fema_buyout_fma   = buyout_fma    ///
        (sum)  hma_fed_oblg      = total_fed_obligated ///
        (sum)  hma_fed_oblg_hmgp = fed_hmgp      ///
        (sum)  hma_fed_oblg_fma  = fed_fma       ///
        (sum)  hma_n_elev        = n_elevation   ///
        (sum)  hma_n_buyout      = n_buyout      ///
        (mean) hma_avg_bca       = avg_bca       ///
        , by(fips_county year)

    rename year hma_year
    tempfile hma_cy
    save `hma_cy'
restore

gen int hma_year = permit_year
merge m:1 fips_county hma_year using `hma_cy', keep(master match) nogen
drop hma_year

foreach v of varlist fema_elev fema_elev_hmgp fema_elev_fma ///
    fema_buyout fema_buyout_hmgp fema_buyout_fma             ///
    hma_fed_oblg hma_fed_oblg_hmgp hma_fed_oblg_fma         ///
    hma_n_elev hma_n_buyout {
    replace `v' = 0 if missing(`v')
}

label var fema_elev          "=1 if county-year has any FEMA elevation project"
label var fema_elev_hmgp     "=1 if county-year has FEMA elevation via HMGP"
label var fema_elev_fma      "=1 if county-year has FEMA elevation via FMA"
label var fema_buyout        "=1 if county-year has any FEMA buyout"
label var fema_buyout_hmgp   "=1 if county-year has FEMA buyout via HMGP"
label var fema_buyout_fma    "=1 if county-year has FEMA buyout via FMA"
label var hma_fed_oblg       "FEMA federal obligated in county-year, all programs ($)"
label var hma_fed_oblg_hmgp  "FEMA federal obligated via HMGP in county-year ($)"
label var hma_fed_oblg_fma   "FEMA federal obligated via FMA in county-year ($)"
label var hma_n_elev         "FEMA elevation projects in county-year"
label var hma_n_buyout       "FEMA buyout projects in county-year"
label var hma_avg_bca        "Mean BCA of HMA projects in county-year"

********************************************************************************
* 6. Derived variables
********************************************************************************
gen log_nfip_damage = log(nfip_total_damage + 1)
gen log_hma_fed     = log(hma_fed_oblg + 1)
gen hma_fed_per_cap = hma_fed_oblg / population if population > 0

label var log_nfip_damage "Log(NFIP building damage + 1) in county-year"
label var log_hma_fed     "Log(FEMA federal obligated + 1) in county-year"
label var hma_fed_per_cap "FEMA federal obligated per capita in county-year ($)"

********************************************************************************
* 7. Variable order and sort
*    County identifiers first, then permit info, then county-year controls
********************************************************************************
order fips_county county_name permit_year zip_code                     ///
      PERMIT_NUMBER PERMIT_DATE                                         
sort fips_county permit_year

********************************************************************************
* 8. Quick summary
********************************************************************************
di "================================================================"
di "PROPERTY PANEL SUMMARY"
di "================================================================"

di "Merge coverage:"
qui count if !missing(flood_fluvial_rating)
di "  ClimateRisk (ZIP):  " r(N) " / " _N " (" %4.1f (r(N)/_N*100) "%)"
qui count if !missing(ifl_risk_rating)
di "  NRI (county):       " r(N) " / " _N " (" %4.1f (r(N)/_N*100) "%)"
qui count if nfip_n_claims > 0
di "  NFIP active cy:     " r(N) " / " _N " (" %4.1f (r(N)/_N*100) "%)"
qui count if fema_elev == 1 | fema_buyout == 1
di "  HMA active cy:      " r(N) " / " _N " (" %4.1f (r(N)/_N*100) "%)"
di ""
qui count if missing(prop_value)
di "Missing prop_value (no ATTOM record): " r(N)
tabstat prop_value JOB_VALUE val_cost_ratio ifl_risk_score cfl_risk_score ///
        flood_fluvial_rating nfip_n_claims hma_fed_oblg, ///
        stat(n mean p25 p50 p75) col(stat)
		
		//go with inland flood risk rating; coastal flood risk rating seems to be incompelte
********************************************************************************
* 9. Save
********************************************************************************
save "`root'/property_panel.dta", replace
di "Saved: property_panel.dta  (" _N " obs)"

*==============================================================================
* merge_states.do
* Builds two analysis datasets from the pre-merged Builty+HMA+ATTOM DTA file:
*   1. Property-level panel  → data/{state_l}/{state_l}_property_panel.dta
*   2. County × year panel   → data/{state_l}/{state_l}_county_panel.dta
*
* Input: data/{state_l}/{state_l}_attom_permit.dta
*
* Adds to what is already in {state_l}_attom_permit.dta:
*   - ClimateRisk (ZIP cross-section): flood_fluvial_rating, flood_pluvial_rating
*   - NRI (county cross-section): risk scores, population, building value
*   - NFIP (county × year): claims, damage, payout
*
* Prerequisites (already run upstream):
*   merge_hma_onto_permits.py   → {state_l}_flood_elev_hma.parquet
*   merge_attom_onto_permits.py → {state_l}_attom_permit.dta
*   nfip_clean_state.do         → nfip_{state_l}_countyyear.dta
*==============================================================================

clear all
set more off

*------------------------------------------------------------------------------
* CONFIGURE: change this one line to run for a different state
*------------------------------------------------------------------------------
local state_l "tx"

*------------------------------------------------------------------------------
* Derived from state_l — do not edit below this line
*------------------------------------------------------------------------------
local state_name = cond("`state_l'"=="tx", "Texas",       ///
                   cond("`state_l'"=="va", "Virginia",    ///
                   cond("`state_l'"=="fl", "Florida",     ///
                   cond("`state_l'"=="nc", "North Carolina", ///
                   cond("`state_l'"=="la", "Louisiana",   ///
                   cond("`state_l'"=="sc", "South Carolina", ///
                   cond("`state_l'"=="ga", "Georgia", "")))))))

local root  "/Users/anna/Desktop/Research/climate-investments/data"
local sdir  "`root'/`state_l'"

di "================================================================"
di "State: `state_l'  (`state_name')"
di "================================================================"

*==============================================================================
* 1. Load pre-merged Builty + HMA + ATTOM dataset
*    Unit: flood elevation permit (residential, 2000+, flood_elev_final == 1)
*==============================================================================
use "`sdir'/attom_permit.dta", clear
di "Loaded `state_l'_attom_permit.dta: " _N " permits"

* Rename merge keys to match FEMA data conventions
capture rename county_fips fips_county
capture rename zip_clean   zip_code

*==============================================================================
* 2. ClimateRisk (ZIP cross-section → m:1 zip_code)
*==============================================================================
preserve
    use "`root'/disasters/climaterisk.dta", clear
    keep if state == "`state_name'"
    tostring zip, gen(zip_code) format("%05.0f")
    keep zip_code flood_fluvial_rating flood_pluvial_rating
    tempfile cr
    save `cr'
restore

merge m:1 zip_code using `cr', keep(master match) nogen

qui count if !missing(flood_fluvial_rating)
di "ClimateRisk matched: " r(N) " / " _N " permits"

label var flood_fluvial_rating  "ClimateRisk: riverine flood rating (ZIP)"
label var flood_pluvial_rating  "ClimateRisk: pluvial flood rating (ZIP)"

*==============================================================================
* 3. NRI (county cross-section → m:1 fips_county)
*    Also provides county_name for all observations
*==============================================================================
preserve
    use "`root'/fema/nri_clean.dta", clear
    keep fips_county county_name population building_value  ///
         nri_rating nri_score                               ///
         ifl_risk_rating ifl_risk_score                     ///
         cfl_risk_rating cfl_risk_score                     ///
         svi_rating svi_score
    tempfile nri
    save `nri'
restore

merge m:1 fips_county using `nri', keep(master match) nogen

qui count if !missing(county_name)
di "NRI matched: " r(N) " / " _N " permits"

label var county_name    "County name"
label var population     "County population (NRI)"
label var building_value "County total building replacement value ($, NRI)"
label var nri_rating     "NRI composite risk rating"
label var ifl_risk_rating "Inland flood risk rating"
label var ifl_risk_score  "Inland flood risk score"
label var cfl_risk_rating "Coastal flood risk rating"
label var cfl_risk_score  "Coastal flood risk score"
label var svi_rating     "Social vulnerability rating"
label var svi_score      "Social vulnerability score"

*==============================================================================
* 4. NFIP (county × year → m:1 fips_county × permit_year)
*==============================================================================
local nfip_file "`root'/fema/nfip_`state_l'_countyyear.dta"
capture confirm file "`nfip_file'"
if _rc == 0 {
    preserve
        use "`nfip_file'", clear
        keep fips_county year                       ///
             n_claims_total n_substantially_dmgd    ///
             total_damage total_payout              ///
             avg_damage_ratio avg_water_depth

        rename year              nfip_year
        rename n_claims_total    nfip_n_claims
        rename n_substantially_dmgd nfip_n_subst_dmgd
        rename total_damage      nfip_total_damage
        rename total_payout      nfip_total_payout
        rename avg_damage_ratio  nfip_avg_dmg_ratio
        rename avg_water_depth   nfip_avg_water_depth

        tempfile nfip_cy
        save `nfip_cy'
    restore

    gen int nfip_year = permit_year
    merge m:1 fips_county nfip_year using `nfip_cy', keep(master match) nogen
    drop nfip_year

    foreach v of varlist nfip_n_claims nfip_n_subst_dmgd nfip_total_damage nfip_total_payout {
        replace `v' = 0 if missing(`v')
    }

    qui count if nfip_n_claims > 0
    di "NFIP active county-years: " r(N) " / " _N " permits"

    label var nfip_n_claims       "NFIP residential claims in county-year"
    label var nfip_n_subst_dmgd   "NFIP claims: substantial damage (>=50%) in county-year"
    label var nfip_total_damage   "Total NFIP building damage in county-year ($)"
    label var nfip_total_payout   "Total NFIP building payout in county-year ($)"
    label var nfip_avg_dmg_ratio  "Mean NFIP damage/value ratio in county-year"
    label var nfip_avg_water_depth "Mean flood water depth in county-year (ft)"
}
else {
    di "WARNING: `nfip_file' not found — NFIP variables set to missing"
    foreach v in nfip_n_claims nfip_n_subst_dmgd nfip_total_damage ///
                 nfip_total_payout nfip_avg_dmg_ratio nfip_avg_water_depth {
        gen long `v' = .
    }
}

*==============================================================================
* 5. Derived variables
*==============================================================================
gen log_nfip_damage  = log(nfip_total_damage + 1)
gen log_nfip_payout  = log(nfip_total_payout + 1)
gen log_hma_fed      = log(hma_fed_obligated + 1)
gen hma_fed_per_cap  = hma_fed_obligated / population if population > 0
gen claims_per_1000  = (nfip_n_claims / population) * 1000 if population > 0
gen pct_subst_dmgd   = (nfip_n_subst_dmgd / nfip_n_claims) * 100 ///
                         if nfip_n_claims > 0
replace pct_subst_dmgd = 0 if nfip_n_claims == 0

label var log_nfip_damage  "Log(NFIP building damage + 1)"
label var log_nfip_payout  "Log(NFIP building payout + 1)"
label var log_hma_fed      "Log(HMA federal obligated + 1)"
label var hma_fed_per_cap  "HMA federal obligated per capita ($)"
label var claims_per_1000  "NFIP claims per 1,000 residents"
label var pct_subst_dmgd   "% of NFIP claims with substantial damage"

*==============================================================================
* 6. Variable order, sort, and labels for pre-existing columns
*==============================================================================
label var prop_value         "Property value ($, ATTOM market or assessed)"
label var log_prop_value     "Log property value"
label var log_job_value      "Log elevation job value"
label var val_cost_ratio     "Property value / elevation job cost"
label var permit_year        "Year permit filed"
label var fips_county        "5-digit county FIPS"
label var zip_code           "5-digit ZIP code"

label var hma_fema_elev         "=1 if any FEMA elevation project this county-year"
label var hma_fema_elev_hmgp    "=1 if FEMA elevation via HMGP this county-year"
label var hma_fema_elev_fma     "=1 if FEMA elevation via FMA this county-year"
label var hma_fema_buyout       "=1 if any FEMA buyout this county-year"
label var hma_fema_buyout_hmgp  "=1 if FEMA buyout via HMGP this county-year"
label var hma_fema_buyout_fma   "=1 if FEMA buyout via FMA this county-year"
label var hma_fed_obligated     "HMA federal dollars obligated this county-year"
label var hma_n_elev_total      "FEMA elevation projects this county-year"
label var hma_n_buyout_total    "FEMA buyout projects this county-year"
label var hma_avg_bca           "Mean HMA benefit-cost ratio this county-year"

order fips_county county_name permit_year zip_code              ///
      PERMIT_NUMBER PERMIT_DATE addr_clean                       ///
      prop_value log_prop_value TAXMARKETVALUETOTAL              ///
      TAXASSESSEDVALUETOTAL YEARBUILT                            ///
      cnty_yr_propval_median cnty_yr_propval_mean                ///
      propval_rel_cnty_yr                                        ///
      JOB_VALUE log_job_value val_cost_ratio                     ///
      hma_fema_elev hma_fema_elev_hmgp hma_fema_elev_fma        ///
      hma_fema_buyout hma_fema_buyout_hmgp hma_fema_buyout_fma  ///
      hma_fed_obligated log_hma_fed hma_fed_per_cap              ///
      hma_n_elev_total hma_n_buyout_total hma_avg_bca            ///
      nfip_n_claims nfip_n_subst_dmgd pct_subst_dmgd             ///
      nfip_total_damage nfip_total_payout                        ///
      log_nfip_damage log_nfip_payout                            ///
      claims_per_1000 nfip_avg_dmg_ratio nfip_avg_water_depth    ///
      flood_fluvial_rating flood_pluvial_rating                  ///
      ifl_risk_score ifl_risk_rating cfl_risk_score cfl_risk_rating ///
      nri_rating svi_score svi_rating                            ///
      population building_value

sort fips_county permit_year

*==============================================================================
* 7. Summary and save: property-level panel
*==============================================================================
di ""
di "================================================================"
di "PROPERTY-LEVEL PANEL SUMMARY (`state_l')"
di "================================================================"
di "Total permits: " _N
qui count if !missing(prop_value)
di "  With ATTOM prop_value:       " r(N)
qui count if !missing(flood_fluvial_rating)
di "  With ClimateRisk (ZIP):      " r(N)
qui count if !missing(ifl_risk_score)
di "  With NRI (county):           " r(N)
qui count if nfip_n_claims > 0 & !missing(nfip_n_claims)
di "  In NFIP-active county-years: " r(N)
qui count if hma_fema_elev == 1
di "  In HMA-elevation county-yrs: " r(N)

save "`sdir'/`state_l'_property_panel.dta", replace
di "Saved: `state_l'_property_panel.dta  (" _N " obs)"

*==============================================================================
* 8. Collapse to county × year panel
*==============================================================================
preserve
    gen byte one = 1

    * ClimateRisk is ZIP-level — average across permits within county-year
    * NRI and NFIP are already county(-year) — take firstnm
    * HMA indicators are already county-year — take max
    collapse                                                       ///
        (sum)     n_permits          = one                        ///
        (sum)     total_job_value    = JOB_VALUE                  ///
        (mean)    avg_job_value      = JOB_VALUE                  ///
        (mean)    avg_prop_value     = prop_value                  ///
        (mean)    avg_log_prop_value = log_prop_value              ///
        (mean)    avg_log_job_value  = log_job_value               ///
        (mean)    avg_val_cost_ratio = val_cost_ratio              ///
        (max)     hma_fema_elev      = hma_fema_elev               ///
        (max)     hma_fema_elev_hmgp = hma_fema_elev_hmgp          ///
        (max)     hma_fema_elev_fma  = hma_fema_elev_fma           ///
        (max)     hma_fema_buyout    = hma_fema_buyout              ///
        (max)     hma_fema_buyout_hmgp = hma_fema_buyout_hmgp      ///
        (max)     hma_fema_buyout_fma  = hma_fema_buyout_fma       ///
        (firstnm) hma_n_elev_total   = hma_n_elev_total            ///
        (firstnm) hma_n_buyout_total = hma_n_buyout_total          ///
        (firstnm) hma_fed_obligated  = hma_fed_obligated           ///
        (firstnm) hma_avg_bca        = hma_avg_bca                 ///
        (firstnm) log_hma_fed        = log_hma_fed                 ///
        (firstnm) hma_fed_per_cap    = hma_fed_per_cap             ///
        (firstnm) nfip_n_claims      = nfip_n_claims               ///
        (firstnm) nfip_n_subst_dmgd  = nfip_n_subst_dmgd           ///
        (firstnm) nfip_total_damage  = nfip_total_damage            ///
        (firstnm) nfip_total_payout  = nfip_total_payout            ///
        (firstnm) nfip_avg_dmg_ratio = nfip_avg_dmg_ratio           ///
        (firstnm) nfip_avg_water_depth = nfip_avg_water_depth       ///
        (firstnm) log_nfip_damage    = log_nfip_damage              ///
        (firstnm) log_nfip_payout    = log_nfip_payout              ///
        (firstnm) claims_per_1000    = claims_per_1000              ///
        (firstnm) pct_subst_dmgd     = pct_subst_dmgd               ///
        (mean)    flood_fluvial_rating = flood_fluvial_rating        ///
        (mean)    flood_pluvial_rating = flood_pluvial_rating        ///
        (firstnm) county_name        = county_name                  ///
        (firstnm) population         = population                   ///
        (firstnm) building_value     = building_value               ///
        (firstnm) nri_rating         = nri_rating                   ///
        (firstnm) ifl_risk_score     = ifl_risk_score               ///
        (firstnm) ifl_risk_rating    = ifl_risk_rating              ///
        (firstnm) cfl_risk_score     = cfl_risk_score               ///
        (firstnm) cfl_risk_rating    = cfl_risk_rating              ///
        (firstnm) svi_score          = svi_score                    ///
        (firstnm) svi_rating         = svi_rating                   ///
        , by(fips_county permit_year)

    rename permit_year year

    order fips_county county_name year                              ///
          n_permits total_job_value avg_job_value                   ///
          avg_prop_value avg_log_prop_value avg_log_job_value        ///
          avg_val_cost_ratio                                         ///
          hma_fema_elev hma_fema_elev_hmgp hma_fema_elev_fma        ///
          hma_fema_buyout hma_fema_buyout_hmgp hma_fema_buyout_fma  ///
          hma_fed_obligated log_hma_fed hma_fed_per_cap              ///
          hma_n_elev_total hma_n_buyout_total hma_avg_bca            ///
          nfip_n_claims nfip_n_subst_dmgd pct_subst_dmgd             ///
          nfip_total_damage nfip_total_payout                        ///
          log_nfip_damage log_nfip_payout                            ///
          claims_per_1000 nfip_avg_dmg_ratio nfip_avg_water_depth    ///
          flood_fluvial_rating flood_pluvial_rating                  ///
          ifl_risk_score ifl_risk_rating cfl_risk_score cfl_risk_rating ///
          nri_rating svi_score svi_rating                            ///
          population building_value

    sort fips_county year

    di ""
    di "================================================================"
    di "COUNTY × YEAR PANEL SUMMARY (`state_l')"
    di "================================================================"
    di "Total county-year cells: " _N
    qui count if hma_fema_elev == 1
    di "  With FEMA elevation:   " r(N)
    qui count if nfip_n_claims > 0
    di "  With NFIP claims:      " r(N)

    save "`sdir'/`state_l'_county_panel.dta", replace
    di "Saved: `state_l'_county_panel.dta  (" _N " obs)"
restore

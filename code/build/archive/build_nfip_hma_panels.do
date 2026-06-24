/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-06-05

Description: Builds the NFIP/HMA property- and county-level panels.
    Property level: base = build/{state}_attom_builty.dta; NFIP matched with
    tiered Wagner-style policy-cell keys (ZIP x construction-year x policy-year;
    ZIP x construction-decade x policy-year; ZIP x policy-year; county x
    construction-year x policy-year; county x policy-year); HMA merged at
    county x permit-year.
    County level: base = union of HMA, NFIP, and all Builty elevation
    county-years -- intentionally NOT restricted to Builty-ATTOM matches.

Outputs: analysis/{state}_property_nfip_hma.dta, analysis/{state}_county_nfip_hma.dta
******************************************************************************/

version 18

args data states

capture mkdir "`data'/analysis"

********************************************************************************
* 1. HMA county-year file, for both property and county panels
********************************************************************************

use "`data'/clean/fma_elevation_grants.dta", clear

gen str2 state_abbr = ""
replace state_abbr = "TX" if statenumbercode == 48
replace state_abbr = "VA" if statenumbercode == 51
keep if inlist(state_abbr, "TX", "VA")

destring countycode programfy projectamount initialobligationamount ///
    federalshareobligated benefitcostratio netvaluebenefits ///
    numberofproperties numberoffinalproperties, replace force

drop if missing(countycode) | countycode <= 0 | missing(programfy)

gen str5 fips_county = string(statenumbercode, "%02.0f") + string(countycode, "%03.0f")
gen int year = programfy

gen strL projecttype_l = lower(projecttype)
gen str20 programarea_u = upper(programarea)

gen byte hma_is_elev   = regexm(projecttype_l, "(^|[^0-9])202\.") | strpos(projecttype_l, "elevat") > 0
gen byte hma_is_buyout = regexm(projecttype_l, "(^|[^0-9])20[01]\.") | strpos(projecttype_l, "acquisition") > 0 | strpos(projecttype_l, "relocation") > 0
gen byte hma_is_hmgp = programarea_u == "HMGP"
gen byte hma_is_fma  = programarea_u == "FMA"
gen byte hma_is_bric = programarea_u == "BRIC"
gen byte hma_is_pdm  = programarea_u == "PDM"
gen byte hma_project_row = 1

collapse ///
    (sum) hma_n_projects_total = hma_project_row ///
          hma_n_elev_total     = hma_is_elev ///
          hma_n_buyout_total   = hma_is_buyout ///
          hma_n_hmgp           = hma_is_hmgp ///
          hma_n_fma            = hma_is_fma ///
          hma_n_bric           = hma_is_bric ///
          hma_n_pdm            = hma_is_pdm ///
          hma_project_amount   = projectamount ///
          hma_initial_obligation = initialobligationamount ///
          hma_fed_obligated    = federalshareobligated ///
          hma_net_benefits     = netvaluebenefits ///
          hma_n_properties     = numberofproperties ///
          hma_n_final_properties = numberoffinalproperties ///
    (mean) hma_avg_bca         = benefitcostratio, ///
    by(state_abbr fips_county year)

gen byte hma_fema_any    = hma_n_projects_total > 0
gen byte hma_fema_elev   = hma_n_elev_total > 0
gen byte hma_fema_buyout = hma_n_buyout_total > 0
gen double hma_log_fed_obligated = log(hma_fed_obligated + 1)

tempfile hma_cy
save `hma_cy', replace

********************************************************************************
* 2. Build state property panels and county panels
********************************************************************************

foreach st of local states {

    local st = strlower("`st'")
    local ST = strupper("`st'")
    local sfips = cond("`ST'" == "TX", "48", "51")

    di as text "============================================================"
    di as text "State `ST'"
    di as text "============================================================"

    ***************************************************************************
    * 2A. NFIP base and tiered policy-cell summaries
    ***************************************************************************

    use "`data'/clean/nfip_policies_`st'.dta", clear

    * Wagner-style residential homeowner sample.
    keep if single_family_policy == 1 & primary_residence == 1

    gen str2 state_abbr = "`ST'"
    gen int year = policy_effective_year

    capture confirm string variable zip5
    if _rc {
        tostring zip5, replace format("%05.0f") force
    }
    gen str5 zip_key = substr(strtrim(zip5), 1, 5)
    replace zip_key = "" if zip_key == "." | zip_key == "00000"

    capture confirm string variable fips_county
    if _rc {
        tostring fips_county, replace format("%05.0f") force
    }
    replace fips_county = substr("00000" + strtrim(fips_county), length("00000" + strtrim(fips_county)) - 4, 5) ///
        if strtrim(fips_county) != "" & strtrim(fips_county) != "."

    gen int construction_year = original_construction_year
    replace construction_year = . if construction_year < 1700 | construction_year > 2035
    gen int construction_decade = floor(construction_year / 10) * 10

    foreach v in policy_count total_policy_premium total_building_coverage total_contents_coverage ///
        is_elevated post_firm {
        capture confirm string variable `v'
        if !_rc destring `v', replace ignore(", $") force
    }

    capture confirm variable has_building_policy
    if _rc gen byte has_building_policy = total_building_coverage > 0 if !missing(total_building_coverage)
    capture confirm variable has_contents_policy
    if _rc gen byte has_contents_policy = total_contents_coverage > 0 if !missing(total_contents_coverage)

    gen double nfip_total_coverage = total_building_coverage + total_contents_coverage
    gen double nfip_premium_per_1000 = total_policy_premium / (nfip_total_coverage / 1000) if nfip_total_coverage > 0
    gen byte nfip_high_risk_zone = regexm(upper(rated_flood_zone), "^(A|V)") | regexm(upper(flood_zone_current), "^(A|V)")
    gen byte nfip_row = 1

    tempfile nfip_base cell1 cell2 cell3 cell4 cell5 cell4p cell5p nfip_county
    save `nfip_base', replace

    preserve
        collapse ///
            (sum)   nfip_policy_rows = nfip_row ///
            (sum)   nfip_policy_count = policy_count ///
            (mean)  nfip_avg_premium = total_policy_premium ///
                    nfip_avg_building_coverage = total_building_coverage ///
                    nfip_avg_contents_coverage = total_contents_coverage ///
                    nfip_avg_total_coverage = nfip_total_coverage ///
                    nfip_avg_premium_per_1000 = nfip_premium_per_1000 ///
                    nfip_share_building_coverage = has_building_policy ///
                    nfip_share_contents_coverage = has_contents_policy ///
                    nfip_share_elevated = is_elevated ///
                    nfip_share_post_firm = post_firm ///
                    nfip_share_high_risk_zone = nfip_high_risk_zone, ///
            by(zip_key construction_year year)
        save `cell1', replace
    restore

    preserve
        collapse ///
            (sum)   nfip_policy_rows = nfip_row ///
            (sum)   nfip_policy_count = policy_count ///
            (mean)  nfip_avg_premium = total_policy_premium ///
                    nfip_avg_building_coverage = total_building_coverage ///
                    nfip_avg_contents_coverage = total_contents_coverage ///
                    nfip_avg_total_coverage = nfip_total_coverage ///
                    nfip_avg_premium_per_1000 = nfip_premium_per_1000 ///
                    nfip_share_building_coverage = has_building_policy ///
                    nfip_share_contents_coverage = has_contents_policy ///
                    nfip_share_elevated = is_elevated ///
                    nfip_share_post_firm = post_firm ///
                    nfip_share_high_risk_zone = nfip_high_risk_zone, ///
            by(zip_key construction_decade year)
        save `cell2', replace
    restore

    preserve
        collapse ///
            (sum)   nfip_policy_rows = nfip_row ///
            (sum)   nfip_policy_count = policy_count ///
            (mean)  nfip_avg_premium = total_policy_premium ///
                    nfip_avg_building_coverage = total_building_coverage ///
                    nfip_avg_contents_coverage = total_contents_coverage ///
                    nfip_avg_total_coverage = nfip_total_coverage ///
                    nfip_avg_premium_per_1000 = nfip_premium_per_1000 ///
                    nfip_share_building_coverage = has_building_policy ///
                    nfip_share_contents_coverage = has_contents_policy ///
                    nfip_share_elevated = is_elevated ///
                    nfip_share_post_firm = post_firm ///
                    nfip_share_high_risk_zone = nfip_high_risk_zone, ///
            by(zip_key year)
        save `cell3', replace
    restore

    preserve
        collapse ///
            (sum)   nfip_policy_rows = nfip_row ///
            (sum)   nfip_policy_count = policy_count ///
            (mean)  nfip_avg_premium = total_policy_premium ///
                    nfip_avg_building_coverage = total_building_coverage ///
                    nfip_avg_contents_coverage = total_contents_coverage ///
                    nfip_avg_total_coverage = nfip_total_coverage ///
                    nfip_avg_premium_per_1000 = nfip_premium_per_1000 ///
                    nfip_share_building_coverage = has_building_policy ///
                    nfip_share_contents_coverage = has_contents_policy ///
                    nfip_share_elevated = is_elevated ///
                    nfip_share_post_firm = post_firm ///
                    nfip_share_high_risk_zone = nfip_high_risk_zone, ///
            by(fips_county construction_year year)
        save `cell4', replace
        rename fips_county county_fips
        save `cell4p', replace
    restore

    preserve
        collapse ///
            (sum)   nfip_policy_rows = nfip_row ///
            (sum)   nfip_policy_count = policy_count ///
            (mean)  nfip_avg_premium = total_policy_premium ///
                    nfip_avg_building_coverage = total_building_coverage ///
                    nfip_avg_contents_coverage = total_contents_coverage ///
                    nfip_avg_total_coverage = nfip_total_coverage ///
                    nfip_avg_premium_per_1000 = nfip_premium_per_1000 ///
                    nfip_share_building_coverage = has_building_policy ///
                    nfip_share_contents_coverage = has_contents_policy ///
                    nfip_share_elevated = is_elevated ///
                    nfip_share_post_firm = post_firm ///
                    nfip_share_high_risk_zone = nfip_high_risk_zone, ///
            by(fips_county year)
        save `cell5', replace
        save `nfip_county', replace
        rename fips_county county_fips
        save `cell5p', replace
    restore

    ***************************************************************************
    * 2B. Property panel: Builty+ATTOM base, then tiered NFIP and HMA
    ***************************************************************************

    use "`data'/build/`st'_attom_builty.dta", clear
    gen long _pid = _n
    gen str2 state_abbr = "`ST'"

    capture confirm string variable zip_clean
    if _rc tostring zip_clean, replace format("%05.0f") force
    gen str5 zip_key = substr(strtrim(zip_clean), 1, 5)
    replace zip_key = "" if zip_key == "." | zip_key == "00000"

    capture confirm string variable county_fips
    if _rc tostring county_fips, replace format("%05.0f") force
    replace county_fips = substr("00000" + strtrim(county_fips), length("00000" + strtrim(county_fips)) - 4, 5) ///
        if strtrim(county_fips) != "" & strtrim(county_fips) != "."

    gen int year = permit_year
    gen int construction_year = YEARBUILT
    replace construction_year = YEARBUILTEFFECTIVE if missing(construction_year) & !missing(YEARBUILTEFFECTIVE)
    replace construction_year = . if construction_year < 1700 | construction_year > 2035
    gen int construction_decade = floor(construction_year / 10) * 10

    tempfile permits_base allmatches best_nfip property_nfip
    save `permits_base', replace

    clear
    save `allmatches', emptyok replace

    use `permits_base', clear
    keep _pid zip_key construction_year year
    merge m:1 zip_key construction_year year using `cell1', keep(match) nogen
    gen byte nfip_tier_rank = 1
    gen str40 nfip_match_tier = "zip_yearbuilt_policyyear"
    append using `allmatches'
    save `allmatches', replace

    use `permits_base', clear
    keep _pid zip_key construction_decade year
    merge m:1 zip_key construction_decade year using `cell2', keep(match) nogen
    gen byte nfip_tier_rank = 2
    gen str40 nfip_match_tier = "zip_decade_policyyear"
    append using `allmatches'
    save `allmatches', replace

    use `permits_base', clear
    keep _pid zip_key year
    merge m:1 zip_key year using `cell3', keep(match) nogen
    gen byte nfip_tier_rank = 3
    gen str40 nfip_match_tier = "zip_policyyear"
    append using `allmatches'
    save `allmatches', replace

    use `permits_base', clear
    keep _pid county_fips construction_year year
    merge m:1 county_fips construction_year year using `cell4p', keep(match) nogen
    gen byte nfip_tier_rank = 4
    gen str40 nfip_match_tier = "county_yearbuilt_policyyear"
    append using `allmatches'
    save `allmatches', replace

    use `permits_base', clear
    keep _pid county_fips year
    merge m:1 county_fips year using `cell5p', keep(match) nogen
    gen byte nfip_tier_rank = 5
    gen str40 nfip_match_tier = "county_policyyear"
    append using `allmatches'
    save `allmatches', replace

    use `allmatches', clear
    sort _pid nfip_tier_rank
    by _pid: keep if _n == 1
    keep _pid nfip_*
    save `best_nfip', replace

    use `permits_base', clear
    merge 1:1 _pid using `best_nfip', keep(master match) nogen
    replace nfip_match_tier = "unmatched" if missing(nfip_match_tier)

    tempfile hma_prop
    preserve
        use `hma_cy', clear
        rename fips_county county_fips
        save `hma_prop', replace
    restore

    merge m:1 state_abbr county_fips year using `hma_prop', keep(master match) nogen
    foreach v of varlist hma_* {
        capture confirm numeric variable `v'
        if !_rc replace `v' = 0 if missing(`v') & "`v'" != "hma_avg_bca"
    }

    drop _pid
    save "`data'/analysis/`st'_property_nfip_hma.dta", replace
    tab nfip_match_tier

    ***************************************************************************
    * 2C. County panel: union of HMA, NFIP county-years, and all Builty elevation
    ***************************************************************************

    use `hma_cy', clear
    keep if state_abbr == "`ST'"
    keep state_abbr fips_county year
    tempfile county_base
    save `county_base', replace

    use `nfip_county', clear
    gen str2 state_abbr = "`ST'"
    keep state_abbr fips_county year
    append using `county_base'
    duplicates drop
    save `county_base', replace

    use "`data'/clean/all_builty_elevations.dta", clear
    keep if upper(STATE) == "`ST'"
    capture confirm variable event_year
    if !_rc {
        gen int year = event_year
    }
    else {
        capture confirm variable permit_year
        if !_rc {
            gen int year = permit_year
        }
        else {
            capture confirm variable date_issued_d
            if !_rc {
                gen int year = year(date_issued_d)
            }
            else {
                capture confirm variable DATE_ISSUED
                if !_rc {
                    gen int year = year(date(DATE_ISSUED, "YMD"))
                    replace year = year(date(DATE_SUBMITTED, "YMD")) if missing(year) & !missing(DATE_SUBMITTED)
                    replace year = year(date(DATE_FINALED, "YMD")) if missing(year) & !missing(DATE_FINALED)
                }
                else {
                    di as error "Could not find event_year, permit_year, date_issued_d, or DATE_ISSUED in all_builty_elevations.dta"
                    exit 111
                }
            }
        }
    }
    capture confirm variable fips_county
    if !_rc {
        capture confirm string variable fips_county
        if _rc tostring fips_county, replace format("%05.0f") force
        replace fips_county = substr("00000" + strtrim(fips_county), length("00000" + strtrim(fips_county)) - 4, 5) ///
            if strtrim(fips_county) != "" & strtrim(fips_county) != "."
    }
    else {
        capture confirm string variable FIPS_COUNTY
        if _rc tostring FIPS_COUNTY, replace format("%03.0f") force
        gen str5 fips_county = "`sfips'" + substr("000" + strtrim(FIPS_COUNTY), length("000" + strtrim(FIPS_COUNTY)) - 2, 3)
    }
    gen str2 state_abbr = "`ST'"
    keep if !missing(year) & fips_county != ""
    gen byte builty_one = 1
    capture confirm variable flood_elev_final
    if _rc {
        gen byte flood_elev_final = 1
    }
    capture confirm variable flood_adaptation_context
    if _rc {
        gen byte flood_adaptation_context = 0
    }
    gen byte builty_highconf = flood_elev_final == 1 & flood_adaptation_context == 1
    capture confirm numeric variable PROJECT_VALUE
    if _rc destring PROJECT_VALUE, replace force
    collapse ///
        (sum) builty_elev_permits = builty_one ///
              builty_elev_highconf_permits = builty_highconf ///
              builty_total_job_value = PROJECT_VALUE ///
        (mean) builty_avg_job_value = PROJECT_VALUE, ///
        by(state_abbr fips_county year)
    tempfile builty_cy
    save `builty_cy', replace

    keep state_abbr fips_county year
    append using `county_base'
    duplicates drop
    save `county_base', replace

    use `county_base', clear
    merge 1:1 state_abbr fips_county year using `hma_cy', keep(master match) nogen
    merge 1:1 fips_county year using `nfip_county', keep(master match) nogen
    merge 1:1 state_abbr fips_county year using `builty_cy', keep(master match) nogen

    foreach v of varlist hma_* nfip_* builty_elev_permits builty_elev_highconf_permits builty_total_job_value {
        capture confirm numeric variable `v'
        if !_rc replace `v' = 0 if missing(`v') & "`v'" != "hma_avg_bca" & "`v'" != "builty_avg_job_value"
    }

    order state_abbr fips_county year
    sort fips_county year
    save "`data'/analysis/`st'_county_nfip_hma.dta", replace

    count if hma_fema_any == 1
    di as result "`ST' county HMA county-years retained: " r(N)
}

di as result "Done: property and county NFIP/HMA panels written to `data'/analysis"

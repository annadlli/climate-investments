/******************************************************************************
Authors: Vendela Norman, Anna Li
Date: 2026-09-03 
Edited: 2026-09-13

Description: Prepares the final analysis dataset: restricts the NFIP-FMA panel to
    county-years with Builty permit coverage, merges in the ATTOM/Builty property
    links and the ATTOM value for the policy year, and builds the harmonized
    elevation variables (NFIP flag flip or ICC claim, Builty retrofit permit,
    county HMA probability). Two files are saved: analysis.dta carries the
    parsimonious variable set; analysis_with_diagnostics.dta adds the match and
    coverage diagnostics (issue #23).

Revisions: Keep Builty cost and funding columns, attempt to harmonize elvation variable, and save diagnostics as separate
******************************************************************************/

args data states

* -----------------------------------------------------------------------------
* Section 1: Prepare ATTOM/Builty property links
* -----------------------------------------------------------------------------

* Append the state link files
clear
foreach st of local states {
    local stl = strlower("`st'")
    append using "`data'/build/nfip_attom_property/`stl'_nfip_attom_property.dta", ///
        keep(state property_id_state assigned_attomid builty_elevated builty_elevation_year ///
             builty_retrofit builty_project_value builty_funding_type ///
             match_tier_number builty_new_construction builty_geo_backfilled)
}
isid state property_id_state
tempfile links
save `links'

* -----------------------------------------------------------------------------
* Section 2: Merge datasets
* -----------------------------------------------------------------------------

* Import NFIP-FMA panel
use "`data'/build/nfip_hma_panel.dta", clear

* Restrict to the states in master.do
gen byte in_sample = 0
foreach st of local states {
    replace in_sample = 1 if state == "`st'"
}
keep if in_sample
drop in_sample

* Merge Builty permit coverage
ren policy_year year
merge m:1 countycode year using "`data'/clean/builty_coverage_county.dta", keep(1 3) ///
    keepusing(year builty_per_100 builty_covered_strict) gen(builty_merge)
ren year policy_year

* Merge ATTOM/Builty property links
merge m:1 state property_id_state using `links', keep(1 3) nogen
gen attom_matched = assigned_attomid != ""

* Merge the ATTOM value for the policy year (2023)
preserve
    clear
    foreach st of local states {
        local stl = strlower("`st'")
        append using "`data'/build/attom_value_wide/`stl'_attom_value_wide.dta", keep(attomid value_*)
    }
    isid attomid
    rename attomid assigned_attomid
    tempfile values
    save `values'
restore
// 09-13: one year at a time loop to release memory pressure so runnable locally
gen attom_value_2023 = .
forvalues y = 2000/2026 {
    merge m:1 assigned_attomid using `values', keep(1 3) keepusing(value_`y') nogen
    replace attom_value_2023 = value_`y' if policy_year == `y'
    drop value_`y'
}
compress

* -----------------------------------------------------------------------------
* Section 3: Harmonized elevation status (09-12)
* -----------------------------------------------------------------------------
* Within NFIP, flag first year when elevation changes from 0 to 1
sort property_id policy_year
by property_id: gen _flip = elevated == 1 & elevated[_n-1] == 0 if _n > 1
by property_id: egen nfip_elevation_year = min(cond(_flip == 1, policy_year, .))
* Same idea, but for ICC claim: first year where ICC claim > 0
by property_id: egen _icc_year = min(cond(claim_icc > 0, policy_year, .))
*ICC year used if elevation year is missing only
replace nfip_elevation_year = _icc_year if mi(nfip_elevation_year)
gen elevated_nfip = !mi(nfip_elevation_year)

* Builty: retrofit permit on the assigned ATTOM property (missing if no ATTOM match)
gen elevated_builty = builty_retrofit == 1
// 09-14: a new build's declared value is its construction cost, not an elevation cost, so the cost is kept for retrofits only
replace builty_project_value = . if builty_retrofit != 1
// Claude change 09-15: values outside $1,000-$1,000,000 (2023 $) are not elevation costs.
// Below: paperwork lines (spot-elevation documents, an EV charger "above BFE"), 3.8% of
// retrofit values. Above: a pump-station upgrade ($7.3M), new elevated homes and raised
// slabs the screen let through, 0.9%. Kept here rather than in clean_builty.do so a change
// to the rule does not force the matching to rerun; the permit still counts as an elevation.
replace builty_project_value = . if builty_project_value < 1000 | builty_project_value > 1000000

* Harmonize all sources to create one elvation indicator
* Priority is Builty, elevation NFIP, then ICC claim
gen elevation = elevated_nfip | elevated_builty
gen elevation_year = builty_elevation_year if elevated_builty
replace elevation_year = nfip_elevation_year if mi(elevation_year)
*label source of elevation variable
gen elevation_source = elevated_nfip + 2 * elevated_builty
label define elevation_source_lbl 0 "None" 1 "NFIP only" 2 "Builty only" 3 "NFIP and Builty"
label values elevation_source elevation_source_lbl

* Funding: grant language from Builty permit descriptions: FEMA, HMGP, FMA, or Build Back Better 
gen elevation_funded = inlist(builty_funding_type, 1, 2, 3, 5) if elevated_builty

* Number of unique properties with NFIP per county
egen _tag = tag(property_id)
bysort countycode: egen n_nfip_county = total(_tag)
*percentage of grant-elevated properties per insured houses in a county
gen hma_p_fma  = min(fma_n_properties / n_nfip_county, 1)
gen hma_p_hmgp = min(hmgp_n_properties / n_nfip_county, 1)
drop _flip _icc_year _tag

* -----------------------------------------------------------------------------
* Section 4: Apply sample restrictions
* -----------------------------------------------------------------------------

* Restrict to county-years w/ builty coverage
// Note: This is a generous restriction that needs to be refined. Some localities
// within county don't report.
keep if builty_merge == 3

* Drop extraneous variables
drop builty_merge countycode assigned_attomid property_id_state

* Drop years w/ missing data
drop if policy_year > 2025

* -----------------------------------------------------------------------------
* Section 5: Label and save
* -----------------------------------------------------------------------------
* Label
label var attom_matched            "NFIP property has an assigned ATTOM property"
label var attom_value_2023         "ATTOM market value in the policy year (2023 $)"
label var builty_elevated          "Builty elevation permit on assigned ATTOM property"
label var builty_retrofit          "Builty permit elevates an existing structure"
label var builty_elevation_year    "Earliest Builty elevation-permit year"
label var builty_project_value     "Builty declared elevation project value (2023 $)"
label var builty_funding_type      "Funding source named in the Builty permit text"
label var builty_new_construction  "Builty permit is elevated new construction"
label var builty_geo_backfilled    "ATTOM coordinates/block group backfilled from Builty geocode"
label var match_tier_number        "NFIP-ATTOM match tier (1 = block group x zone x year ... 15)"
label var builty_per_100           "Builty permits per 100 ATTOM single-family properties, county-year"
label var builty_covered_strict    "County-year has >= 1 Builty permit per 100 SF properties"
label var nfip_elevation_year      "First year the NFIP flag flips to elevated, else first ICC payment"
label var elevated_nfip            "Elevation observed in NFIP (flag flip or ICC payment)"
label var elevated_builty          "Elevation observed in Builty (retrofit permit)"
label var elevation                "Elevated: NFIP flip or ICC, or Builty retrofit permit"
label var elevation_year           "Elevation year (Builty permit, else NFIP)"
label var elevation_source         "Source of the elevation"
label var elevation_funded         "Builty permit text names a grant (FEMA, HMGP, FMA, BBB)"
label var n_nfip_county            "NFIP-insured single-family properties in county"
label var hma_p_fma                "FMA-elevated properties per NFIP property in county"
label var hma_p_hmgp               "HMGP-elevated properties per NFIP property in county"
label define funding_type_lbl 0 "Unknown" 1 "FEMA" 2 "HMGP" 3 "FMA" 4 "SFHA" 5 "Build Back Better"
label values builty_funding_type funding_type_lbl

* Order and sort
order builty_elevated, after(elevated)
order elevation elevation_year elevation_source elevation_funded hma_p_fma hma_p_hmgp ///
    attom_matched attom_value_2023 builty_retrofit builty_elevation_year builty_project_value, ///
    after(cumulative_claims)
order nfip_elevation_year elevated_nfip elevated_builty builty_funding_type ///
    builty_new_construction builty_geo_backfilled match_tier_number builty_per_100 n_nfip_county, last
sort state property_id policy_year
compress

* Save the version with diagnostics
save "`data'/analysis/analysis_with_diagnostics.dta", replace

* Save the parsimonious analysis file
// 09-12: Drop more diagnostic variables
// Note: builty_covered_strict stays because es_prices_mitigation.do conditions on it
drop nfip_elevation_year elevated_nfip elevated_builty builty_funding_type ///
    builty_new_construction builty_geo_backfilled match_tier_number builty_per_100 n_nfip_county
sa "`data'/analysis/analysis.dta", replace

* Save extract that can be used for Claude
bysort property_id (policy_year): gen _draw = runiform() if _n == 1
bysort property_id (policy_year): replace _draw = _draw[1]
keep if _draw < 0.1
drop _draw
sa "`data'/analysis/extracts/500M_subsample.dta", replace

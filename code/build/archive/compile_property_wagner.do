/******************************************************************************
Authors: Anna Li
Date: 2026-08-04
Description: Merge state Wagner one-to-one ATTOM/Builty links onto the existing
    NFIP property-level analysis base produced by compile.do.
******************************************************************************/

version 18
args data states

tempfile links
local first = 1
foreach state of local states {
    local st = lower("`state'")
    capture confirm file "`data'/build/`st'_property_wagner_links.dta"
    if !_rc {
        if `first' {
            use "`data'/build/`st'_property_wagner_links.dta", clear
            local first = 0
        }
        else append using "`data'/build/`st'_property_wagner_links.dta"
    }
}
if `first' {
    di as error "No state property_wagner_links.dta files found"
    exit 601
}
capture confirm variable builty_data_available
if _rc gen builty_data_available = !inlist(state, "ME", "MS", "FL", "TX")
else replace builty_data_available = !inlist(state, "ME", "MS", "FL", "TX") ///
    if missing(builty_data_available)
replace builty_elevated_wagner = . if builty_data_available == 0
replace builty_n_permits = . if builty_data_available == 0
keep state property_id reference_year attomid wagner_tier year attom_value_asof ///
    market_value_total market_value_land market_value_improvements ///
    assessed_value_total assessed_value_improvements previous_assessed_value ///
    property_use_std area_building area_lot_acres bedrooms baths ///
    last_sale_price last_sale_date longitude latitude geocode_match ///
    builty_data_available builty_elevated_wagner builty_elevation_year builty_n_permits ///
    builty_attom_match_tier
isid state property_id
save `links'

use "`data'/analysis/analysis.dta", clear
merge 1:1 state property_id using `links', keep(master match) gen(wagner_merge)
label variable wagner_merge "NFIP property matched to one-to-one Wagner ATTOM link"
label variable builty_elevated_wagner "Builty elevation on assigned ATTOM property"
compress
save "`data'/analysis/analysis_wagner.dta", replace
di as result "Saved: `data'/analysis/analysis_wagner.dta"

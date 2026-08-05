/******************************************************************************
Authors: Vendela Norman
Date: 2026-08-05

Description: PRELIMINARY second-stage compile. Attaches ATTOM property values
    and Builty elevation permits onto analysis.dta from Anna's property-level
    Wagner links (build/temp wagner/, 2026-08-04 vintage). The links key on a
    per-dataset property_id that does not align with the current NFIP build,
    so they are collapsed to block-group x construction-year cells (the primary
    Wagner tier) and merged at that grain. Janky by design -- supersede once
    the link build keys on shared data values and is wired into the pipeline.

******************************************************************************/

args data states

* Append the per-state Wagner link files
// Note: ME and MS links are not yet built (2026-08-05); those states carry
// missing ATTOM/Builty fields in analysis2 rather than being dropped.
clear
tempfile links
save `links', emptyok
foreach state of local states {
    local st = lower("`state'")
    local f "`data'/build/temp wagner/`st'_property_wagner_links.dta"
    capture confirm file "`f'"
    if _rc {
        di as txt "compile2: no Wagner links for `state', skipped"
        continue
    }
    use bg_key construction_year market_value_total builty_elevated_wagner ///
        builty_elevation_year builty_n_permits using "`f'", clear
    append using `links'
    save `links', replace
}

* Collapse to block-group x construction-year cells
// Note: ATTOM logs some parcels with a zero market value; treat as missing so
// they do not drag the cell median.
drop if bg_key == "" | mi(construction_year)
replace construction_year = round(construction_year)
keep if inrange(construction_year, 1700, 2030)
replace market_value_total = . if market_value_total == 0
collapse (median) attom_value_bg = market_value_total ///
    (count) attom_n_bg = market_value_total ///
    (max) builty_elevated_bg = builty_elevated_wagner ///
    (min) builty_elevation_year_bg = builty_elevation_year ///
    (sum) builty_n_permits_bg = builty_n_permits, ///
    by(bg_key construction_year)
ren bg_key censusblockgroupfips
tempfile cells
save `cells'

* Attach to the analysis dataset
use "`data'/analysis/analysis.dta", clear
merge m:1 censusblockgroupfips construction_year using `cells', keep(1 3)
gen attom_linked = _merge == 3
drop _merge

* Label
label var attom_value_bg          "ATTOM median market value, bg x constr-yr cell (nominal)"
label var attom_n_bg              "ATTOM parcels with value in cell"
label var builty_elevated_bg      "Any Builty elevation permit in cell"
label var builty_elevation_year_bg "Earliest Builty elevation year in cell"
label var builty_n_permits_bg     "Builty permits in cell"
label var attom_linked            "In a linked Wagner bg x constr-yr cell"

* Order, sort, save
sort state zipcode censusblockgroupfips
compress
save "`data'/analysis/analysis2.dta", replace

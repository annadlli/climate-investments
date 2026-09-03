/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-08-07

Description: PRELIMINARY second-stage compile. Attaches ATTOM property values
    and Builty elevation permits onto analysis.dta from Anna's property-level
    Wagner links (build/, 2026-08-04 vintage). The links key on a
    per-dataset property_id that does not align with the current NFIP build,
    so they are collapsed to block-group x construction-year cells (the primary
    Wagner tier) and merged at that grain. This is a preliminary cell-level
    merge, not a parcel-level match. Supersede it once the link build keys on
    shared data values.

Notes / Sources:
    Inputs:
      `data'/analysis/analysis.dta
      `data'/build/{state}_property_wagner_links.dta
    Outputs:
      `data'/analysis/analysis2.dta
      `data'/analysis/analysis2_merge_diagnostics.dta

    Merge key: state x census block group x construction year. ME and MS do
    not yet have property-Wagner link files and remain in the analysis sample
    with missing ATTOM/Builty fields. FL and TX have ATTOM links but their
    current link files predate usable Builty inputs, so Builty fields are set
    to missing rather than treating placeholder zeros as confirmed negatives.

    ATTOM value: market_value_total is the preferred measure but is not
    universally populated -- it is zero for every row in CT, MA and RI, for all
    but one row in NH, and for 28-68% of rows in DE, LA, PA, VA and VT.
    assessed_value_total is populated in nearly all of those cases (rows with
    both zero are rare: 0 in RI, 3 in DE, 144 in PA, 265 in VA, 863 in LA), so
    the cell value coalesces market -> assessed. New England assessors publish
    an assessed value at (nominally) full market value; elsewhere assessment
    ratios vary by jurisdiction, so attom_value_assessed_bg carries the share
    of the cell valued off assessed rather than market value -- treat cells
    with a high share as measured on a different scale, not as comparable.

******************************************************************************/

args data states output_suffix links_dir

local output_stub "analysis2"
if trim("`output_suffix'") != "" {
    local output_stub "analysis2_`output_suffix'"
}
if trim("`links_dir'") == "" {
    local links_dir "`data'/build"
}

* Fail before constructing temporary files or overwriting outputs when the
* analysis base is unavailable.
capture confirm file "`data'/analysis/analysis.dta"
if _rc {
    di as error "compile2: required input not found: `data'/analysis/analysis.dta"
    di as error "Run prep_nfip_policies.do and compile.do before compile2.do."
    exit 601
}

* Append the per-state Wagner link files
// Note: ME and MS links are not yet built (2026-08-05); those states carry
// missing ATTOM/Builty fields in analysis2 rather than being dropped.
tempfile links cells
local first = 1
local states_found ""
local states_missing ""
foreach state of local states {
    local st = lower("`state'")
    local f "`links_dir'/`st'_property_wagner_links.dta"
    capture confirm file "`f'"
    if _rc {
        di as txt "compile2: no Wagner links for `state', skipped"
        local states_missing "`states_missing' `state'"
        continue
    }
    use state bg_key construction_year market_value_total assessed_value_total ///
        builty_elevated_wagner builty_elevation_year builty_n_permits ///
        using "`f'", clear
    replace state = upper(trim(state))
    assert state == upper("`state'")
    gen builty_data_available = !inlist(state, "FL", "TX")
    foreach variable in builty_elevated_wagner builty_elevation_year builty_n_permits {
        replace `variable' = . if inlist(state, "FL", "TX")
    }
    local states_found "`states_found' `state'"

    if `first' == 1 {
        save `links', replace
        local first = 0
    }
    else {
        append using `links'
        save `links', replace
    }
}

if `first' == 1 {
    di as error "compile2: no property-Wagner link files found"
    exit 601
}

* Collapse to block-group x construction-year cells
// Note: ATTOM logs zero rather than missing where it holds no value, for both
// market and assessed value; treat zeros as missing so they do not drag the
// cell median. attom_value coalesces market -> assessed (see header).
drop if bg_key == "" | mi(construction_year)
replace construction_year = round(construction_year)
keep if inrange(construction_year, 1700, 2030)
gen attom_value = .
gen attom_value_assessed = .
replace attom_value = market_value_total if market_value_total > 0 & !mi(market_value_total)
replace attom_value_assessed = 0 if !mi(attom_value)
replace attom_value = assessed_value_total if mi(attom_value) ///
    & assessed_value_total > 0 & !mi(assessed_value_total)
replace attom_value_assessed = 1 if mi(attom_value_assessed) & !mi(attom_value)
assert mi(attom_value) == mi(attom_value_assessed)
collapse (median) attom_value_bg = attom_value ///
    (count) attom_n_bg = attom_value ///
    (mean) attom_value_assessed_bg = attom_value_assessed ///
    (max) builty_elevated_bg = builty_elevated_wagner ///
    (max) builty_data_available = builty_data_available ///
    (min) builty_elevation_year_bg = builty_elevation_year ///
    (sum) builty_n_permits_bg = builty_n_permits, ///
    by(state bg_key construction_year)
replace builty_n_permits_bg = . if missing(builty_elevated_bg)
ren bg_key censusblockgroupfips
save `cells'

* Attach to the analysis dataset
use "`data'/analysis/analysis.dta", clear
isid state property_id
merge m:1 state censusblockgroupfips construction_year using `cells', keep(1 3)
gen attom_linked = _merge == 3
drop _merge

* Diagnostics by state; unmatched properties remain in the analysis universe.
// A linked cell need not carry a value, so n_attom_valued is reported
// separately -- it is what a value-using analysis actually has.
preserve
    gen attom_valued = !mi(attom_value_bg)
    collapse (count) n_properties=property_id (sum) n_attom_linked=attom_linked ///
        (sum) n_attom_valued=attom_valued ///
        (mean) share_value_assessed=attom_value_assessed_bg ///
        (sum) n_builty_elevated=builty_elevated_bg ///
        (max) builty_data_available=builty_data_available, by(state)
    replace n_builty_elevated = . if builty_data_available != 1
    gen attom_link_rate = n_attom_linked / n_properties
    gen attom_value_rate = n_attom_valued / n_properties
    gen builty_elevated_rate = n_builty_elevated / n_attom_linked ///
        if n_attom_linked > 0
    label var n_properties          "NFIP properties"
    label var n_attom_linked        "Properties linked to ATTOM cell"
    label var n_attom_valued        "Properties with a nonmissing ATTOM cell value"
    label var share_value_assessed  "Share of cell value from assessed, not market, value"
    label var n_builty_elevated     "Properties in Builty-elevation cells"
    label var attom_link_rate       "Share linked to ATTOM cell"
    label var attom_value_rate      "Share with an ATTOM cell value"
    label var builty_elevated_rate  "Share of ATTOM-linked properties in elevation cells"
    sort state
    compress
    save "`data'/analysis/`output_stub'_merge_diagnostics.dta", replace
restore

* Label
label var attom_value_bg          "ATTOM median property value, bg x constr-yr cell (nominal)"
label var attom_n_bg              "Linked properties with a value in cell"
label var attom_value_assessed_bg "Share of cell value from assessed, not market, value"
label var builty_elevated_bg      "Any Builty elevation permit in cell"
label var builty_elevation_year_bg "Earliest Builty elevation year in cell"
label var builty_n_permits_bg     "Builty permits in cell"
label var builty_data_available   "Usable Builty data in linked state"
label var attom_linked            "In a linked Wagner bg x constr-yr cell"

* Order, sort, save
order attom_linked attom_value_bg attom_n_bg attom_value_assessed_bg ///
    builty_elevated_bg builty_elevation_year_bg builty_n_permits_bg ///
    builty_data_available, last
sort state zipcode censusblockgroupfips
compress
save "`data'/analysis/`output_stub'.dta", replace

di as result "Saved: `data'/analysis/`output_stub'.dta"
di as result "Saved: `data'/analysis/`output_stub'_merge_diagnostics.dta"
di as result "States with links:`states_found'"
if trim("`states_missing'") != "" {
    di as txt "States without links:`states_missing'"
}

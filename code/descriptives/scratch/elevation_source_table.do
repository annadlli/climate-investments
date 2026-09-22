/******************************************************************************
Authors: Anna Li
Date: 2026-09-22
Description: Property counts by source of the elevation event on the analysis sample, and the
    NFIP-side breakdown (flag flip, ICC accepted or screened out, stock flagged from the first year).
******************************************************************************/

args data output

* Analysis sample: one row per property, Builty permit flag
use state property_id elevation_source using "`data'/analysis/analysis.dta", clear
bysort property_id: keep if _n == 1
gen builty = elevation_source == 3
keep state property_id builty
tempfile an
save `an'

* NFIP side from the build panel, before the harmonization overwrote the flag
use property_id policy_year elevated nfip_flip nfip_icc using "`data'/build/nfip_hma_panel.dta", clear
merge m:1 property_id using `an', keep(3) nogen
bysort property_id (policy_year): gen first_year = policy_year[1]
bysort property_id (policy_year): gen last_year = policy_year[_N]
bysort property_id (policy_year): gen stock = elevated[1] == 1
bysort property_id: keep if _n == 1
replace nfip_flip = . if nfip_flip == 2010 // panel start, as in complete.do
gen flip = !mi(nfip_flip)
gen icc_any = !mi(nfip_icc)
gen icc_ok = icc_any & nfip_icc < last_year // continued-coverage rule
gen icc_out = icc_any & !icc_ok
gen nfip = flip | icc_ok

* Table 1: source of the elevation
gen source = cond(!nfip & !builty, 1, cond(nfip & !builty, 2, cond(!nfip & builty, 3, 4)))
label define src 1 "None" 2 "NFIP only (flag flip or ICC)" 3 "Builty only (retrofit permit)" 4 "NFIP and Builty"
label values source src
tab source
tab source state

* Table 2: NFIP side
di "Elevated flag flips 0 -> 1 within the property, no accepted ICC: " 
count if flip & !icc_ok
di "ICC accepted (coverage continues), no flip: "
count if icc_ok & !flip
di "Both: "
count if flip & icc_ok
di "ICC screened out (coverage ends in the ICC year): "
count if icc_out
di "Flagged elevated from the first observed year (stock): "
count if stock
di "Total properties: " _N
* Reconciliation with the pipeline's elevation_source == 2 (ICC accepted, no flip, no Builty permit)
tab icc_ok builty if !flip, m
count if icc_ok & !flip & !builty
count if flip & builty
count if icc_out & builty

* Export
preserve
    contract source, freq(properties)
    decode source, gen(label)
    keep label properties
    export excel using "`output'/tables/elevation_source_table.xlsx", sheet("source") firstrow(variables) replace
restore
preserve
    gen cat = cond(flip & !icc_ok, 1, cond(icc_ok & !flip, 2, cond(flip & icc_ok, 3, cond(icc_out, 4, .))))
    replace cat = 5 if stock & mi(cat)
    label define cat 1 "Elevated flag flips 0 -> 1 within the property" 2 "ICC payment accepted (coverage continues), no flip" ///
        3 "Both" 4 "ICC payment screened out (coverage ends in the ICC year)" 5 "Flagged elevated from the first observed year (stock, no event)"
    label values cat cat
    drop if mi(cat)
    contract cat, freq(properties)
    decode cat, gen(label)
    keep label properties
    export excel using "`output'/tables/elevation_source_table.xlsx", sheet("nfip_side") firstrow(variables)
restore

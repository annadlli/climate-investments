/******************************************************************************
Author: Anna Li (drafted by Claude, 2026-09-05)
Date: 2026-09-05

Description: Counts home-elevation retrofits by state -- properties, not
    projects -- in Builty and in FEMA HMA, for the "Elevations by State" deck
    tab. HMA is split into FMA (incl. SRL), HMGP and all programs.

    HMA records are screened like clean_fma.do (single-family elevations,
    approved/obligated/closed projects) but without the program restriction,
    and numberofproperties is summed per record. The unscreened raw counts go
    on a second sheet for reference. Builty counts unique addresses in the
    screened elevation file, which already excludes elevated new construction.

    Note: the deck's earlier FMA-by-state table (5,572 total) came from an older
    cleaner version; the screened FMA column here totals 5,268. Rankings agree.

Inputs:  clean/builty_elevations.dta
         raw/hma_mitigated_properties.csv, raw/HazardMitigationAssistanceProjects.csv
Outputs: ../output/tables/elevations_by_state.xlsx  (sheets: funded, raw)
         ../output/figures/elevations_by_state.{gph,pdf,png}

******************************************************************************/

args data output

* Set options
// Sample states (master.do's 20-state list); the table keeps every state with
// a count but the figure shows these
local sample_states "AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA"
local blue   "23 107 135"
local light  "142 202 230"
local orange "224 122 95"
local opts graphregion(color(white)) plotregion(color(white))

* -----------------------------------------------------------------------------
* Section 1: HMA elevations by state and program
* -----------------------------------------------------------------------------

* Import property-level HMA data; keep single-family elevations, all programs
import delimited "`data'/raw/hma_mitigated_properties.csv", clear stringcols(_all)
keep if propertyaction == "Elevation" & structuretype == "Single Family"
destring numberofproperties, gen(n_properties) force
replace n_properties = 1 if mi(n_properties) | n_properties == 0 // a record is at least one structure
gen program = "Other"
replace program = "FMA"  if inlist(programarea, "FMA", "SRL")
replace program = "HMGP" if programarea == "HMGP"

* Raw counts, before any funding screen
preserve
    collapse (sum) n_properties, by(state program)
    reshape wide n_properties, i(state) j(program) string
    rename (n_propertiesFMA n_propertiesHMGP n_propertiesOther) (hma_fma hma_hmgp hma_other)
    foreach var in hma_fma hma_hmgp hma_other {
        replace `var' = 0 if mi(`var')
    }
    gen hma_total = hma_fma + hma_hmgp + hma_other
    tempfile hma_raw
    save `hma_raw'
restore

* Funding screen: merge project status and keep approved projects, as in clean_fma.do
preserve
    import delimited using "`data'/raw/HazardMitigationAssistanceProjects.csv", clear ///
        varnames(1) stringcols(_all) bindquote(strict)
    keep projectidentifier status
    duplicates drop projectidentifier, force
    tempfile projects
    save `projects'
restore
merge m:1 projectidentifier using `projects', keep(1 3) nogen
drop if inlist(status, "Not Approved / Denied", "Not Selected", "Withdrawn", ///
    "Void", "Revision Requested", "Pending")
drop if mi(status)

collapse (sum) n_properties, by(state program)
reshape wide n_properties, i(state) j(program) string
rename (n_propertiesFMA n_propertiesHMGP n_propertiesOther) (hma_fma hma_hmgp hma_other)
foreach var in hma_fma hma_hmgp hma_other {
    replace `var' = 0 if mi(`var')
}
gen hma_total = hma_fma + hma_hmgp + hma_other
tempfile hma_funded
save `hma_funded'

* -----------------------------------------------------------------------------
* Section 2: Builty elevation retrofits by state
* -----------------------------------------------------------------------------

use state street_address using "`data'/clean/builty_elevations.dta", clear
duplicates drop
contract state, freq(builty)
rename state state_abbrev

* State names, to match HMA
local abbrevs AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY
local names `" "Alabama" "Alaska" "Arizona" "Arkansas" "California" "Colorado" "Connecticut" "Delaware" "District of Columbia" "Florida" "Georgia" "Hawaii" "Idaho" "Illinois" "Indiana" "Iowa" "Kansas" "Kentucky" "Louisiana" "Maine" "Maryland" "Massachusetts" "Michigan" "Minnesota" "Mississippi" "Missouri" "Montana" "Nebraska" "Nevada" "New Hampshire" "New Jersey" "New Mexico" "New York" "North Carolina" "North Dakota" "Ohio" "Oklahoma" "Oregon" "Pennsylvania" "Rhode Island" "South Carolina" "South Dakota" "Tennessee" "Texas" "Utah" "Vermont" "Virginia" "Washington" "West Virginia" "Wisconsin" "Wyoming" "'
gen state = ""
local i = 1
foreach ab of local abbrevs {
    local nm : word `i' of `names'
    replace state = "`nm'" if state_abbrev == "`ab'"
    local i = `i' + 1
}
assert state != ""
tempfile builty
save `builty'

* -----------------------------------------------------------------------------
* Section 3: Combine and save
* -----------------------------------------------------------------------------

* One row per state: funded HMA + Builty
use `hma_funded', clear
merge 1:1 state using `builty', nogen
foreach var in builty hma_total hma_hmgp hma_fma hma_other {
    replace `var' = 0 if mi(`var')
}
// Fill abbreviations for HMA-only states
local i = 1
foreach ab of local abbrevs {
    local nm : word `i' of `names'
    replace state_abbrev = "`ab'" if state == "`nm'" & mi(state_abbrev)
    local i = `i' + 1
}
gen sample = strpos(" `sample_states' ", " " + state_abbrev + " ") > 0
gsort -sample -hma_total
order state state_abbrev sample builty hma_total hma_hmgp hma_fma hma_other

label var state        "State"
label var state_abbrev "State"
label var sample       "In the 20-state sample"
label var builty       "Builty elevation permits (properties)"
label var hma_total    "HMA elevations, all programs (properties)"
label var hma_hmgp     "HMGP elevations (properties)"
label var hma_fma      "FMA + SRL elevations (properties)"
label var hma_other    "Other HMA programs: PDM, BRIC, RFC, LPDM (properties)"

export excel using "`output'/tables/elevations_by_state.xlsx", ///
    sheet("funded") firstrow(varlabels) replace
tempfile combined
save `combined'

* Raw sheet: HMA before the funding screen
use `hma_raw', clear
order state hma_total hma_hmgp hma_fma hma_other
label var hma_total "HMA elevations, all records (properties)"
label var hma_hmgp  "HMGP elevations, all records"
label var hma_fma   "FMA + SRL elevations, all records"
label var hma_other "Other programs, all records"
gsort -hma_total
export excel using "`output'/tables/elevations_by_state.xlsx", ///
    sheet("raw") firstrow(varlabels) sheetreplace

* -----------------------------------------------------------------------------
* Section 4: Figure -- sample states, sorted by HMA total
* -----------------------------------------------------------------------------

use `combined', clear
keep if sample
gsort -hma_total
gen order = _n
forvalues i = 1/`=_N' {
    label define state_order `=order[`i']' "`=state_abbrev[`i']'", add
}
label values order state_order

// Three bars per state: Builty permits, HMGP, FMA (HMA total = HMGP + FMA + other)
graph hbar builty hma_hmgp hma_fma, over(order, label(labsize(small))) ///
    bar(1, color("`orange'")) bar(2, color("`light'")) bar(3, color("`blue'")) ///
    `opts' ///
    legend(order(1 "Builty elevation permits" 2 "HMGP-funded" 3 "FMA-funded") ///
        rows(1) pos(6) region(lcolor(white)) size(small)) ///
    ytitle("Elevated single-family properties", size(small)) ///
    blabel(bar, size(tiny) format(%9.0fc)) ///
    note("Builty: screened retrofit permits, unique addresses. HMA: approved projects, properties per record.", ///
        pos(7) size(vsmall) color(gs7))
graph save   "`output'/figures/elevations_by_state.gph", replace
graph export "`output'/figures/elevations_by_state.png", width(2400) replace

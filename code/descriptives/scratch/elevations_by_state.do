/******************************************************************************
Author: Anna Li
Date: 2026-09-05 (rewritten 2026-09-08 to read the clean files)

Description: Home-elevation retrofits by state, properties not projects, in
    Builty and in FEMA HMA (all programs, HMGP, FMA incl. SRL), for the deck's
    "Elevations by State" tab. One table and one figure.

Inputs:  clean/builty_elevations.dta (the appended states), clean/fma_elevation.dta
Outputs: ../output/tables/elevations_by_state.xlsx
         ../output/figures/elevations_by_state.{gph,png}

******************************************************************************/

args data output

* Set options
local sample_states "AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA" // the figure shows these; the table keeps every state
local blue   "23 107 135"
local light  "142 202 230"
local orange "224 122 95"
local opts graphregion(color(white)) plotregion(color(white))

* -----------------------------------------------------------------------------

* HMA: properties per record summed by state and program (clean_fma keeps all programs)
use state programarea n_properties_rec using "`data'/clean/fma_elevation.dta", clear
gen program = "Other"
replace program = "FMA"  if inlist(programarea, "FMA", "SRL")
replace program = "HMGP" if programarea == "HMGP"
collapse (sum) n_properties_rec, by(state program)
reshape wide n_properties_rec, i(state) j(program) string
rename (n_properties_recFMA n_properties_recHMGP n_properties_recOther) (hma_fma hma_hmgp hma_other)
foreach var in hma_fma hma_hmgp hma_other {
    replace `var' = 0 if mi(`var')
}
gen hma_total = hma_fma + hma_hmgp + hma_other
tempfile hma
save `hma'

* Builty: every state file in clean/builty_states, collapsed to one row per property
// Note: the appended clean/builty_elevations.dta covers only master.do's states, so the
// per-state files are read directly; states screened before 09-06 have no
// new_construction flag and are all retrofits by construction
local files : dir "`data'/clean/builty_states" files "builty_elevations_*.dta"
tempfile permits
local first = 1
foreach f of local files {
    use "`data'/clean/builty_states/`f'", clear
    capture confirm variable new_construction
    if _rc != 0 gen byte new_construction = 0
    keep state county street_address new_construction
    if `first' == 0 append using `permits'
    save `permits', replace
    local first = 0
}
keep if new_construction == 0
duplicates drop state county street_address, force
contract state, freq(builty)
rename state state_abbrev

* State names to match HMA (the clean file carries full names)
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

* Combine: one row per state
merge 1:1 state using `hma', nogen
foreach var in builty hma_total hma_hmgp hma_fma hma_other {
    replace `var' = 0 if mi(`var')
}
local i = 1
foreach ab of local abbrevs {
    local nm : word `i' of `names'
    replace state_abbrev = "`ab'" if state == "`nm'" & mi(state_abbrev)
    local i = `i' + 1
}
gsort -hma_total
order state state_abbrev builty hma_total hma_hmgp hma_fma hma_other

* Label variables
label var state        "State"
label var state_abbrev "State"
label var builty       "Builty elevation retrofits (properties)"
label var hma_total    "HMA elevations, all programs (properties)"
label var hma_hmgp     "HMGP elevations (properties)"
label var hma_fma      "FMA + SRL elevations (properties)"
label var hma_other    "Other HMA programs (properties)"

* Output table
export excel using "`output'/tables/elevations_by_state.xlsx", firstrow(varlabels) replace

* Figure: sample states only, sorted by HMA total, largest on top
gen byte sample = 0
foreach ab of local sample_states {
    replace sample = 1 if state_abbrev == "`ab'"
}
keep if sample
gsort -hma_total
gen order = _n
forvalues i = 1/`=_N' {
    label define state_order `=order[`i']' "`=state_abbrev[`i']'", add
}
label values order state_order
graph hbar builty hma_hmgp hma_fma, over(order, label(labsize(small))) ///
    bar(1, color("`orange'")) bar(2, color("`light'")) bar(3, color("`blue'")) ///
    `opts' ///
    legend(order(1 "Builty elevation permits" 2 "HMGP-funded" 3 "FMA-funded") ///
        rows(1) pos(6) region(lcolor(white)) size(small)) ///
    blabel(bar, size(tiny) format(%9.0fc)) ///
    ytitle("Elevated single-family properties", size(small)) ///
    note("Builty: screened retrofit permits, unique addresses. HMA: approved projects, properties per record.", ///
        pos(7) size(vsmall) color(gs7))
graph save   "`output'/figures/elevations_by_state.gph", replace
graph export "`output'/figures/elevations_by_state.png", width(2400) replace

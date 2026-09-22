/******************************************************************************
Author: Anna Li
Date: 2026-09-05 
Revised： 2026-09-22

Description: Home-elevation retrofits by state, properties not projects, in
    Builty, in FEMA HMA (all programs, HMGP, FMA incl. SRL) and in NFIP, for the
    deck's "Elevations by State" tab. One table and one figure.
     09-15: NFIP series added, including ICC and elevated flag flips

******************************************************************************/

args data output

* Set options
local sample_states "AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA" // the figure shows these; the table keeps every state
local blue   "23 107 135"
local light  "142 202 230"
local orange "224 122 95"
local gray   "110 110 110" // 09-15: NFIP bar
local opts graphregion(color(white)) plotregion(color(white))

* -----------------------------------------------------------------------------

* HMA: properties per record summed by state and program (clean_fma keeps all programs)
// 09-20: reads hma_elevation.dta (the 09-15 name) and adds a closed-projects-only count
// (optional variant, not in the pipeline; status comes from the raw projects file)
use state programarea project_identifier n_properties_rec using "`data'/clean/hma_elevation.dta", clear
// Project status from the raw HMA projects file (the clean file does not carry it)
preserve
    import delimited "`data'/raw/HazardMitigationAssistanceProjects.csv", varnames(1) case(lower) stringcols(_all) bindquote(strict) clear
    keep projectidentifier status
    gen str100 project_identifier = projectidentifier // import gives a strL, which cannot be a merge key
    drop projectidentifier
    duplicates drop project_identifier, force
    tempfile hma_status
    save `hma_status'
restore
merge m:1 project_identifier using `hma_status', keep(1 3) nogen keepusing(status)
gen closed = status == "Closed"
gen program = "Other"
replace program = "FMA"  if inlist(programarea, "FMA", "SRL")
replace program = "HMGP" if programarea == "HMGP"
gen n_closed = n_properties_rec * closed
collapse (sum) n_properties_rec n_closed, by(state program)
reshape wide n_properties_rec n_closed, i(state) j(program) string
rename (n_properties_recFMA n_properties_recHMGP n_properties_recOther) (hma_fma hma_hmgp hma_other)
gen hma_closed = n_closedFMA + n_closedHMGP + n_closedOther
drop n_closed*
foreach var in hma_fma hma_hmgp hma_other hma_closed {
    replace `var' = 0 if mi(`var')
}
gen hma_total = hma_fma + hma_hmgp + hma_other
tempfile hma
save `hma'

* NFIP: properties whose elevated flag flips 0 -> 1 or that receive an ICC payment (09-15)
use state property_id policy_year elevated claim_icc using "`data'/build/nfip_hma_panel.dta", clear
sort property_id policy_year
by property_id: gen flip = elevated == 1 & elevated[_n-1] == 0 if _n > 1
by property_id: egen nfip_flip = max(flip)
by property_id: egen nfip_icc = max(claim_icc > 0 & !mi(claim_icc))
by property_id: keep if _n == 1
gen nfip_any = nfip_flip == 1 | nfip_icc == 1
collapse (sum) nfip_flip nfip_icc nfip_any, by(state)
rename state state_abbrev
tempfile nfip
save `nfip'

* Analysis sample: elevation events per insured home, by state  -- 09-16 (Vendela 09-15)
// analysis.dta after complete.do's restrictions: SFHA homes in FL LA TX, county-years with
// Builty coverage. Events are the harmonized elevation (Builty permit; NFIP flag flip; ICC
// payment with continued coverage). Source 3 = Builty, 1 = NFIP flip, 2 = ICC.
use state property_id elevation_retrofit elevation_source using "`data'/analysis/analysis.dta", clear
bysort state property_id: keep if _n == 1
gen byte ev_builty = elevation_source == 3
gen byte ev_nfip   = inlist(elevation_source, 1, 2)
collapse (count) an_homes = property_id (sum) an_events = elevation_retrofit an_builty = ev_builty an_nfip = ev_nfip, by(state)
gen an_per_1000 = 1000 * an_events / an_homes
rename state state_abbrev
tempfile asample
save `asample'

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
foreach var in builty hma_total hma_hmgp hma_fma hma_other hma_closed {
    replace `var' = 0 if mi(`var')
}
local i = 1
foreach ab of local abbrevs {
    local nm : word `i' of `names'
    replace state_abbrev = "`ab'" if state == "`nm'" & mi(state_abbrev)
    local i = `i' + 1
}
merge 1:1 state_abbrev using `nfip', nogen // 09-15
foreach var in nfip_any nfip_flip nfip_icc {
    replace `var' = 0 if mi(`var')
}
merge 1:1 state_abbrev using `asample', nogen // 09-16: analysis-sample columns, missing outside FL LA TX
// 09-20: every count as a share of the state's properties in analysis.dta
// (the final analysis sample; missing outside FL LA TX)
foreach var in builty hma_total hma_hmgp hma_fma hma_closed nfip_any an_events {
    gen sh_`var' = `var' / an_homes
}
gsort -hma_total
order state state_abbrev builty hma_total hma_closed hma_hmgp hma_fma hma_other nfip_any nfip_flip nfip_icc ///
    an_homes an_events an_builty an_nfip an_per_1000 sh_builty sh_hma_total sh_hma_hmgp sh_hma_fma sh_hma_closed sh_nfip_any sh_an_events

* Label variables
label var state        "State"
label var state_abbrev "State"
label var builty       "Builty elevation retrofits (properties)"
label var hma_closed   "FEMA HMA elevations, closed (completed) projects only (properties)"
label var sh_builty    "Builty properties / analysis-sample properties"
label var sh_hma_total "HMA properties / analysis-sample properties"
label var sh_hma_closed "HMA closed-project properties / analysis-sample properties"
label var sh_hma_hmgp  "HMGP properties / analysis-sample properties"
label var sh_hma_fma   "FMA + SRL properties / analysis-sample properties"
label var sh_nfip_any  "NFIP elevations / analysis-sample properties"
label var sh_an_events "Analysis-sample elevation events / analysis-sample properties"
label var hma_total    "HMA elevations, all programs (properties)"
label var hma_hmgp     "HMGP elevations (properties)"
label var hma_fma      "FMA + SRL elevations (properties)"
label var hma_other    "Other HMA programs (properties)"
label var nfip_any     "NFIP elevations: flag flip or ICC payment (properties)" 
label var nfip_flip    "NFIP elevated flag flips 0 to 1 (properties)"
label var nfip_icc     "NFIP ICC payment received (properties)"
label var an_homes     "Analysis sample: insured SFHA homes (properties)"
label var an_events    "Analysis sample: elevation events, any source (properties)"
label var an_builty    "Analysis sample: Builty permit events (properties)"
label var an_nfip      "Analysis sample: NFIP flip or ICC events (properties)"
label var an_per_1000  "Analysis sample: elevation events per 1,000 insured homes"

* Output table
export excel using "`output'/tables/elevations_by_state.xlsx", firstrow(varlabels) replace

* Second table (09-22, Anna): count and share of the state's analysis-sample properties in one
* cell, "1,568 (0.26%)", for the three analysis states only (the base exists nowhere else)
preserve
    keep if !mi(an_homes)
    foreach var in builty hma_total hma_hmgp hma_fma hma_closed nfip_any an_events {
        gen str30 fmt_`var' = string(`var', "%12.0fc") + " (" + string(100 * sh_`var', "%4.2f") + "%)"
    }
    gen str12 fmt_an_homes = string(an_homes, "%12.0fc")
    keep state fmt_an_homes fmt_builty fmt_hma_total fmt_hma_hmgp fmt_hma_fma fmt_hma_closed fmt_nfip_any fmt_an_events
    order state fmt_an_homes fmt_builty fmt_hma_total fmt_hma_hmgp fmt_hma_fma fmt_hma_closed fmt_nfip_any fmt_an_events
    label var fmt_an_homes  "Analysis sample: insured SFHA homes"
    label var fmt_builty    "Builty elevation retrofits (share of homes)"
    label var fmt_hma_total "HMA elevations, all programs (share of homes)"
    label var fmt_hma_hmgp  "HMGP elevations (share of homes)"
    label var fmt_hma_fma   "FMA + SRL elevations (share of homes)"
    label var fmt_hma_closed "HMA closed-project elevations (share of homes)"
    label var fmt_nfip_any  "NFIP flag flip or ICC payment (share of homes)"
    label var fmt_an_events "Analysis-sample elevation events, any source (share of homes)"
    export excel using "`output'/tables/elevations_by_state_shares.xlsx", firstrow(varlabels) replace
restore

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
// 09-15: NFIP bar added; bar labels off 
// legend inside the plot at the lower right, where the small states leave room
graph hbar builty hma_hmgp hma_fma nfip_flip, over(order, label(labsize(small)) gap(30)) ///
    bar(1, color("`orange'")) bar(2, color("`light'")) bar(3, color("`blue'")) bar(4, color("`gray'")) ///
    `opts' xsize(8) ysize(5) ylabel(0(1000)3000) yscale(range(0 3600)) /// 
    legend(order(1 "Builty elevation permits" 2 "HMGP-funded" 3 "FMA-funded" ///
        4 "NFIP: elevated flag flips 0 to 1") ///
        cols(1) pos(5) ring(0) region(lcolor(white) fcolor(white)) size(small)) ///
    ytitle("Elevated single-family properties", size(small)) 
graph save   "`output'/figures/elevations_by_state.gph", replace
graph export "`output'/figures/elevations_by_state.png", width(2400) replace

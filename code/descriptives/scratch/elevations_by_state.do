/******************************************************************************
Authors: Anna Li
Date: 2026-10-02

Description: Home-elevation retrofits by state, properties not projects, from ATTOM
    building permits (FL LA TX), the superseded Builty permits (20 states), FEMA HMA
    grants (all programs) and the NFIP flag, with the analysis-sample rates and the
    tabulation of elevated_attom by state. Three tables and one figure.

******************************************************************************/

* Claude change 10-02: ATTOM permits replace Builty as the permit source. Run by hand:
*   python descriptives/scratch/permit_homes_sfha.py --data <data> --output <output>   (SFHA column)
*   do elevations_by_state.do "<data root>" "<output root>"
* The Builty column reads the old clean/builty_states files that are still on Dropbox.

args data output

* Set options
local blue   "23 107 135"
local light  "142 202 230"
local orange "224 122 95"
local gray   "110 110 110"
local opts graphregion(color(white)) plotregion(color(white))

* -----------------------------------------------------------------------------

* HMA: properties per record summed by state and program, with a closed-projects-only count
// (status comes from the raw projects file; the clean file does not carry it)
use state programarea project_identifier n_properties_rec using "`data'/clean/hma_elevation.dta", clear
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

* NFIP: properties whose elevated flag flips 0 -> 1 or that receive an ICC payment
// (kept for comparison; neither is an elevation event in the pipeline since 10-02)
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

* Analysis sample: permit events per insured home, by state, and elevated_attom by state
// analysis.dta after complete.do's restrictions: SFHA homes in FL LA TX. One row per home,
// taken in its last policy year, so elevated_attom is the home's final status
use state property_id policy_year elevation_retrofit elevated_attom using "`data'/analysis/analysis.dta", clear
bysort state property_id (policy_year): keep if _n == _N
tab elevated_attom state, m
preserve
    gen status = cond(mi(elevated_attom), "Unobservable (no ATTOM match or no permit feed)", ///
        cond(elevated_attom == 1, "Elevated per ATTOM permit", "Not elevated"))
    contract state status, freq(homes)
    bysort state: egen state_homes = total(homes)
    gen share = homes / state_homes
    label var status "elevated_attom (last policy year)"
    label var homes  "Insured SFHA homes"
    label var share  "Share of the state's homes"
    export excel state status homes share using "`output'/tables/elevated_attom_by_state.xlsx", firstrow(varlabels) replace
restore
gen observable = !mi(elevated_attom)
collapse (count) an_homes = property_id (sum) an_observable = observable an_events = elevation_retrofit, by(state)
gen an_per_1000 = 1000 * an_events / an_homes
gen an_per_1000_obs = 1000 * an_events / an_observable
rename state state_abbrev
tempfile asample
save `asample'

* ATTOM permits: retrofit addresses by state
local permit_files : dir "`data'/clean/permit_states" files "permit_elevations_*.dta"
tempfile permits
local first = 1
foreach f of local permit_files {
    use state county street_address retrofit using "`data'/clean/permit_states/`f'", clear
    if `first' == 0 append using `permits'
    save `permits', replace
    local first = 0
}
keep if retrofit == 1
duplicates drop state county street_address, force
contract state, freq(attom_permits)
rename state state_abbrev
tempfile attom
save `attom'

* ATTOM permits that reached an ATTOM property record, and those in an SFHA (permit_homes_sfha.py)
import delimited "`output'/tables/permit_homes_sfha.csv", varnames(1) clear
keep state_abbrev permit_homes_linked permit_homes_sfha
rename (permit_homes_linked permit_homes_sfha) (attom_permits_linked attom_permits_sfha)
merge 1:1 state_abbrev using `attom', nogen
save `attom', replace

* Builty (superseded 10-02): every state file in clean/builty_states, one row per property
local files : dir "`data'/clean/builty_states" files "builty_elevations_*.dta"
tempfile bpermits
local first = 1
foreach f of local files {
    use "`data'/clean/builty_states/`f'", clear
    capture confirm variable new_construction
    if _rc != 0 gen byte new_construction = 0
    keep state county street_address new_construction
    if `first' == 0 append using `bpermits'
    save `bpermits', replace
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
merge 1:1 state_abbrev using `nfip', nogen
foreach var in nfip_any nfip_flip nfip_icc {
    replace `var' = 0 if mi(`var')
}
merge 1:1 state_abbrev using `attom', nogen   // ATTOM permit columns, missing outside FL LA TX
merge 1:1 state_abbrev using `asample', nogen // analysis-sample columns, missing outside FL LA TX
// every count as a share of the state's properties in analysis.dta (missing outside FL LA TX)
foreach var in attom_permits attom_permits_sfha builty hma_total hma_closed nfip_any an_events {
    gen sh_`var' = `var' / an_homes
}
gsort -hma_total
order state state_abbrev attom_permits attom_permits_linked attom_permits_sfha builty hma_total hma_closed hma_hmgp hma_fma hma_other ///
    nfip_any nfip_flip nfip_icc an_homes an_observable an_events an_per_1000 an_per_1000_obs ///
    sh_attom_permits sh_attom_permits_sfha sh_builty sh_hma_total sh_hma_closed sh_nfip_any sh_an_events

* Label variables
label var state                "State"
label var state_abbrev         "State"
label var attom_permits        "ATTOM elevation permits, retrofit addresses"
label var attom_permits_linked "ATTOM permit addresses linked to an ATTOM property record"
label var attom_permits_sfha   "ATTOM permit homes in an SFHA (NFHL)"
label var builty               "Builty elevation retrofits (properties; superseded source)"
label var hma_total            "HMA elevations, all programs (properties)"
label var hma_closed           "FEMA HMA elevations, closed (completed) projects only (properties)"
label var hma_hmgp             "HMGP elevations (properties)"
label var hma_fma              "FMA + SRL elevations (properties)"
label var hma_other            "Other HMA programs (properties)"
label var nfip_any             "NFIP: flag flip or ICC payment (properties; not an event)"
label var nfip_flip            "NFIP elevated flag flips 0 to 1 (properties)"
label var nfip_icc             "NFIP ICC payment received (properties)"
label var an_homes             "Analysis sample: insured SFHA homes (properties)"
label var an_observable        "Analysis sample: homes where a permit is observable (ATTOM match, permit feed)"
label var an_events            "Analysis sample: ATTOM permit events (properties)"
label var an_per_1000          "Analysis sample: permit events per 1,000 insured homes"
label var an_per_1000_obs      "Analysis sample: permit events per 1,000 observable homes"
label var sh_attom_permits     "ATTOM permit addresses / analysis-sample properties"
label var sh_attom_permits_sfha "ATTOM SFHA permit homes / analysis-sample properties"
label var sh_builty            "Builty properties / analysis-sample properties"
label var sh_hma_total         "HMA properties / analysis-sample properties"
label var sh_hma_closed        "HMA closed-project properties / analysis-sample properties"
label var sh_nfip_any          "NFIP flip or ICC / analysis-sample properties"
label var sh_an_events         "Analysis-sample permit events / analysis-sample properties"

* Output table
export excel using "`output'/tables/elevations_by_state.xlsx", firstrow(varlabels) replace

* Second table: count and share of the state's analysis-sample properties in one cell,
* "1,568 (0.26%)", for the three analysis states only (the base exists nowhere else)
preserve
    keep if !mi(an_homes)
    foreach var in attom_permits attom_permits_sfha builty hma_total hma_closed nfip_any an_events {
        gen str30 fmt_`var' = string(`var', "%12.0fc") + " (" + string(100 * sh_`var', "%4.2f") + "%)"
    }
    gen str12 fmt_an_homes = string(an_homes, "%12.0fc")
    gen str12 fmt_an_observable = string(an_observable, "%12.0fc")
    keep state fmt_an_homes fmt_an_observable fmt_attom_permits fmt_attom_permits_sfha fmt_builty fmt_hma_total fmt_hma_closed fmt_nfip_any fmt_an_events
    order state fmt_an_homes fmt_an_observable fmt_attom_permits fmt_attom_permits_sfha fmt_builty fmt_hma_total fmt_hma_closed fmt_nfip_any fmt_an_events
    label var fmt_an_homes      "Analysis sample: insured SFHA homes"
    label var fmt_an_observable "of which permit observable"
    label var fmt_attom_permits "ATTOM elevation permits, retrofit addresses (share of homes)"
    label var fmt_attom_permits_sfha "ATTOM permit homes in an SFHA (share of homes)"
    label var fmt_builty        "Builty elevation retrofits, superseded (share of homes)"
    label var fmt_hma_total     "HMA elevations, all programs (share of homes)"
    label var fmt_hma_closed    "HMA closed-project elevations (share of homes)"
    label var fmt_nfip_any      "NFIP flag flip or ICC payment (share of homes)"
    label var fmt_an_events     "Analysis-sample permit events (share of homes)"
    export excel using "`output'/tables/elevations_by_state_shares.xlsx", firstrow(varlabels) replace
restore

* Figure: every state, sorted by ATTOM retrofit addresses (Claude change 10-09)
// NFIP flips are left out: the NFIP panel is three-state, so other states would show false zeros.
// The SFHA bar comes from permit_homes_sfha_rest.py (FEMA's live NFHL service, every state);
// the FL LA TX column above uses the pipeline's 2018 NFHL join instead, so they differ a little.
preserve
    keep if (attom_permits > 0 | hma_total > 0) & state_abbrev != "" // territories carry HMA rows but no code
    tempfile all
    save `all'
    import delimited "`output'/tables/permit_homes_sfha_all.csv", varnames(1) clear
    keep state_abbrev in_sfha
    rename in_sfha attom_permits_sfha_rest
    merge 1:1 state_abbrev using `all', keep(2 3) nogen
    replace attom_permits_sfha_rest = 0 if mi(attom_permits_sfha_rest)
    gsort -attom_permits
    gen order_all = _n
    forvalues i = 1/`=_N' {
        label define state_order_all `=order_all[`i']' "`=state_abbrev[`i']'", add
    }
    label values order_all state_order_all
    graph hbar attom_permits attom_permits_sfha_rest hma_hmgp hma_fma, over(order_all, label(labsize(vsmall)) gap(20)) ///
        bar(1, color("`orange'")) bar(2, color("`orange'*0.5")) bar(3, color("`light'")) bar(4, color("`blue'")) ///
        `opts' xsize(7) ysize(10) ///
        legend(order(1 "ATTOM elevation permits (retrofit addresses)" 2 "of which in an SFHA (current NFHL)" 3 "HMGP-funded" 4 "FMA-funded") ///
            cols(1) pos(6) ring(1) region(lcolor(white) fcolor(white)) size(small)) ///
        ytitle("Elevated single-family properties", size(small))
    graph save   "`output'/figures/elevations_by_state_all.gph", replace
    graph export "`output'/figures/elevations_by_state_all.png", width(2400) replace
restore

* Figure: the three permit states, sorted by HMA total, largest on top
keep if inlist(state_abbrev, "FL", "LA", "TX")
gsort -hma_total
gen order = _n
forvalues i = 1/`=_N' {
    label define state_order `=order[`i']' "`=state_abbrev[`i']'", add
}
label values order state_order
graph hbar attom_permits attom_permits_sfha hma_hmgp hma_fma nfip_flip, over(order, label(labsize(small)) gap(30)) ///
    bar(1, color("`orange'")) bar(2, color("`orange'*0.5")) bar(3, color("`light'")) bar(4, color("`blue'")) bar(5, color("`gray'")) ///
    `opts' xsize(8) ysize(5) ///
    legend(order(1 "ATTOM elevation permits (retrofit addresses)" 2 "of which in an SFHA" 3 "HMGP-funded" 4 "FMA-funded" ///
        5 "NFIP: elevated flag flips 0 to 1") ///
        cols(2) pos(6) ring(1) region(lcolor(white) fcolor(white)) size(small)) ///
    ytitle("Elevated single-family properties", size(small)) 
graph save   "`output'/figures/elevations_by_state.gph", replace
graph export "`output'/figures/elevations_by_state.png", width(2400) replace

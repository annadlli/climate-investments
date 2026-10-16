/******************************************************************************
Authors: Anna Li
Date: 2026-10-07

Claude change 10-07: new scratch script, run by hand (not in master.do).

Description: Permit status of the screened ATTOM elevation permits, by state,
    for the deck's elevations-by-state table. One row per state with retrofit
    addresses by status group (filed, approved, issued, complete, final/closed,
    expired/cancelled/withdrawn, other) as counts and shares, built to merge onto
    elevations_by_state.xlsx on state_abbrev. A second sheet lists every raw
    status string with its group and count, so the mapping can be checked.

    The status is the one on the address's earliest permit (clean_permits.do
    collapses to one row per address with firstnm after sorting by year). Runs on
    whatever states clean/permit_elevations.dta holds (FL LA TX from the local
    10-02 run; all states once the cluster file is downloaded).

Inputs:  clean/permit_elevations.dta
Outputs: ../output/tables/permit_status_by_state.xlsx (sheets by_state, status_map)
Run:     do permit_status_by_state.do "<data root>" "<output folder>"
******************************************************************************/

args data output

use state status retrofit new_construction using "`data'/clean/permit_elevations.dta", clear
rename state state_abbrev
gen s = lower(trim(status))

* Status groups: later lines win, so a specific word beats a generic one
gen group = "other"
replace group = "filed"    if ustrregexm(s, "filed|applied|application|triage|plan ?check|plan review|review|pending|submit|resubmit|received|open|new|hold|waiting|in progress|fees|intake|prescreen|dox")
replace group = "approved" if ustrregexm(s, "approved|released")
replace group = "issued"   if ustrregexm(s, "issued|picked up|printed|active|reinstated|inspection|insp\b|substantial")
replace group = "complete" if ustrregexm(s, "complete|completed|passed|done|coc|c\.o\.|co''ed|coed|certificate")
replace group = "final"    if ustrregexm(s, "final|closed|closeout|clno")
replace group = "expired"  if ustrregexm(s, "expired|cancel|withdr|rejected|revoked|void|suspend|denied|invalid|error|stop work|inactive")
// Claude change 10-12: "incomplete application", "in-complete", "abandoned" matched the complete
// group through "complete"; they are dead applications
replace group = "expired"  if ustrregexm(s, "incomplete|in-complete|abandon")
replace group = "missing"  if mi(s)

* Mapping sheet
preserve
    contract state_abbrev s group, freq(addresses)
    collapse (sum) addresses, by(s group)
    gsort group -addresses
    rename s raw_status
    export excel using "`output'/tables/permit_status_by_state.xlsx", sheet("status_map") firstrow(variables) replace
restore

* By state, retrofit addresses
keep if retrofit == 1
gen one = 1
collapse (sum) one, by(state_abbrev group)
reshape wide one, i(state_abbrev) j(group) string
foreach g in filed approved issued complete final expired other missing {
    capture confirm variable one`g'
    if _rc gen one`g' = 0
    replace one`g' = 0 if mi(one`g')
    rename one`g' status_`g'
}
egen retrofit_addresses = rowtotal(status_*)
foreach g in filed approved issued complete final expired other missing {
    gen share_`g' = status_`g' / retrofit_addresses
}
order state_abbrev retrofit_addresses status_filed status_approved status_issued status_complete status_final status_expired status_other status_missing
label var state_abbrev        "State"
label var retrofit_addresses  "Retrofit addresses"
label var status_filed        "Filed / applied / under review"
label var status_approved     "Approved"
label var status_issued       "Issued"
label var status_complete     "Complete"
label var status_final        "Final / closed"
label var status_expired      "Expired / cancelled / withdrawn"
label var status_other        "Other status"
label var status_missing      "No status"
gsort -retrofit_addresses
export excel using "`output'/tables/permit_status_by_state.xlsx", sheet("by_state", replace) firstrow(variables)
list state_abbrev retrofit_addresses status_*, clean noobs
format share_* %5.2f
list state_abbrev share_*, clean noobs

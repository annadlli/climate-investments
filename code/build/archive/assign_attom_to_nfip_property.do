/******************************************************************************
Authors: Anna Li
Date: 2026-08-14

Description: Assign one ATTOM property to each NFIP property, once, at the
    initial policy year. Stata port of assign_attom_to_nfip_property.py.

    NFIP has no address, so each property is paired with one ATTOM property by
    deterministic rank inside a tiered cell. Ranking uses a cell-seeded Mata
    hash rather than the sort order, so the pairing is reproducible and
    uncorrelated with ATTOMID or property_id.

    Block group first, then ZIP, then community -- and at every geography,
    exact construction year before any coarsened bin.

    Every NFIP property is retained; unmatched ones carry missing ATTOM fields.
    Assignment is one-to-one, so assigned_attomid is unique among matches.

    Input  : {data}/clean/nfip_policies_property.dta
             {data}/clean/nfip_policies_state/{state}.dta   (community recovery)
             {data}/build/attom_stata/{state}_attom_candidates.dta
             {data}/build/attom_stata/{state}_attom_values.dta
             (the two ATTOM files come from export_attom_for_stata.py)
    Output : {data}/build/nfip_attom_property/{state}_nfip_attom_property.dta
             {data}/build/nfip_attom_property/{state}_tier_diagnostics.dta
******************************************************************************/

version 18
args data state

local st = lower("`state'")
local ST = upper("`state'")
local attom_dir "`data'/build/attom_stata"
local out_dir   "`data'/build/nfip_attom_property"
capture mkdir "`out_dir'"

* -----------------------------------------------------------------------------
* Programs
* -----------------------------------------------------------------------------
capture program drop hashrank
program define hashrank
    * Cell-seeded pseudo-random key: deterministic, independent of row order.
    syntax , CELL(name) ID(name) SALT(string) GEN(name)
    tempvar idstr
    capture confirm string variable `id'
    if _rc qui gen str32 `idstr' = string(`id', "%20.0f")
    else   qui gen str32 `idstr' = `id'
    qui gen double `gen' = .
    mata: _hashrank("`cell'", "`idstr'", "`salt'", "`gen'")
end

mata:
void _hashrank(string scalar cellv, string scalar idv, string scalar salt,
               string scalar outv)
{
    real scalar    i, n
    string colvector c, d
    real colvector h
    c = st_sdata(., cellv)
    d = st_sdata(., idv)
    n = rows(c)
    h = J(n, 1, .)
    for (i = 1; i <= n; i++) h[i] = hash1(c[i] + "|" + salt + "|" + d[i], 2147483647)
    st_store(., outv, h)
}
end

capture program drop cellkey
program define cellkey
    * Build the cell string and an eligibility flag from a list of key variables.
    syntax varlist, CELL(name) OK(name)
    qui gen str244 `cell' = ""
    qui gen byte   `ok'   = 1
    foreach v of local varlist {
        capture confirm string variable `v'
        if !_rc {
            qui replace `cell' = `cell' + "|" + `v'
            qui replace `ok'   = 0 if `v' == ""
        }
        else {
            qui replace `cell' = `cell' + "|" + string(`v')
            qui replace `ok'   = 0 if missing(`v')
        }
    }
end

* -----------------------------------------------------------------------------
* 1. NFIP property base for this state
* -----------------------------------------------------------------------------
use property_id state zipcode censusblockgroupfips construction_year ///
    ratedfloodzone postfirm policy_year_init ///
    using "`data'/clean/nfip_policies_property.dta", clear
qui keep if upper(strtrim(state)) == "`ST'"
qui count
if r(N) == 0 {
    di as error "assign: no NFIP properties for `ST'"
    exit 2000
}
local n_nfip = r(N)
di as txt "`ST': " as res %12.0fc `n_nfip' as txt " NFIP properties"

* Community lives only in the state policy file. property_id differs between the
* two files, so match on within-state rank and assert the counts agree first.
preserve
    use property_id policy_year nfipratedcommunitynumber ///
        using "`data'/clean/nfip_policies_state/`st'.dta", clear
    bysort property_id (policy_year): keep if _n == _N
    sort property_id
    qui count
    if r(N) != `n_nfip' {
        di as error "assign: property count mismatch state=" r(N) " canonical=`n_nfip'"
        exit 459
    }
    gen long _rank = _n
    gen str6 community_key = ""
    qui replace community_key = ///
        substr("000000" + strtrim(nfipratedcommunitynumber), ///
               length("000000" + strtrim(nfipratedcommunitynumber)) - 5, 6) ///
        if !missing(nfipratedcommunitynumber) & strtrim(nfipratedcommunitynumber) != ""
    keep _rank community_key
    tempfile comm
    save `comm'
restore
sort property_id
gen long _rank = _n
merge 1:1 _rank using `comm', assert(match) nogen
drop _rank

* Merge keys
gen str5  zip_key        = substr(strtrim(zipcode), 1, 5)
qui replace zip_key = "" if inlist(zip_key, ".", "00000")
gen str12 blockgroup_key = strtrim(censusblockgroupfips)
qui replace blockgroup_key = "" if blockgroup_key == "."
gen long   reference_year = policy_year_init
gen        construction_5yr    = floor(construction_year / 5)  * 5
gen        construction_decade = floor(construction_year / 10) * 10
gen byte   postfirm_key    = postfirm

* Detailed zone, then the two-sided risk allowlist: anything unrecognized
* (AR and friends) stays missing rather than being coded low risk.
gen str10 flood_zone_key = upper(subinstr(strtrim(ratedfloodzone), " ", "", .))
qui replace flood_zone_key = "AE" if regexm(flood_zone_key, "^A[0-9][0-9]?$")
qui replace flood_zone_key = "VE" if regexm(flood_zone_key, "^V[0-9][0-9]?$")
gen str9 flood_risk_key = ""
qui replace flood_risk_key = "high_risk" if inlist(flood_zone_key, "A", "AE", "AH", "AO", "V", "VE")
qui replace flood_risk_key = "low_risk"  if inlist(flood_zone_key, "B", "C", "D", "X", "XE")

* Assignment placeholders
gen byte   nfip_attom_merge_status = 1
gen str40  match_tier        = ""
gen byte   match_tier_number = .
gen str64  match_cell_id     = ""
gen str32  assigned_attomid  = ""
gen str24  assignment_method = ""
gen long   nfip_cell_rank = .
gen long   attom_cell_rank = .
gen long   nfip_cell_n = .
gen long   attom_cell_n = .
gen long   builty_attom_cell_n = .
gen byte   cell_singleton = .

tempfile nfipbase
save `nfipbase'

* -----------------------------------------------------------------------------
* 2. ATTOM candidate universe in its own frame
* -----------------------------------------------------------------------------
capture frame drop attomf
frame create attomf
frame attomf {
    use "`attom_dir'/`st'_attom_candidates.dta", clear
    gen byte assigned = 0
    qui count
    di as txt "`ST': " as res %12.0fc r(N) as txt " ATTOM candidates"
}

* -----------------------------------------------------------------------------
* 3. Tier waterfall
* -----------------------------------------------------------------------------
local tier1  "blockgroup_key flood_zone_key construction_year"
local tier2  "blockgroup_key flood_risk_key construction_year"
local tier3  "blockgroup_key flood_zone_key construction_5yr postfirm_key"
local tier4  "blockgroup_key flood_zone_key construction_decade postfirm_key"
local tier5  "zip_key flood_zone_key construction_year"
local tier6  "zip_key flood_risk_key construction_year"
local tier7  "zip_key flood_risk_key construction_decade postfirm_key"
local tier8  "community_key flood_zone_key construction_year"
local tier9  "community_key flood_risk_key construction_5yr postfirm_key"
local tier10 "community_key flood_risk_key construction_decade postfirm_key"

local lab1  "1_bg_zone_exact_year"
local lab2  "2_bg_risk_exact_year"
local lab3  "3_bg_zone_5yr_postfirm"
local lab4  "4_bg_zone_decade_postfirm"
local lab5  "5_zip_zone_exact_year"
local lab6  "6_zip_risk_exact_year"
local lab7  "7_zip_risk_decade_postfirm"
local lab8  "8_community_zone_exact_year"
local lab9  "9_community_risk_5yr_postfirm"
local lab10 "10_community_risk_decade_postfirm"

tempname diag
postfile `diag' str40 match_tier int tier_number long cells ///
    long nfip_in_cells long attom_in_cells long singleton_cells ///
    long assignments long unmatched_after using "`out_dir'/`st'_tier_diagnostics.dta", replace

forvalues t = 1/10 {
    local keys "`tier`t''"
    local label "`lab`t''"

    * --- ATTOM side: rank the still-unassigned candidates within each cell
    tempfile aside
    frame attomf {
        preserve
            qui keep if assigned == 0
            cellkey `keys', cell(_cell) ok(_ok)
            qui keep if _ok == 1
            hashrank, cell(_cell) id(attomid) salt("attom|fixed_seed") gen(_h)
            sort _cell _h attomid
            by _cell: gen long _rank = _n
            by _cell: gen long _an   = _N
            by _cell: egen long _bn  = total(builty_elevated)
            keep _cell _rank _an _bn attomid ///
                 attom_flood_zone_original flood_zone_key flood_risk_key ///
                 attom_nfhl_flood_matched attom_nfhl_community_matched ///
                 builty_elevated builty_elevation_year builty_n_properties ///
                 builty_merge_status builty_attom_match_tier
            rename flood_zone_key attom_flood_zone_key
            rename flood_risk_key attom_flood_risk_key
            rename attomid _attomid
            save `aside'
        restore
    }

    * --- NFIP side: rank the still-unmatched properties within the same cells
    tempfile hits
    preserve
        qui keep if nfip_attom_merge_status == 1
        cellkey `keys', cell(_cell) ok(_ok)
        qui keep if _ok == 1
        hashrank, cell(_cell) id(property_id) salt("nfip|fixed_seed") gen(_h)
        sort _cell _h property_id
        by _cell: gen long _rank = _n
        by _cell: gen long _nn   = _N
        keep property_id _cell _rank _nn
        * Rank r on one side pairs with rank r on the other.
        merge 1:1 _cell _rank using `aside', keep(match) nogen
        save `hits'
    restore

    * --- Record the assignments
    qui merge 1:1 property_id using `hits', keep(master match) gen(_hit)
    qui count if _hit == 3
    local n_assigned = r(N)
    if `n_assigned' > 0 {
        qui replace nfip_attom_merge_status = 3         if _hit == 3
        qui replace match_tier        = "`label'"       if _hit == 3
        qui replace match_tier_number = `t'             if _hit == 3
        qui replace match_cell_id     = "`label'" + _cell if _hit == 3
        qui replace assigned_attomid  = _attomid        if _hit == 3
        qui replace nfip_cell_rank    = _rank           if _hit == 3
        qui replace attom_cell_rank   = _rank           if _hit == 3
        qui replace nfip_cell_n       = _nn             if _hit == 3
        qui replace attom_cell_n      = _an             if _hit == 3
        qui replace builty_attom_cell_n = _bn           if _hit == 3
        qui replace cell_singleton    = (_nn == 1 & _an == 1) if _hit == 3
        qui replace assignment_method = cond(cell_singleton == 1, "singleton", ///
                                             "deterministic_hash_rank")        if _hit == 3
    }
    drop _hit _cell _attomid _rank _nn _an _bn

    * --- Retire the used ATTOM properties
    if `n_assigned' > 0 {
        tempfile used
        preserve
            qui keep if nfip_attom_merge_status == 3 & match_tier == "`label'"
            keep assigned_attomid
            rename assigned_attomid attomid
            duplicates drop
            save `used'
        restore
        frame attomf {
            qui merge 1:1 attomid using `used', keep(master match) gen(_u)
            qui replace assigned = 1 if _u == 3
            drop _u
        }
    }

    qui count if nfip_attom_merge_status == 1
    local left = r(N)
    qui summarize nfip_cell_n if match_tier == "`label'", meanonly
    post `diag' ("`label'") (`t') (.) (.) (.) (.) (`n_assigned') (`left')
    di as txt "  " %-36s "`label'" as res %10.0fc `n_assigned' ///
       as txt "   (unmatched left " as res %12.0fc `left' as txt ")"
}
postclose `diag'

* -----------------------------------------------------------------------------
* 4. ATTOM values as of the reference year (closest assessment at or before it)
* -----------------------------------------------------------------------------
tempfile vals
preserve
    qui keep if assigned_attomid != ""
    keep property_id assigned_attomid reference_year
    rename assigned_attomid attomid
    joinby attomid using "`attom_dir'/`st'_attom_values.dta", unmatched(none)
    qui drop if year > reference_year
    bysort property_id (year): keep if _n == _N
    rename year attom_value_year
    gen long attom_value_lag = reference_year - attom_value_year
    * attom_assessed_value_improvements would be 33 characters; Stata caps names
    * at 32, and the parquet build truncates it the same way.
    rename assessed_value_improvements attom_assessed_value_improvement
    foreach v in market_value_total market_value_land market_value_improvements ///
                 assessed_value_total previous_assessed_value last_sale_price {
        rename `v' attom_`v'
    }
    drop attomid reference_year
    save `vals'
restore
merge 1:1 property_id using `vals', keep(master match) nogen

* -----------------------------------------------------------------------------
* 5. Checks, labels, save
* -----------------------------------------------------------------------------
isid property_id
qui count if assigned_attomid != ""
local n_matched = r(N)
preserve
    qui keep if assigned_attomid != ""
    qui duplicates report assigned_attomid
    if r(unique_value) != r(N) {
        di as error "assign: assigned_attomid is not unique"
        exit 459
    }
restore

label var nfip_attom_merge_status "1 = NFIP only, 3 = NFIP matched to ATTOM"
label var assigned_attomid        "ATTOM property assigned to this NFIP property"
label var match_tier              "Cell tier that produced the assignment"
label var attom_value_year        "ATTOM tax year used (as-of reference year)"
label var attom_value_lag         "Reference year minus ATTOM tax year"
label var builty_elevated         "Builty elevation permit on the assigned ATTOM property"
label var cell_singleton          "Cell held exactly one NFIP and one ATTOM property"

compress
save "`out_dir'/`st'_nfip_attom_property.dta", replace
di as result "`ST': matched " %12.0fc `n_matched' " / " %12.0fc `n_nfip' ///
    "  (" %5.1f 100*`n_matched'/`n_nfip' "%); ATTOMIDs unique"
di as result "Saved: `out_dir'/`st'_nfip_attom_property.dta"
di as result "Saved: `out_dir'/`st'_tier_diagnostics.dta"

capture frame drop attomf

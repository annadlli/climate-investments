/******************************************************************************
Authors: Anna Li
Date: 2026-07-06

Description:
    Wagner-style tiered merge of ATTOM property values onto the NFIP policy
    universe. Runs entirely on the small ATTOM value-cell files produced by
    build_attom_wagner_cells.py -- the raw 41GB parquet is never needed here.

    NFIP has no street address, so ATTOM value attaches by *cell*, finest first.
    Each policy-year takes its value from the first tier that matches; the tier
    and the ATTOM tax year used are recorded in attom_tier / attom_year. Within a
    tier the tax year is matched AS-OF the policy year (most recent assessment at
    or before the policy year).

    Tiers (exhaust zip x construction bins first, then county, then pure-geo):
        1. zip    x construction-year   x ~year
        2. zip    x construction-5yr    x ~year
        3. zip    x construction-decade x ~year
        4. county x construction-year   x ~year
        5. county x construction-5yr    x ~year
        6. county x construction-decade x ~year
        7. zip    x ~year                        (backstop)
        8. county x ~year                        (backstop)

******************************************************************************/

version 18

args data state
local stl = lower("`state'")

* -----------------------------------------------------------------------------
* As-of tier program: fill ATTOM value on still-unmatched rows from one cell file.
*   Backward as-of on year: a policy-year takes the cell from the latest tax year
*   <= the policy year, within the exact geo (+ construction) key. Dependency-free
*   (append cell + NFIP rows, sort by year with cells before NFIP at ties, then
*   carry the last cell value forward onto the NFIP rows).
* -----------------------------------------------------------------------------
capture program drop asoftier
program define asoftier
    syntax using/, GEO(name) GEOL(string) CELLTIER(string) TIER(string) [CON(name)]

    tempfile cells
    preserve
        * Pull only the current Wagner cell tier from the combined ATTOM cell file.
        use "`using'", clear
        keep if source_type == "wagner" & geo_level == "`geol'" & tier == "`celltier'"
        if "`con'" != "" keep `geo' `con' year attom_n_properties ///
            attom_mean_market_total attom_median_market_total
        else keep `geo' year attom_n_properties ///
            attom_mean_market_total attom_median_market_total
        save `cells'
    restore

    preserve
        * Eligible = unmatched rows with the exact keys present.
        keep if attom_tier == "" & `geo' != ""
        if "`con'" != "" keep if !missing(`con')
        if "`con'" != "" gen _cellkey = `geo' + "|" + string(`con')
        else             gen _cellkey = `geo'
        keep _id _cellkey year
        gen byte   _src   = 1                       // NFIP rows
        gen double _amean = .
        gen double _amed  = .
        gen double _an    = .
        gen long   _ayear = .

        append using `cells'                       // cell rows: geo (+con), year, attom_*
        replace _src = 0 if missing(_src)
        if "`con'" != "" replace _cellkey = `geo' + "|" + string(`con') if _src == 0
        else             replace _cellkey = `geo'                       if _src == 0
        replace _ayear = year                      if _src == 0
        replace _amean = attom_mean_market_total   if _src == 0
        replace _amed  = attom_median_market_total if _src == 0
        replace _an    = attom_n_properties        if _src == 0

        * Carry the most recent cell value (tax year <= policy year) onto NFIP rows.
        * Cells (_src 0) sort before NFIP (_src 1) at equal year -> exact year wins.
        sort _cellkey year _src
        by _cellkey: replace _amean = _amean[_n-1] if missing(_amean)
        by _cellkey: replace _amed  = _amed[_n-1]  if missing(_amed)
        by _cellkey: replace _an    = _an[_n-1]    if missing(_an)
        by _cellkey: replace _ayear = _ayear[_n-1] if missing(_ayear)

        keep if _src == 1 & !missing(_amean)
        keep _id _amean _amed _an _ayear
        tempfile hits
        save `hits'
    restore

    merge 1:1 _id using `hits', keep(master match) nogen
    replace attom_mean_market_total   = _amean if attom_tier == "" & !missing(_amean)
    replace attom_median_market_total = _amed  if attom_tier == "" & !missing(_amean)
    replace attom_n_properties        = _an    if attom_tier == "" & !missing(_amean)
    replace attom_year                = _ayear if attom_tier == "" & !missing(_amean)
    replace attom_tier                = "`tier'" if attom_tier == "" & !missing(_amean)
    drop _amean _amed _an _ayear

    qui count if attom_tier == "`tier'"
    local m = r(N)
    qui count if attom_tier != ""
    di as txt "  tier `tier': matched " as res %9.0fc `m' as txt "  (cumulative " as res %9.0fc r(N) as txt " / " as res %9.0fc _N as txt ")"
end

* -----------------------------------------------------------------------------
* 1. NFIP base + normalized merge keys
* -----------------------------------------------------------------------------
use "`data'/clean/nfip_policies_state/`stl'.dta", clear

capture confirm string variable zipcode
if _rc tostring zipcode, replace format("%05.0f") force
gen zip_key = substr(strtrim(zipcode), 1, 5)     // ATTOM cells use 5-digit ZIP
replace zip_key = "" if inlist(zip_key, ".", "00000")

capture confirm string variable countycode
if _rc tostring countycode, replace format("%05.0f") force
replace countycode = strtrim(countycode)
replace countycode = substr("00000" + countycode, length("00000" + countycode) - 4, 5) ///
    if countycode != "" & countycode != "."

gen construction_5yr    = floor(construction_year / 5)  * 5
gen construction_decade = floor(construction_year / 10) * 10
rename policy_year year

gen long   _id = _n
gen double attom_mean_market_total   = .
gen double attom_median_market_total = .
gen double attom_n_properties        = .
gen long   attom_year                = .
gen str30  attom_tier                = ""

* -----------------------------------------------------------------------------
* 2. Tiered as-of waterfall (finest first; con = "." means pure-geography)
* -----------------------------------------------------------------------------
di as txt "NFIP policy-years (`=upper("`stl'")'): " as res %9.0fc _N
local combined_cells "`data'/build/`stl'_attom_cells_combined.dta"
foreach t in ///
    "zip_key    zip    construction_year   constr_year      zip_construction_year" ///
    "zip_key    zip    construction_5yr    constr_5yr       zip_construction_5yr" ///
    "zip_key    zip    construction_decade constr_decade    zip_construction_decade" ///
    "countycode county construction_year   constr_year      county_construction_year" ///
    "countycode county construction_5yr    constr_5yr       county_construction_5yr" ///
    "countycode county construction_decade constr_decade    county_construction_decade" ///
    "zip_key    zip    .                   year             zip_year" ///
    "countycode county .                   year             county_year" {
    gettoken geo t : t
    gettoken geol t : t
    gettoken con t : t
    gettoken celltier t : t
    gettoken lab t : t
    local conopt = cond("`con'" == ".", "", "con(`con')")
    asoftier using "`combined_cells'", geo(`geo') geol(`geol') ///
        celltier(`celltier') tier(`lab') `conopt'
}
replace attom_tier = "unmatched" if attom_tier == ""
qui count if attom_tier != "unmatched"
di as txt "ATTOM value attached: " as res %9.0fc r(N) as txt " / " as res %9.0fc _N

* -----------------------------------------------------------------------------
* 3. Label, order, save
* -----------------------------------------------------------------------------
drop _id construction_5yr construction_decade

label var zip_key                   "ZIP (5-digit, ATTOM merge key)"
label var attom_mean_market_total   "ATTOM cell mean market value"
label var attom_median_market_total "ATTOM cell median market value"
label var attom_n_properties        "ATTOM properties in matched cell"
label var attom_year                "ATTOM tax year used (as-of policy year)"
label var attom_tier                "ATTOM value-cell match tier"

order property_id state zip_key countycode year construction_year ///
    attom_tier attom_year attom_mean_market_total attom_median_market_total attom_n_properties
sort property_id year
compress
save "`data'/build/`stl'_nfip_attom_wagner.dta", replace
di as result "Saved: `stl'_nfip_attom_wagner.dta"

/******************************************************************************
Authors: Anna Li
Date: 2026-10-02

Description: Share of NFIP-insured SFHA homes (FL LA TX) whose county-year, and whose
    ZIP-year, has a building-permit feed, by state and policy year, for the ATTOM permit
    file and for Builty. One table and one three-panel figure.

******************************************************************************/

* Claude change 10-02: new scratch script, run by hand:
*   do permit_coverage_by_year.do "<data root>" "<output root>"
* Reads the old Builty coverage files that are still in clean/ for the comparison.

args data output

local blue   "23 107 135"
local light  "142 202 230"
local orange "224 122 95"
local opts graphregion(color(white)) plotregion(color(white))

* Insured SFHA homes by county-year and ZIP-year
use state property_id policy_year sfha countycode zipcode using "`data'/build/nfip_hma_panel.dta" ///
    if sfha == 1 & policy_year <= 2025, clear
rename policy_year year

* ZIP-year feeds (ZIP permits in the ATTOM file; Builty's ZIP index undercounts since a third of its permits lack a ZIP)
preserve
    use "`data'/clean/permit_coverage_zip.dta", clear
    rename permit_n_permits attom_zip_permits
    tempfile azip
    save `azip'
    use "`data'/clean/builty_coverage_zip.dta", clear
    rename builty_n_permits builty_zip_permits
    tempfile bzip
    save `bzip'
restore
merge m:1 state zipcode year using `azip', keep(1 3) gen(m_azip)
merge m:1 state zipcode year using `bzip', keep(1 3) gen(m_bzip)

* County-year feeds
merge m:1 countycode year using "`data'/clean/permit_coverage_county.dta", keep(1 3) ///
    keepusing(permit_covered_strict permit_per_100) gen(m_attom)
merge m:1 countycode year using "`data'/clean/builty_coverage_county.dta", keep(1 3) ///
    keepusing(builty_covered_strict builty_per_100) gen(m_builty)

gen attom_any     = m_attom == 3
gen attom_strict  = permit_covered_strict == 1
gen attom_zip     = m_azip == 3
gen builty_any    = m_builty == 3
gen builty_strict = builty_covered_strict == 1
gen builty_zip    = m_bzip == 3
gen both_strict   = attom_strict & builty_strict

collapse (mean) attom_any attom_strict attom_zip builty_any builty_strict builty_zip both_strict ///
    (count) homes = property_id, by(state year)
// within county-years that have an ATTOM feed, the share of homes whose own ZIP shows a permit that year
gen attom_zip_given_county = attom_zip / attom_any

label var homes         "Insured SFHA homes (property-years)"
label var attom_any     "ATTOM: share in county-years with any permit"
label var attom_strict  "ATTOM: share in county-years with >= 1 permit per 100 homes"
label var attom_zip     "ATTOM: share whose ZIP has a permit that year"
label var attom_zip_given_county "ATTOM: ZIP share within covered county-years"
label var builty_any    "Builty: share in county-years with any permit"
label var builty_strict "Builty: share in county-years with >= 1 permit per 100 homes"
label var builty_zip    "Builty: share whose ZIP has a permit that year"
label var both_strict   "Share in county-years strictly covered by both"
order state year homes attom_any attom_strict attom_zip attom_zip_given_county builty_any builty_strict builty_zip both_strict
export excel using "`output'/tables/permit_coverage_by_year.xlsx", firstrow(varlabels) replace
list state year homes attom_strict attom_zip builty_strict if inlist(year, 2009, 2012, 2015, 2018, 2021, 2024), clean noobs

* Figure: one panel per state, strict county coverage and ZIP coverage, ATTOM vs Builty
// one legend for the combined figure (grc1leg, from http://www.stata.com/users/vwiggins)
foreach st in FL LA TX {
    local leg
    twoway (line attom_strict year if state == "`st'", lcolor("`blue'") lwidth(medthick)) ///
           (line attom_zip year if state == "`st'", lcolor("`blue'") lpattern(dash)) ///
           (line builty_strict year if state == "`st'", lcolor("`orange'") lwidth(medthick)) ///
           (line builty_zip year if state == "`st'", lcolor("`orange'") lpattern(dash)), ///
        `opts' subtitle("`st'", size(medium) color(gs4)) ylabel(0(0.2)1, format(%3.1f)) ///
        xlabel(2009(4)2025) xtitle("Policy year") ytitle("Share of insured SFHA homes") ///
        legend(order(1 "ATTOM, county >= 1 permit/100 homes" 2 "ATTOM, own ZIP has a permit" ///
            3 "Builty, county >= 1 permit/100 homes" 4 "Builty, own ZIP has a permit") cols(2) size(small) `leg' pos(6)) ///
        name(g_`st', replace)
}
grc1leg g_FL g_LA g_TX, rows(1) ycommon xsize(12) ysize(4.5) graphregion(color(white)) legendfrom(g_FL) position(6)
graph save   "`output'/figures/permit_coverage_by_year.gph", replace
graph export "`output'/figures/permit_coverage_by_year.png", width(3000) replace

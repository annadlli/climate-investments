/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-01

Description: Prepares the annual CPI deflator used to convert nominal dollar
    amounts (FMA grants, ATTOM property values) to real dollars.
    Deflate with: real = nominal / cpi (cpi = 1 in the base year).

Source: FRED series CPALTT01USM661S (CPI: total, all items, United States)
    https://fred.stlouisfed.org/series/CPALTT01USM661S
******************************************************************************/

args data

* Import CPI data (monthly index)
import delimited "`data'/raw/cpi.csv", clear

* Clean
ren (observation_date cpaltt01usm661s) (date cpi)
gen year = year(date(date, "YMD"))
drop date

* Collapse to an annual average index
collapse (mean) cpi, by(year)

* Rescale to a base year (deflator = 1 in the base year)
local base_year 2023
sum cpi if year == `base_year'
replace cpi = cpi / r(mean)

* Label
label var year "Year"
label var cpi  "CPI deflator (base `base_year' = 1; real = nominal / cpi)"
label data     "Annual CPI deflator, base `base_year'"

* Order, sort, save
order year cpi
sort year
compress
save "`data'/clean/cpi.dta", replace

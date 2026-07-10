/******************************************************************************
Authors: Vendela Norman
Date: 2026-07-10

Description: Prepares the annual CPI deflator used to convert nominal dollar
    amounts to real dollars.

Source: FRED series CPIAUCNS (CPI: total, all items, United States city average)
    https://fred.stlouisfed.org/graph/?id=CPIAUCSL,CPIAUCNS
******************************************************************************/

args data

* Import CPI data 
import delimited "`data'/raw/cpi.csv", clear

* Clean
ren (observation_date cpiaucns) (date cpi)
gen year = year(date(date, "YMD"))
drop date

* Collapse to an annual average
collapse (mean) cpi, by(year)

* Rescale to a base year 
local base_year 2023
sum cpi if year == `base_year'
replace cpi = cpi / r(mean)

* Label
label var year        "Year"
label var cpi         "CPI deflator (`base_year' base)"

* Order, sort, save
compress
save "`data'/clean/cpi.dta", replace

//investment type 
use "/Users/anna/Desktop/climate-investments/data/fire/clean/permits_filtered_further_generous.dta", clear

local out "/Users/anna/Desktop/climate-investments/results/record_type_tables_gen.xlsx"

*--------------------------
* record_type_original
*--------------------------
preserve
contract record_type_original
rename _freq count
gsort -count
egen total = total(count)
gen pct = 100 * count / total
gen cum_pct = sum(pct)
format pct cum_pct %9.2f
drop total
gen rank = _n
order rank record_type_original count pct cum_pct

export excel using "`out'", firstrow(variables) sheet("record_type") replace
restore

*--------------------------
* record_subtype_original
*--------------------------
preserve
contract record_subtype_original
rename _freq count
gsort -count
egen total = total(count)
gen pct = 100 * count / total
gen cum_pct = sum(pct)
format pct cum_pct %9.2f
drop total
gen rank = _n
order rank record_subtype_original count pct cum_pct

export excel using "`out'", firstrow(variables) sheet("record_subtype") sheetmodify
restore

//browse missing investment values
br if missing(job_value) //7502 of 18712 for strict

//categorize investments
gen adaptation_type = ""

replace adaptation_type = "Roofing" if ///
    record_type_original == "Re-Roof, Overlay"

replace adaptation_type = "Fence / Defensible Space" if ///
    record_type_original == "FENCE & WALL"

replace adaptation_type = "Electrical Hardening" if ///
    strpos(record_type_original, "Electrical")

replace adaptation_type = "HVAC / Cooling" if ///
    strpos(record_type_original, "Mechanical")

replace adaptation_type = "Plumbing / Water" if ///
    strpos(record_type_original, "Plumbing")

replace adaptation_type = "Other Adaptation" if ///
    adaptation_type == "" & ///
    inlist(record_type_original, ///
        "Residential", ///
        "Project - Building Permit")

replace adaptation_type = "Non-adaptation / Admin" if ///
    adaptation_type == ""

preserve
contract adaptation_type
rename _freq count
gsort -count
egen total = total(count)
gen pct = 100 * count / total
format pct %9.2f
list adaptation_type count pct
restore

//categorize by description
gen wildfire_cat = "Other / non-wildfire"

* 1) Roofing / structure hardening
replace wildfire_cat = "Roofing / structure hardening" if ///
    regexm(desc, "(^|[^a-z])(re[- ]?roof|reroof|roofing)([^a-z]|$)") ///
    | regexm(desc, "class[ ]*a") ///
    | regexm(desc, "roof(ing)?[ ]*cert")

* 2) Defensible space / perimeter
replace wildfire_cat = "Defensible space / perimeter" if ///
    regexm(desc, "(^|[^a-z])(fence|wall|retaining|grading|clearance|brush|vegetation)([^a-z]|$)")

* 3) Electrical hardening
replace wildfire_cat = "Electrical hardening" if ///
    regexm(desc, "(^|[^a-z])(electrical|panel|service upgrade|meter|rewir|subpanel|breaker)([^a-z]|$)")

* 4) HVAC / smoke resilience
replace wildfire_cat = "HVAC / smoke resilience" if ///
    regexm(desc, "(^|[^a-z])(hvac|mechanical|vent(ilation)?|exhaust|air filter|filtration)([^a-z]|$)")

* 5) Water & fire suppression
replace wildfire_cat = "Water & fire suppression" if ///
    regexm(desc, "(^|[^a-z])(sprinkler|fire[ -]?line|standpipe|hydrant)([^a-z]|$)") ///
    | regexm(desc, "(^|[^a-z])(plumb(ing)?|water line)([^a-z]|$)")

	
tab wildfire_cat

//by category investment summary stats
local inv job_value
local out "/Users/anna/Desktop/climate-investments/results/wildfire_investment_summary_gen.xlsx"

preserve
keep wildfire_cat `inv'
drop if missing(wildfire_cat)

collapse ///
    (count) N = `inv' ///
    (mean)  mean_inv = `inv' ///
    (p50)   med_inv  = `inv' ///
    (p25)   p25_inv  = `inv' ///
    (p75)   p75_inv  = `inv' ///
    (sum)   total_inv = `inv', ///
    by(wildfire_cat)

egen grand_total = total(total_inv)
gen share_total_inv = 100 * total_inv / grand_total

format mean_inv med_inv p25_inv p75_inv total_inv %14.0fc
format share_total_inv %9.2f

gsort -total_inv
order wildfire_cat N mean_inv med_inv p25_inv p75_inv total_inv share_total_inv

list, noobs abbreviate(24)

export excel using "`out'", firstrow(variables) sheet("by_category") replace
restore




//year 
gen permit_date_d = daily(substr(permit_date, 1, 10), "YMD")
format permit_date_d %td
gen permit_year = year(permit_date_d)

///overall year
histogram permit_year, discrete ///
    xtitle("Permit year") ///
    ytitle("Number of permits") ///
    title("Distribution of permit years")
	
//by investment cateogry
preserve
keep if !missing(permit_year)

gen one = 1
collapse (sum) permits = one, by(permit_year wildfire_cat)

twoway ///
    (line permits permit_year if wildfire_cat=="Roofing / structure hardening",  legend(label(1 "Roofing"))) ///
    (line permits permit_year if wildfire_cat=="Defensible space / perimeter",  legend(label(2 "Perimeter"))) ///
    (line permits permit_year if wildfire_cat=="Electrical hardening",          legend(label(3 "Electrical"))) ///
    (line permits permit_year if wildfire_cat=="HVAC / smoke resilience",       legend(label(4 "HVAC"))) ///
    (line permits permit_year if wildfire_cat=="Water & fire suppression",      legend(label(5 "Water"))) ///
    (line permits permit_year if wildfire_cat=="Other / non-wildfire",          legend(label(6 "Other"))), ///
    xtitle("Year") ytitle("Number of permits") ///
    title("Wildfire-proofing permits by category over time")

restore


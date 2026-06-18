clear all

* --------------------------------------------------
* Load data
* --------------------------------------------------
use "/Users/anna/Desktop/climate-investments/data/clean/permits_filtered_all.dta", clear

* --------------------------------------------------
* Clean JOB_VALUE
count if missing(job_value)

drop if missing(job_value)
drop if job_value <= 0

summarize job_value, detail

* --------------------------------------------------
* Convert file_date string -> Stata date
* --------------------------------------------------
gen file_date_clean = substr(file_date, 1, 10)
gen file_date_d = daily(file_date_clean, "YMD")
format file_date_d %td

* sanity check
list file_date file_date_d in 1/10

* --------------------------------------------------
* Extract year (time index)
* --------------------------------------------------
gen year = year(file_date_d)

* --------------------------------------------------
* Aggregate to ZIP–year
* --------------------------------------------------
collapse ///
    (count) permit_count = job_value ///
    (sum)   total_investment = job_value ///
    (mean)  avg_investment = job_value, ///
    by(zipcode year)

* --------------------------------------------------
* Inspect resulting investment measures
* --------------------------------------------------
summarize avg_investment total_investment permit_count

* --------------------------------------------------
* Simple descriptive plots (no imputation yet)
* --------------------------------------------------
histogram avg_investment, bin(10) ///
    title("ZIP–Year Average Permit Investment")


//log scale it
gen ln_avg_inv = ln(avg_investment)
//trim
summ ln_avg_inv, detail
local p99 = r(p99)
histogram ln_avg_inv if ln_avg_inv <= `p99', bin(40) ///
    title("ZIP–Year Average Permit Investment (Log Scale)") ///
    xtitle("ln(average permit investment)")
summ avg_investment, detail



//updated Nov 14: append script
local dest "/Users/anna/Desktop/climate-investments/data/fire/process"
local final_dest "/Users/anna/Desktop/climate-investments/data/fire/clean"

local files : dir "`dest'" files "*_filtered.dta"
local nfiles : word count `files'

if (`nfiles' == 0) {
    di as error "No *_filtered.dta files found in `dest'."
    exit
}

di as result "Found `nfiles' filtered .dta files."
local first : word 1 of `files'

//first dataset
use "`dest'/`first'", clear
di as result "Loaded first dataset: `first'"

//append rest
forvalues i = 2/`nfiles' {
    local f : word `i' of `files'
    di as txt "----- Appending file `i' of `nfiles': `f' -----"

    quietly capture append using "`dest'/`f'"

    // Handle type mismatches
    if _rc {
        di as error "Type mismatch in `f' — retrying with force."
        capture append using "`dest'/`f'", force
    }
}

//save
save "`final_dest'/permits_filtered_all.dta", replace
di as result " Combined dataset saved → `final_dest'/permits_filtered_all.dta"
//98959 obs

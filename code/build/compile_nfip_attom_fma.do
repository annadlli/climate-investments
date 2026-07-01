/******************************************************************************
Authors: Anna Li
Date: 2026-07-01

Description:
    Appends state-level NFIP+ATTOM+FMA property files into a single
    national analysis dataset.

Inputs:  `data'/build/{state}_nfip_attom_fma_property.dta
Output:  `data'/build/nfip_attom_fma_property.dta
******************************************************************************/

args data states

clear
tempfile combined
save `combined', emptyok replace

foreach st of local states {
    local stl = strlower("`st'")
    local f = "`data'/build/`stl'_nfip_attom_fma_property.dta"
    if fileexists("`f'") {
        append using "`f'"
        di as result "Appended: `st'"
    }
    else {
        di as error "File not found, skipping: `f'"
    }
}

compress
save "`data'/analysis/compiled_nfip_attom_fma_property.dta", replace
di as result "Done. `=_N' observations across `=wordcount("`states'")' states."

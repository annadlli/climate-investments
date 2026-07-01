/******************************************************************************
Authors: Anna Li
Date: 2026-07-01

Description:
    Appends state-level NFIP+ATTOM+FMA property files into a single
    national analysis dataset.
******************************************************************************/
args data states

* Start with the first requested state, then append each additional state.
local first = 1
foreach st of local states {
    local stl = strlower("`st'")
    local f "`data'/build/`stl'_nfip_attom_fma_property.dta"

    * Load the first state file, then append subsequent state files.
    if `first' == 1 {
        use "`f'", clear
        local first = 0
    }
    else {
        append using "`f'"
    }
}

* Save the combined property-level analysis dataset.
compress
save "`data'/analysis/compiled_analysis_propertylevel.dta", replace
di as result "Done. `=_N' observations across `=wordcount("`states'")' states."

/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-06-05

Description: Runs the data-construction pipeline for the climate-investments
    project (clean -> build). Toggle the switches in Section 1, then run.
    Analysis and descriptives are run separately (see code/analysis and descriptives/).

Notes: Set the two roots below per machine -- `code' (this git repo) and `data'
    (Dropbox). Every path derives from them; nothing else is machine-specific.
    Raw ATTOM/BUILTY parquet are licensed Dewey downloads; fill the
    placeholder Dewey endpoints in clean/import_dewey.py before running.
******************************************************************************/

version 18
clear all
set more off

* -----------------------------------------------------------------------------
* Paths  
* -----------------------------------------------------------------------------
* --- Vendela ---
local code "/Users/vendelasolvindnorman/Documents/Econ_PhD/Projects/climate-investments/code"
local data "/Users/vendelasolvindnorman/Library/CloudStorage/Dropbox/Flooding/Data"

* --- Anna ---
/* local code "/Users/anna/Desktop/Research/climate-investments/code"
local data "/Users/anna/Desktop/Research/climate-investments/data" */

* Machine-specific: full path to a python with duckdb/pandas. Stata's GUI shell PATH
* does NOT include conda, so a bare "python3" resolves to system python (no duckdb).
local python "/Users/vendelasolvindnorman/anaconda3/bin/python3"
* --- Anna --- local python "python3"

* -----------------------------------------------------------------------------
* Section 1: Set switches  (1 = run, 0 = skip)
* -----------------------------------------------------------------------------

// State scope
local states "TX VA"

// i) Clean 
local import_dewey         = 0
local import_nfip_policies = 1   
local clean_fma            = 0
local clean_nfip_claims    = 0   // PENDING: not sure how/if we will use this data, so pausing cleanup here
local clean_nfip_policies  = 0

// ii) Build 
local filter_builty        = 0
local split_states         = 0
local match_attom          = 0
local merge_fma            = 0
local make_dta             = 0
local build_nfip           = 0    // see header: needs reconciling with clean_nfip_claims.do
local build_panels         = 0

// iii) Descriptives
local compare_builty_hmgp_coverage = 0

* -----------------------------------------------------------------------------
* Section 2: Run code    
* -----------------------------------------------------------------------------

// i) Clean
if `import_dewey' == 1 {
    shell `python' "`code'/clean/import_dewey.py" --data "`data'"
}
if `import_nfip_policies' == 1 {
    shell `python' "`code'/clean/import_nfip_policies.py" --data "`data'" --states "`states'"
}
if `clean_fma' == 1 {
    do "`code'/clean/clean_fma.do" "`data'"
}
if `clean_nfip_claims' == 1 {
    do "`code'/clean/clean_nfip_claims.do" "`data'"
}
if `clean_nfip_policies' == 1 {
    do "`code'/clean/clean_nfip_policies.do" "`data'"
}

// ii) Build
if `filter_builty' == 1 {
    shell `python' "`code'/build/build_builty_filter.py" --data "`data'"
}
if `split_states' == 1 {
    shell `python' "`code'/build/build_split_builty_states.py" ///
        --data "`data'" ///
        --states `states'
}
if `match_attom' == 1 {
    foreach state of local states {
        shell `python' "`code'/build/build_attom_onto_permits.py" --data "`data'" --state "`state'"
    }
} //run with TORCH due to size, not locally

if `merge_fma' == 1 {
    foreach state of local states {
        shell `python' "`code'/build/build_fma_onto_builty_attom.py" --data "`data'" --state "`state'"
    }
}
if `make_dta' == 1 {
    foreach state of local states {
        shell `python' "`code'/build/parquetdta.py" --state "`state'" --data "`data'"
    }
}
if `build_nfip' == 1 {
    do "`code'/build/nfip_build.do" "`data'"
}
if `build_panels' == 1 {
    do "`code'/build/build_nfip_hma_panels.do" "`data'"
}

// iii) Descriptives
if `compare_builty_hmgp_coverage' == 1 {
    shell `python' "`code'/../descriptives/compare_builty_hmgp_coverage.py" --data "`data'"
}

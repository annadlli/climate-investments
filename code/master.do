/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-06-05

Description: Runs the data-construction pipeline for the climate-investments
    project (clean -> build). Toggle the switches in Section 1, then run.
    Analysis and descriptives are run separately (see code/analysis, code/descriptives).

Notes: Set the two roots below per machine -- `code' (this git repo) and `data'
    (Dropbox). Every path derives from them; nothing else is machine-specific.
    Raw ATTOM/BUILTY parquet are created upstream from the Dewey API (torch_work/).
    filter_builty_strict.py and attom_onto_permits.py are pending Anna's revised
    versions (lost work) -- those steps will not run end-to-end yet.
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

local python "python3"

* -----------------------------------------------------------------------------
* Section 1: Set switches  (1 = run, 0 = skip)
* -----------------------------------------------------------------------------

// i) Clean 
local clean_hma            = 0
local clean_nfip_claims    = 1
local clean_nfip_policies  = 0

// ii) Build 
local filter_builty        = 0    // PENDING Anna's revised filter_builty_strict.py
local split_states         = 0
local match_attom          = 0    // PENDING Anna's revised attom_onto_permits.py
local make_dta             = 0
local build_nfip           = 0    // see header: needs reconciling with clean_nfip_claims.do
local build_panels         = 0

* -----------------------------------------------------------------------------
* Section 2: Run code    
* -----------------------------------------------------------------------------

// i) Clean
if `clean_hma' == 1 {
    do "`code'/clean/clean_hma.do" "`data'"
}
if `clean_nfip_claims' == 1 {
    do "`code'/clean/clean_nfip_claims.do" "`data'"
}
if `clean_nfip_policies' == 1 {
    do "`code'/clean/clean_nfip_policies.do" "`data'"
}

// ii) Build
if `filter_builty' == 1 {
    shell `python' "`code'/build/filter_builty_strict.py"
}
if `split_states' == 1 {
    shell `python' "`code'/build/split_builty_states.py" ///
        --input "`data'/clean/all_elevation.parquet" ///
        --out-dir "`data'/build" ///
        --states TX VA
}
if `match_attom' == 1 {
    shell `python' "`code'/build/attom_onto_permits.py" --state TX
    shell `python' "`code'/build/attom_onto_permits.py" --state VA
}
if `make_dta' == 1 {
    shell `python' "`code'/build/parquetdta.py" --state TX --data "`data'"
    shell `python' "`code'/build/parquetdta.py" --state VA --data "`data'"
}
if `build_nfip' == 1 {
    do "`code'/build/nfip_build.do" "`data'"
}
if `build_panels' == 1 {
    do "`code'/build/build_nfip_hma_panels.do" "`data'"
}

// iii) Analysis (TBD)

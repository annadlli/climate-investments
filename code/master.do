/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-06-25

Description: Runs the data-construction pipeline for the climate-investments
    project.

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
local python "/Users/vendelasolvindnorman/anaconda3/bin/python3"

* --- Anna ---
/* local code "/Users/anna/Desktop/Research/climate-investments/code"
local data "/Users/anna/Desktop/Research/climate-investments/data" 
local python "python3" */ 

* -----------------------------------------------------------------------------
* Section 1: Set switches 
* -----------------------------------------------------------------------------

// State scope
local states "TX VA"

// Dewey/ATTOM acquisition inputs. The manifest is private because it contains
// licensed Dewey endpoint URLs. compile_attom_batches requires an existing run id.
local dewey_manifest "`code'/../anna_private/dewey_manifest_wagner_template.csv"
local dewey_run_id ""

// i) Clean 
local import_dewey             = 0 // import Attom and Builty data from Dewey
local import_nfip_policies     = 0 // import NFIP policies data 
local clean_fma                = 0 // clean FEMA FMA data
local clean_nfip_policies      = 1 // clean NFIP policies data
local clean_nfip_multiple_loss = 0 // clean NFIP multiple-loss data
local collapse_nfip_policies   = 1 // collapse NFIP policy data to property level

// ii) Build
local compile_attom_batches    = 0 // compile raw Dewey ATTOM batch pulls to per-state parquet
local build_attom_values       = 0
local compile                  = 0

* -----------------------------------------------------------------------------
* Section 2: Run code    
* -----------------------------------------------------------------------------

// i) Clean
if `import_dewey' == 1 {
    local dewey_run_opt ""
    if "`dewey_run_id'" != "" {
        local dewey_run_opt "--run-id `dewey_run_id'"
    }
    shell `python' "`code'/clean/import_dewey.py" ///
        --data "`data'" ///
        --manifest "`dewey_manifest'" ///
        `dewey_run_opt'
}
if `import_nfip_policies' == 1 {
    shell `python' "`code'/clean/import_nfip_policies.py" --data "`data'" --states "`states'"
}
if `clean_fma' == 1 {
    do "`code'/clean/clean_fma.do" "`data'"
}
if `clean_nfip_policies' == 1 {
    do "`code'/clean/clean_nfip_policies.do" "`data'" "`states'"
}
if `clean_nfip_multiple_loss' == 1 {
    do "`code'/clean/clean_nfip_multiple_loss.do" "`data'"
}
if `collapse_nfip_policies' == 1 {
    do "`code'/clean/collapse_nfip_policies.do" "`data'" "`states'"
}

// ii) Build
if `compile_attom_batches' == 1 {
    shell `python' "`code'/clean/compile_attom_batches.py" ///
        --data "`data'" ///
        --manifest "`dewey_manifest'" ///
        --run-id "`dewey_run_id'"
}
if `build_attom_values' == 1 { //run with TORCH due to size, not locally
    foreach state of local states {
        shell `python' "`code'/build/build_attom_value_cells.py" --data "`data'" --state "`state'"
    }
}
if `compile' == 1 {
    do "`code'/build/compile.do" "`data'" "`states'"
}

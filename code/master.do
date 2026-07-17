/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-07-16

Description: Runs the data-construction pipeline for the climate-investments
    project.

******************************************************************************/

version 18
clear all
set more off

* -----------------------------------------------------------------------------
* Locals
* -----------------------------------------------------------------------------

// States 
local states "AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA"

// Dewey/ATTOM acquisition inputs. The manifest is private because it contains
// licensed Dewey endpoint URLs. compile_attom_batches requires an existing run id.
local dewey_manifest "`code'/../anna_private/dewey_manifest_wagner_template.csv"
local dewey_run_id ""

* -----------------------------------------------------------------------------
* Paths 
* -----------------------------------------------------------------------------

* --- Vendela ---
local code "/Users/vendelasolvindnorman/Documents/Econ_PhD/Projects/climate-investments/code"
local data "/Users/vendelasolvindnorman/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
local python "/Users/vendelasolvindnorman/anaconda3/bin/python3"

/* * --- Anna ---
 local code "/Users/anna/Desktop/climate-investments/code"
local data "/Users/anna/Desktop/climate-investments/data" 
local python "python3"  */

* -----------------------------------------------------------------------------
* Section 1: Set switches 
* -----------------------------------------------------------------------------

// i) Clean
local import_dewey             = 0 // import Attom and Builty data from Dewey
local extract_nfip_policies    = 0 // extract per-state NFIP policies
local extract_builty           = 0 // extract per-state Builty elevation-candidate permits
local crosswalks               = 0 // create geographic crosswalks
local clean_cpi                = 0 // clean CPI deflator data
local clean_fma                = 0 // clean FEMA FMA data
local clean_builty             = 1 // clean Builty permits data
local clean_nfip_policies      = 0 // clean NFIP policies data
local clean_nfip_multiple_loss = 0 // clean NFIP multiple-loss data

// ii) Build
local prep_fma                 = 0 // collapse FMA across years to zip/county level
local prep_nfip_policies       = 0 // collapse NFIP policy data to property level
local compile                  = 1 // compile property-level analysis dataset


local compile_attom_batches    = 0 // compile raw Dewey ATTOM batch pulls to per-state parquet
local build_attom_values       = 0 //generate attom state summary files
local build_nfip_attom_fma     = 0 // build property-level analysis dataset state level
local compile_property         = 0 // compile property-level analysis datasets

* -----------------------------------------------------------------------------
* Section 2: Run code    
* -----------------------------------------------------------------------------

do "`code'/clean/archive/clean_fma_projects.do" "`data'"

// i) Clean
if `import_dewey' == 1 {
    shell `python' "`code'/clean/import_dewey.py" ///
        --data "`data'" ///
        --manifest "`dewey_manifest'" ///
        --run-id "`dewey_run_id'"
}

if `extract_nfip_policies' == 1 {
    shell `python' "`code'/clean/extract_nfip_policies.py" --data "`data'" --states "`states'"
}
if `extract_builty' == 1 {
    shell `python' "`code'/clean/extract_builty.py" --data "`data'" --states "`states'"
}
if `crosswalks' == 1 {
    do "`code'/clean/crosswalks.do" "`data'"
}
if `clean_cpi' == 1 {
    do "`code'/clean/clean_cpi.do" "`data'"
}
if `clean_fma' == 1 {
    do "`code'/clean/clean_fma.do" "`data'"
}
if `clean_builty' == 1 {
    do "`code'/clean/clean_builty.do" "`data'" "`states'"
}
if `clean_nfip_policies' == 1 {
    do "`code'/clean/clean_nfip_policies.do" "`data'" "`states'"
}
if `clean_nfip_multiple_loss' == 1 {
    do "`code'/clean/clean_nfip_multiple_loss.do" "`data'"
}

// ii) Build
if `prep_fma' == 1 {
    do "`code'/build/prep_fma.do" "`data'"
}
if `prep_nfip_policies' == 1 {
    do "`code'/build/prep_nfip_policies.do" "`data'" "`states'"
}
if `compile' == 1 {
    do "`code'/build/compile.do" "`data'" "`states'"
}



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
if `build_nfip_attom_fma' == 1 {
    foreach state of local states {
        do "`code'/build/build_property_panel.do" "`data'" "`state'"
    }
}
if `compile_property' == 1 {
    do "`code'/build/compile_nfip_attom_fma.do" "`data'" "`states'"
}


/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-02

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
local data "/Users/vendelasolvindnorman/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
local python "/Users/vendelasolvindnorman/anaconda3/bin/python3"

* --- Anna ---
/* local code "/Users/anna/Desktop/climate-investments/code"
local data "/Users/anna/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
local python "/opt/anaconda3/bin/python" */

* --- Derived (same for everyone) ---
local output "`code'/../output" // repo-root sibling of code/: tables/ and figures/

* -----------------------------------------------------------------------------
* Locals
* -----------------------------------------------------------------------------

// States 
local states "AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA"

// Dewey/ATTOM acquisition inputs. The manifest is private because it contains
// licensed Dewey endpoint URLs. extract_attom requires an existing run id.
local dewey_manifest "`code'/../anna_private/dewey_manifest_wagner_template.csv"
local dewey_run_id ""

* -----------------------------------------------------------------------------
* Section 1: Set switches 
* -----------------------------------------------------------------------------

// i) Prepare
local import_dewey                      = 0 // import Attom and Builty data from Dewey
local extract_nfip_policies             = 0 // extract per-state NFIP policies
local extract_builty                    = 0 // extract per-state Builty elevation-candidate permits
local extract_attom                     = 0 // extract per-state ATTOM property data
local geocode_attom                     = 0 // geocode ATTOM addresses to fill Census block groups

// ii) Clean
local crosswalks                        = 0 // create geographic crosswalks
local clean_cpi                         = 0 // clean CPI deflator data
local clean_fma                         = 0 // clean FEMA FMA data
local clean_builty                      = 0 // clean Builty permits data
local clean_nfip_policies               = 1 // clean NFIP policies data
local clean_nfip_claims                 = 0 // clean NFIP claims data
local clean_nfip_multiple_loss          = 0 // clean NFIP multiple-loss data

// iii) Build (Vendela -- to be harmonized)
local prep_fma                          = 0 // collapse FMA across years to zip/county level
local prep_nfip_policies                = 1 // append NFIP policy data; collapse to property level
local compile                           = 0 // compile property-level analysis dataset
local complete                          = 0 // prepares final analysis dataset

// iv) Build (Anna)
local geocode_builty                    = 0 // geocode Builty to fill missing ZIP codes
local merge_datasets                    = 0 // runs all of the torch scripts except geocode_attom.
    // local attom_geocode                  = 0 // merge geocoded Census block group to full ATTOM property records
    // local attom_nfhl                     = 0 // merge Attom w/ NFHL flood zone data
    // local attom_builty                   = 0 // merge Attom w/ Builty 
    // local nfip_attom                     = 0 // merge ATTOM with NFIP using the matching tiers
local parquet_dta                       = 0 // convert parquet file to Stata
local final_data                        = 0 // merge results onto analysis.dta to create final two versions of the dataset

// v) Descriptives
local summary_stats                     = 0 // create summary statistics table

* -----------------------------------------------------------------------------
* Section 2: Run code    
* -----------------------------------------------------------------------------

// i) Prepare
if `import_dewey' == 1 {
    shell `python' "`code'/prepare/import_dewey.py" ///
        --data "`data'" ///
        --manifest "`dewey_manifest'" ///
        --run-id "`dewey_run_id'"
}
if `extract_nfip_policies' == 1 {
    shell `python' "`code'/prepare/extract_nfip_policies.py" --data "`data'" --states "`states'"
}
if `extract_builty' == 1 {
    shell `python' "`code'/prepare/extract_builty.py" --data "`data'" --states "`states'"
}
if `extract_attom' == 1 {
    shell `python' "`code'/prepare/extract_attom.py" ///
        --data "`data'" ///
        --manifest "`dewey_manifest'" ///
        --run-id "`dewey_run_id'"
}
if `geocode_attom' == 1 { // run with TORCH: network-bound Census geocode, resume-safe
    foreach state of local states {
        shell `python' "`code'/prepare/geocode_attom.py" --data "`data'" --state "`state'"
    }
}

// ii) Clean
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
if `clean_nfip_claims' == 1 {
    do "`code'/clean/clean_nfip_claims.do" "`data'" "`states'"
}
if `clean_nfip_multiple_loss' == 1 {
    do "`code'/clean/clean_nfip_multiple_loss.do" "`data'"
}

// iii) Build
if `prep_fma' == 1 {
    do "`code'/build/prep_fma.do" "`data'"
}
if `prep_nfip_policies' == 1 {
    do "`code'/build/prep_nfip_policies.do" "`data'" "`states'"
}
if `compile' == 1 {
    do "`code'/build/compile.do" "`data'" "`states'"
}
if `complete' == 1 {
    do "`code'/build/complete.do" "`data'"
}

// iv) Build (Anna)
if `geocode_builty' == 1 {
    shell `python' "`code'/build/geocode_builty.py" --data "`data'"
}
if `merge_datasets' == 1 {
    foreach state of local states {
        shell bash "`code'/slurm/run_property_matching.sh" ///
            --state "`state'" ///
            --data "`data'" ///
            --python "`python'" ///
            --memory "24GB" ///
            --threads 4
    }
}
if `parquet_dta' == 1 {
    foreach state of local states {
        local st = lower("`state'")
        shell `python' "`code'/build/parquet_dta.py" ///
            --input "`data'/build/nfip_attom_pipeline_v2/nfip_attom_property/`st'_nfip_attom_property.parquet" ///
            --output "`data'/build/nfip_attom_property/`st'_nfip_attom_property.dta"
    }
}
if `final_data' == 1 {
    do "`code'/build/final_data.do" "`data'" "`states'"
}

// v) Descriptives
if `summary_stats' == 1 {
    do "`code'/descriptives/summary_table.do" "`data'" "`output'"
}

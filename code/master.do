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
* Locals
* -----------------------------------------------------------------------------

// States 
local states "AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA"

// Dewey/ATTOM acquisition inputs. The manifest is private because it contains
// licensed Dewey endpoint URLs. extract_attom requires an existing run id.
local dewey_manifest "`code'/../anna_private/dewey_manifest_wagner_template.csv"
local dewey_run_id ""

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

* -----------------------------------------------------------------------------
* Section 1: Set switches 
* -----------------------------------------------------------------------------

// i) Clean
local import_dewey             = 0 // import Attom and Builty data from Dewey
local extract_nfip_policies    = 0 // extract per-state NFIP policies
local extract_builty           = 0 // extract per-state Builty elevation-candidate permits
local extract_attom            = 0 // extract per-state ATTOM property data
local crosswalks               = 0 // create geographic crosswalks
local clean_cpi                = 0 // clean CPI deflator data
local clean_fma                = 0 // clean FEMA FMA data
local clean_builty             = 0 // clean Builty permits data
local clean_nfip_policies      = 0 // clean NFIP policies data
local clean_nfip_claims        = 0 // clean NFIP claims data
local clean_nfip_multiple_loss = 0 // clean NFIP multiple-loss data

// ii) Build
local prep_fma                 = 0 // collapse FMA across years to zip/county level
local prep_nfip_policies       = 0 // collapse NFIP policy data to property level
local compile                  = 0 // compile property-level analysis dataset
local complete                 = 1 // prepares final analysis dataset


// iii) Build: finalized Builty -> ATTOM -> NFIP property pipeline
local geocode_attom            = 0 // extract ATTOM addresses + Census geocode to block groups
local run_matching             = 0 // finalized 5-step Builty/ATTOM/NFIP matching, per state
local compile_stable_property  = 0 // matching links -> clean analysis file

* -----------------------------------------------------------------------------
* Section 2: Run code    
* -----------------------------------------------------------------------------

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
if `extract_attom' == 1 {
    shell `python' "`code'/clean/extract_attom.py" ///
        --data "`data'" ///
        --manifest "`dewey_manifest'" ///
        --run-id "`dewey_run_id'"
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
if `clean_nfip_claims' == 1 {
    do "`code'/clean/clean_nfip_claims.do" "`data'" "`states'"
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
if `complete' == 1 {
    do "`code'/build/complete.do" "`data'"
}

if `geocode_attom' == 1 { // run with TORCH: network-bound Census geocode, resume-safe
    foreach state of local states {
        local st = lower("`state'")
        shell mkdir -p "`data'/build/attom_geocode/`st'_addr/chunks" ///
            "`data'/build/attom_geocode/`st'_addr/results" ///
            "`data'/build/attom_geocode/`st'_addr/duckdb_tmp"
        shell `python' "`code'/build/geocode_attom.py" --data "`data'" --state "`state'"
    }
}
if `run_matching' == 1 {
    shell mkdir -p "`data'/build/nfip_attom_pipeline_v2/geocoded" ///
        "`data'/build/nfip_attom_pipeline_v2/nfhl_matches" ///
        "`data'/build/nfip_attom_pipeline_v2/builty_attom" ///
        "`data'/build/nfip_attom_pipeline_v2/attom_nfhl_builty" ///
        "`data'/build/nfip_attom_pipeline_v2/nfip_attom_property"
    foreach state of local states {
        local st = lower("`state'")
        shell mkdir -p "`data'/build/nfip_attom_pipeline_v2/tmp/`st'/geocoded" ///
            "`data'/build/nfip_attom_pipeline_v2/tmp/`st'/builty" ///
            "`data'/build/nfip_attom_pipeline_v2/tmp/`st'/assignment"
        shell bash "`code'/slurm/run_property_matching.sh" ///
            --state "`state'" --data "`data'" --python "`python'" ///
            --memory "24GB" --threads 4
    }
}
if `compile_stable_property' == 1 {
    shell mkdir -p "`data'/build/nfip_attom_property"
    foreach state of local states {
        local st = lower("`state'")
        shell `python' "`code'/build/finalize_nfip_attom_property.py" ///
            --input "`data'/build/nfip_attom_pipeline_v2/nfip_attom_property/`st'_nfip_attom_property.parquet" ///
            --output "`data'/build/nfip_attom_property/`st'_nfip_attom_property.dta"
    }
    do "`code'/build/compile_nfip_property_attom.do" "`data'" "`states'"
}

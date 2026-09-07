/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-02

Description: Runs the data-construction pipeline for the climate-investments
    project.

******************************************************************************/

version 18
clear all
set more off
set seed 20260903

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
local output "`code'/../output" 

* -----------------------------------------------------------------------------
* Locals
* -----------------------------------------------------------------------------

// States 
// narrowed it down here to the new four states
local states "FL LA NJ TX"

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
local geocode_builty                    = 0 // geocode cleaned Builty data to fill in missing ZIP codes
local clean_builty_coverage             = 0 // prepare Builty permit coverage flag
local clean_nfip_policies               = 0 // clean NFIP policies data
local clean_nfip_claims                 = 0 // clean NFIP claims data
local clean_nfip_multiple_loss          = 0 // clean NFIP multiple-loss data

// iii) Build 
local prep_fma                          = 0 // collapse FMA across years to zip/county level
local prep_nfip_policies                = 0 // append NFIP policy data across states; collapse to property level for the ATTOM match
local merge_nfip_fma                    = 0 // merge NFIP policies, claims, multiple-loss data & HMA data
local merge_datasets                    = 0 // runs all of the torch scripts except geocode_attom.
    // local attom_geocode                  = 0 // merge geocoded Census block group to full ATTOM property records
    // local attom_nfhl                     = 0 // merge Attom w/ NFHL flood zone data
    // local attom_builty                   = 0 // merge Attom w/ Builty 
    // local nfip_attom                     = 0 // merge ATTOM with NFIP using the matching tiers
local parquet_dta                       = 0 // convert parquet file to Stata
local attom_value_wide                  = 0 // ATTOM value by tax year, one row per property, 2023 $ (added 2026-09-06)
local attom_value_dta                   = 0 // Stata copy of the value file, restricted to ATTOM properties linked to NFIP (after parquet_dta)
local complete                          = 0 // compile final analysis dataset 

// v) Descriptives
local summary_stats                     = 0 // create summary statistics table
local elevations_by_state               = 0 // count elevation retrofits by state in Builty and HMA (deck tab)

// vi) Analysis
local empirical_facts                   = 0 // create empirical-facts figures and tables

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
if `geocode_builty' == 1 {
    shell `python' "`code'/clean/geocode_builty.py" --data "`data'"
}
if `clean_builty_coverage' == 1 {
    shell `python' "`code'/clean/clean_builty_coverage.py" --data "`data'" --states "`states'"
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
if `merge_nfip_fma' == 1 {
    do "`code'/build/merge_nfip_fma.do" "`data'"
}
if `complete' == 1 {
    do "`code'/build/complete.do" "`data'" "`states'"
}

// iv) Build (Anna)
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
if `attom_value_wide' == 1 {
    shell `python' "`code'/build/attom_value_wide.py" --data "`data'" --states "`states'"
}
if `attom_value_dta' == 1 {
    // 09-07: run after parquet_dta. Keeps only ATTOM IDs that the matcher assigned to an NFIP property, so the .dta is ~sig smaller than state-wide parquet file
    foreach state of local states {
        local st = lower("`state'")
        shell `python' "`code'/build/parquet_dta.py" ///
            --input "`data'/build/attom_value_wide/`st'_attom_value_wide.parquet" ///
            --output "`data'/build/attom_value_wide/`st'_attom_value_wide.dta" ///
            --where "attomid IN (SELECT assigned_attomid FROM read_parquet('`data'/build/nfip_attom_pipeline_v2/nfip_attom_property/`st'_nfip_attom_property.parquet') WHERE assigned_attomid IS NOT NULL)"
    }
}

// v) Descriptives
if `summary_stats' == 1 {
    do "`code'/descriptives/summary_table.do" "`data'" "`output'"
}
if `elevations_by_state' == 1 {
    do "`code'/descriptives/elevations_by_state.do" "`data'" "`output'"
}

// vi) Analysis
if `empirical_facts' == 1 {
    do "`code'/analysis/empirical_facts_figures.do" "`data'" "`output'"
}

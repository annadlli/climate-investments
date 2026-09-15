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

if "`c(username)'" == "anna" {
    local code "/Users/anna/Desktop/climate-investments/code"
    local data "/Users/anna/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
    local python "/opt/anaconda3/bin/python"
}
else {
    local code "/Users/vendelasolvindnorman/Documents/Econ_PhD/Projects/climate-investments/code"
    local data "/Users/vendelasolvindnorman/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
    local python "/Users/vendelasolvindnorman/anaconda3/bin/python3"
}

* --- Derived (same for everyone) ---
local output "`code'/../output" 

* -----------------------------------------------------------------------------
* Locals
* -----------------------------------------------------------------------------

// States 
// NJ dropped for lack of Builty
// coverage. The 20-state list is AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA
// RI SC TX VT VA; every script takes `states' as an argument, so widening is
// a one-line change here.
local states "FL LA TX"

// Matching driver options (merge_datasets): rerun from this step where outputs exists 09-13
local matching_from "3"
local matching_memory "8GB"
local matching_tmp "`code'/../tmp/matching"

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
local clean_builty_coverage_rate        = 0 // Builty permits per 100 ATTOM SF properties; strict coverage flag (09-07)
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
local attom_stata                       = 0 // ATTOM link file + long value file for complete.do (Claude change 09-15: replaces parquet_dta, attom_value_wide, attom_value_dta)
local complete                          = 0 // compile final analysis dataset 

// v) Descriptives
local summary_stats                     = 0 // create summary statistics table
local histograms                        = 0 // histograms of cumulative claims and elevation project cost
local elevations_by_state               = 0 // count elevation retrofits by state in Builty and HMA (deck tab)
local builty_coverage_table             = 0 // Builty permit coverage by state for the deck 
local empirical_facts                   = 0 // create empirical-facts figures and tables

// vi) Analysis
local es_prices_mitigation              = 0 // elevation discount + event studies: prices do not reward mitigation

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
if `clean_builty_coverage_rate' == 1 {
    shell `python' "`code'/clean/clean_builty_coverage_rate.py" --data "`data'" --states "`states'"
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

// iv) Build (Anna)
if `merge_datasets' == 1 {
    foreach state of local states {
        shell bash "`code'/slurm/run_property_matching.sh" ///
            --state "`state'" ///
            --data "`data'" ///
            --python "`python'" ///
            --memory "`matching_memory'" ///
            --threads 4 ///
            --from "`matching_from'" ///
            --tmp "`matching_tmp'/`state'" 
    }
}
if `attom_stata' == 1 {
    // Claude change 09-15: one link file (NFIP property -> ATTOM ID, match tier, Builty flags) and
    // one long value file (assigned ATTOM ID x year, 2023 $, panel years) from the matcher output
    // and the geocoded panels; complete.do merges each once
    shell `python' "`code'/build/attom_stata.py" --data "`data'" --states "`states'"
}
if `complete' == 1 {
    do "`code'/build/complete.do" "`data'" "`states'"
}

// v) Descriptives
if `summary_stats' == 1 {
    do "`code'/descriptives/summary_table.do" "`data'" "`output'"
}
if `histograms' == 1 {
    do "`code'/descriptives/histograms.do" "`data'" "`output'"
}
if `elevations_by_state' == 1 {
    do "`code'/descriptives/scratch/elevations_by_state.do" "`data'" "`output'"
}
if `builty_coverage_table' == 1 {
    do "`code'/descriptives/scratch/builty_coverage_table.do" "`data'" "`output'"
}
if `empirical_facts' == 1 {
    do "`code'/descriptives/scratch/empirical_facts_figures.do" "`data'" "`output'"
}

// vi) Analysis
if `es_prices_mitigation' == 1 {
    do "`code'/analysis/es_prices_mitigation.do" "`data'" "`output'"
}

/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-09-15

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

// States: the panel is built for this list, so a change means rerunning from prep_nfip_policies
local states "FL LA TX"

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
local clean_hma                         = 0 // clean FEMA HMA elevation grants (all programs)
local clean_builty                      = 0 // clean Builty permits data
local geocode_builty                    = 0 // geocode cleaned Builty data to fill in missing ZIP codes
local clean_builty_coverage             = 0 // Builty permit counts by county/ZIP/locality x year; coverage flags (needs geocoded ATTOM)
local clean_nfip_policies               = 0 // clean NFIP policies data
local clean_nfip_claims                 = 0 // clean NFIP claims data
local clean_nfip_multiple_loss          = 0 // clean NFIP multiple-loss data

// iii) Build 
local prep_hma                          = 0 // collapse HMA grants across years to ZIP and county
local prep_nfip_policies                = 0 // append NFIP policy data across states; collapse to property level for the ATTOM match
local merge_nfip_hma                    = 0 // merge NFIP policies, claims, multiple-loss data & HMA data
local merge_datasets                    = 0 // runs all of the torch scripts except geocode_attom.
    // local attom_geocode                  = 0 // merge geocoded Census block group to full ATTOM property records
    // local attom_nfhl                     = 0 // merge Attom w/ NFHL flood zone data
    // local attom_builty                   = 0 // merge Attom w/ Builty 
    // local nfip_attom                     = 0 // merge ATTOM with NFIP using the matching tiers
local parquet_dta                       = 0 // convert parquet file to Stata
local attom_value                       = 0 // ATTOM market value for matched properties, every year, 2023 $ (after parquet_dta)
local attom_value_dta                   = 0 // one Stata copy of the value files
local complete                          = 0 // compile final analysis dataset 

// iv) Descriptives
local summary_table                     = 0 // create summary statistics table
local histograms                        = 1 // histograms of cumulative claims and elevation project cost

// v) Analysis
local es_elevation                      = 0 // event studies of premium and claims around the elevation permit, pooled and by source
local same_flood_claims                 = 0 // claims of permitted homes before and after the permit vs never-elevated homes in the same flood; cost effectiveness

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
if `geocode_attom' == 1 { 
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
if `clean_hma' == 1 {
    do "`code'/clean/clean_hma.do" "`data'"
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
if `prep_hma' == 1 {
    do "`code'/build/prep_hma.do" "`data'"
}
if `prep_nfip_policies' == 1 {
    do "`code'/build/prep_nfip_policies.do" "`data'" "`states'"
}
if `merge_nfip_hma' == 1 {
    do "`code'/build/merge_nfip_hma.do" "`data'"
}

// iv) Build (Anna)
if `merge_datasets' == 1 { 
    foreach state of local states {
        shell bash "`code'/slurm/run_property_matching.sh" ///
            --state "`state'" ///
            --data "`data'" ///
            --python "`python'" ///
            --memory "8GB" ///
            --threads 4 ///
            --from 3 ///
            --tmp "`code'/../tmp/matching/`state'"
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
if `attom_value' == 1 {
    shell `python' "`code'/build/attom_value.py" --data "`data'" --states "`states'"
}
if `attom_value_dta' == 1 {
    shell `python' "`code'/build/parquet_dta.py" ///
        --input "`data'/build/attom_value/*_attom_value.parquet" ///
        --output "`data'/build/attom_value/attom_value.dta"
}
if `complete' == 1 {
    do "`code'/build/complete.do" "`data'" "`states'"
}

// v) Descriptives
if `summary_table' == 1 {
    do "`code'/descriptives/summary_table.do" "`data'" "`output'"
}
if `histograms' == 1 {
    do "`code'/descriptives/histograms.do" "`data'" "`output'"
}

// vi) Analysis
if `es_elevation' == 1 { 
    do "`code'/analysis/es_elevation.do" "`data'" "`output'"
}
if `same_flood_claims' == 1 {
    do "`code'/analysis/same_flood_claims.do" "`data'" "`output'"
}

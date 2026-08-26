/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: 2026-08-04

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

/* * --- Vendela ---
local code "/Users/vendelasolvindnorman/Documents/Econ_PhD/Projects/climate-investments/code"
local data "/Users/vendelasolvindnorman/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
local python "/Users/vendelasolvindnorman/anaconda3/bin/python3"
*/

* --- Anna ---
local code "/Users/anna/Desktop/climate-investments/code"
local data "/Users/anna/Library/CloudStorage/Dropbox/Flooding/Empirical/Data"
local python "/opt/anaconda3/bin/python"

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
local clean_nfip_multiple_loss = 0 // clean NFIP multiple-loss data

// ii) Build
local prep_fma                 = 0 // collapse FMA across years to zip/county level
local prep_nfip_policies       = 1 // collapse NFIP policy data to property level
local compile                  = 1 // compile property-level analysis dataset
local compile2                 = 0 // PRELIMINARY: attach ATTOM values + Builty via Anna's Wagner links

// iii) Build: Builty elevation permits -> ATTOM -> NFIP properties
// Revised 2026-08-25 (Anna; calls to Anna's scripts only). The per-step
// switches build_attom_geocoded, build_attom_nfhl, attom_onto_permits and
// build_attom_nfhl_builty were removed rather than left at 0: each wrote into a
// separate directory with different flags, so running one produced numbers that
// did not match the committed results. run_matching replaces all four and calls
// the same five steps through code/build/run_matching.sh.
// build_nfip_attom_wagner was removed with them -- the Wagner links are
// superseded by the tier assignment in step 5.
local geocode_attom            = 0 // extract ATTOM addresses + Census geocode to block groups
local run_matching             = 0 // finalized 5-step Builty/ATTOM/NFIP matching, per state
local build_policy_consensus   = 0 // alternate: policy-year links, then stable property consensus
local compile_stable_property  = 1 // matching links -> labeled diagnostic + clean analysis files


local build_attom_values       = 0 //generate attom state summary files
local build_nfip_attom_fma     = 0 // build property-level analysis dataset state level
local compile_property         = 0 // compile property-level analysis datasets

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
if `compile2' == 1 {
    do "`code'/build/compile2.do" "`data'" "`states'"
}

// iii) Build: Builty elevation permits -> ATTOM property values
// VN NOTE: Your build code needs to be cleaned up / consolidated
if `geocode_attom' == 1 { // run with TORCH: network-bound Census geocode, resume-safe
    foreach state of local states {
        shell `python' "`code'/build/geocode_attom.py" --data "`data'" --state "`state'"
    }
}
* Added 2026-08-25 (Anna), replacing the four per-step blocks above.
if `run_matching' == 1 {
    * Finalized five-step matching chain, one state at a time: ATTOM geocoded
    * panel -> NFHL join -> Builty address match against raw ATTOM -> Builty
    * flags onto the ATTOM universe -> one ATTOM property assigned per NFIP
    * property. run_matching.sh holds the step order and the flags, and is the
    * same file the cluster wrapper (run_matching_slurm.sh) runs.
    *
    * Each step skips when its output already exists, so a rerun is cheap and an
    * interrupted state resumes. Step 1 needs the cached Census geocoder results,
    * which live on TORCH rather than in Dropbox: locally that step is skipped
    * and the synced panel is used.
    *
    * ATTOM is large, so a big state takes hours here. On the cluster, submit
    * run_matching_slurm.sh instead of this serial loop.
    foreach state of local states {
        shell bash "`code'/build/run_matching.sh" ///
            --state "`state'" --data "`data'" --python "`python'" ///
            --memory "24GB" --threads 4
    }
}

if `build_policy_consensus' == 1 {
    * Alternate design: policy year enters every Wagner-style matching cell.
    * Repeated assignments become evidence for one stable property-level link.
    capture mkdir "`data'/build/nfip_attom_policy_year_v2"
    capture mkdir "`data'/tmp/nfip_attom_policy_year_v2"
    foreach state of local states {
        local st = lower("`state'")
        capture mkdir "`data'/tmp/nfip_attom_policy_year_v2/`st'"
        shell `python' "`code'/build/alternates/assign_attom_to_nfip.py" ///
            --state "`state'" ///
            --nfip "`data'/clean/nfip_policies_state/`st'.dta" ///
            --attom "`data'/build/nfip_attom_pipeline_v2/geocoded/`st'_attom_geocoded.parquet" ///
            --attom-nfhl-builty "`data'/build/nfip_attom_pipeline_v2/attom_nfhl_builty/`st'_attom_nfhl_builty.parquet" ///
            --use-codes 376,380,382,383,385,386 ///
            --out "`data'/build/nfip_attom_policy_year_v2/`st'_nfip_attom_policy_year.parquet" ///
            --tier-diagnostics "`data'/build/nfip_attom_policy_year_v2/`st'_tier_diagnostics.csv" ///
            --cell-diagnostics "`data'/build/nfip_attom_policy_year_v2/`st'_cell_diagnostics.csv" ///
            --tmp "`data'/tmp/nfip_attom_policy_year_v2/`st'"
    }
    shell `python' "`code'/build/alternates/export_policy_links.py" ///
        --data "`data'" --states "`states'"
    do "`code'/build/alternates/collapse_nfip_attom_to_property.do" "`data'" "`states'"
    do "`code'/build/alternates/compile_nfip_attom_policy_consensus.do" "`data'"
}

if `compile_stable_property' == 1 {
    * Finalize the original stable-property V2 outputs. Convert each state
    * parquet separately so the national file never has to live in Python RAM.
    capture mkdir "`data'/build/nfip_attom_property"
    foreach state of local states {
        local st = lower("`state'")
        shell `python' "`code'/build/parquet_to_dta.py" ///
            --input "`data'/build/nfip_attom_pipeline_v2/nfip_attom_property/`st'_nfip_attom_property.parquet" ///
            --output "`data'/build/nfip_attom_property/`st'_nfip_attom_property.dta"
    }
    do "`code'/build/compile_nfip_property_attom.do" "`data'" "`states'"
}

if `build_attom_values' == 1 { //run with TORCH due to size, not locally
    foreach state of local states {
        shell `python' "`code'/build/alternates/build_attom_value_cells.py" --data "`data'" --state "`state'"
    }
}
if `build_nfip_attom_fma' == 1 {
    foreach state of local states {
        do "`code'/build/alternates/build_property_panel.do" "`data'" "`state'"
    }
}
if `compile_property' == 1 {
    do "`code'/build/alternates/compile_nfip_attom_fma.do" "`data'" "`states'"
}

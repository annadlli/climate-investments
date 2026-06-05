// update: June 3, 2026
// Master script for climate-investments project

clear all
set more off

cd "/Users/anna/Desktop/Research/climate-investments"

*************************************************************
* Define folders
*************************************************************
global code     "$c(pwd)/code"
global raw      "$code/raw"
global clean    "$code/clean"
global build    "$code/build"
global analysis "$code/analysis"

global data     "$c(pwd)/data"
global dclean   "$data/clean"
global dbuild   "$data/build"

*************************************************************
* Raw code files
* Raw ATTOM and BUILTY files were created by:
*   python import.py
*   python compile_builty.py
*************************************************************

*************************************************************
* Clean code files
*************************************************************
do "$raw/create_clean_files.do"

*************************************************************
* Build code files
*************************************************************

* 1. Strictly filter BUILTY elevation permits: TO BE REVISED
shell python "$build/filter_builty_strict.py"

* 2. Split filtered BUILTY data into TX and VA state parquet files
shell python "$build/split_builty_states.py" ///
    --input "$dclean/all_elevation.parquet" ///
    --out-dir "$dbuild" ///
    --states TX VA

* 3. Match ATTOM onto permits // TO BE REVISED
shell python "$build/attom_onto_permits.py" --state TX
shell python "$build/attom_onto_permits.py" --state VA

* 4. Convert matched parquet files to Stata .dta
shell python "$build/parquetdta.py" --state TX
shell python "$build/parquetdta.py" --state VA

* 5. Build NFIP files
do "$build/nfip_build.do"

* 6. Build NFIP-HMA panels
do "$build/build_nfip_hma_panels.do"

*************************************************************
* Analysis code files
*************************************************************
 do "$analysis/april_17_descriptives.do" //to be revised

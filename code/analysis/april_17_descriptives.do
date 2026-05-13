* april_17_descriptives.do
* Master descriptive workflow for April 17 figures/tables.
*
* Runs:
*   1. sanity_check.do
*   2. all_elevation dot map + bar chart
*   3. FEMA/FMA-HMGP maps (heat map + dot map + TX/VA state maps)
*
* Outputs written to:
*   output/descriptives/
*
* Notes:
*   - The map / chart scripts are currently implemented in Python because
*     they rely on the county shapefile workflow already validated in this repo.
*   - This do-file is an orchestration entry point so one command can rebuild
*     the full April 17 descriptive set.

clear all
set more off

local root   "/Users/anna/Desktop/Research/climate-investments"
local codedir "`root'/code/analysis"
local outdir "`root'/output/descriptives"

cap mkdir "`outdir'"

cap log close april17desc
log using "`outdir'/april_17_descriptives_log.txt", text replace name(april17desc)

di "================================================================"
di "APRIL 17 DESCRIPTIVES"
di "================================================================"
di ""

********************************************************************************
* 1. Sanity check
********************************************************************************
di "Running sanity_check.do ..."
do "`codedir'/sanity_check.do"

********************************************************************************
* 2. all_elevation figures
*    - county dot map
*    - bar chart over time
********************************************************************************
di ""
di "Running all_elevation dot map + bar chart ..."
shell python3 "`codedir'/generate_all_elevation_dotmap_bar.py"

********************************************************************************
* 3. FEMA/FMA-HMGP figures
*    - national heat map
*    - national dot map
*    - TX heat map
*    - VA heat map
*    - source table
********************************************************************************
di ""
di "Running FEMA/FMA-HMGP map pipeline ..."
shell python3 "`codedir'/generate_elevation_hma_maps.py"

********************************************************************************
* 4. Output checklist
********************************************************************************
di ""
di "Expected outputs in `outdir':"
di "  - sanity_check.txt"
di "  - figure_map_us_all_elevations_dotmap_only.png"
di "  - figure_bar_all_elevations_over_time_only.png"
di "  - figure_all_elevation_year_counts_only.txt"
di "  - figure_map_us_fema_elevation_grants_heatmap.png"
di "  - figure_map_us_fema_elevation_grants_dotmap.png"
di "  - figure_map_tx_fema_elevation_grants_heatmap.png"
di "  - figure_map_va_fema_elevation_grants_heatmap.png"
di "  - figure_heatmap_project_counts_table.txt"
di ""
di "Done."

log close april17desc

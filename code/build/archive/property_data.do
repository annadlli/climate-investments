//will need to match on ADDRID
//nov 25 
cd "/Users/anna/Desktop/climate-investments/"

// directories
local raw        "/Users/anna/Desktop/climate-investments/properties"
local final_dest "/Users/anna/Desktop/climate-investments/data/prop_process"

// list all parquet files
local files : dir "`raw'" files "*.gz"

foreach f of local files {
    
    clear all
    
    // load parquet file
    gzimport delimited using "`raw'/`f'", clear

    // filter: kept houses only: //https://cdn.document360.io/1bfc4c04-1a51-46fe-b02a-f7156bc4af9b/Images/Documentation/Technical%20Documentation%20VMS-RTD_Inbound%20Identity-2025_Rev1.0%20(1).pdf#page=10.10
	drop if prop_mobhome == "Y"
	keep if prop_ind == 10
    // define output filename (remove .parquet and replace with .dta)
    local outname = subinstr("`f'", ".csv.gz", ".dta", .)

    // save to final folder
    save "`final_dest'/`outname'", replace
}



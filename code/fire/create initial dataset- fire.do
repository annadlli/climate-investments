//import data
//last update: November 5, 2025
// gz files imported from dewey using python and the bulk API
//import in batches
clear all
program define import_filter_permits
    syntax, batchprefix(string)

    clear all
    set more off
    cd "/Users/anna/Desktop/climate-investments/data/fire/raw"

    local dest "/Users/anna/Desktop/climate-investments/data/fire/process"

    * Construct file list dynamically based on batch prefix
	//note: differs from florida flooding
 local files : dir "." files "building-permits-united-states_`batchprefix'_*.csv.gz"

    local nfiles : word count `files'
    if (`nfiles' == 0) {
        di as error "No files found for prefix `batchprefix'_"
        exit
    }

    di as result " Found `nfiles' files for prefix `batchprefix'_"

    foreach f of local files {
        di as txt "----- Importing and filtering file: `f' -----"
        clear

        *-----------------------------------------
        * Import CSV.GZ file
        *-----------------------------------------
        gzimport delimited using "`f'", clear

        gen source_file = "`f'"
        gen desc = lower(description)
        replace desc = "" if missing(desc)

        * Regex patterns
		local STRUCTURE"(framing|truss|roof|wall|joist|beam|foundation|demo|demolition|harden)"
		local NEG "(alarm|fire alarm|fa system|sprinkler|suppression system)"
		local FIRE "(wildfire|wild fire|forest fire|brush fire|grass fire|wildland fire|burn scar|fireproof)"


        gen fire   = regexm(desc, "`FIRE'")
        gen structure = regexm(desc, "`STRUCTURE'")
        gen neg       = regexm(desc, "`NEG'")

        * Drop irrelevant record types (if available)
        capture confirm variable record_type_original
        if _rc == 0 {
            gen record_type_l = lower(record_type_original)
            drop if regexm(record_type_l, "sign")
            drop if regexm(record_type_l, "pool")
            drop if regexm(record_type_l, "commercial")
            drop if regexm(record_type_l, "condo")
        }

        drop if regexm(desc, "sign")
        drop if regexm(desc, "channel letters")
        drop if regexm(desc, "radon mitigation")
        drop if regexm(desc, "raise a/c")

        drop if record_type_original == "Residential New Single Family Dwelling/Duplex"
        drop if record_type_original == "Building - Residential - Marine"
        drop if record_type_original == "Commercial Buildings (Including Condo) - Alterations"
        drop if record_type_original == "Temporary Certificate of Occupancy - Residential (Single Family Residence)"
        drop if record_type_original == "Residential New Construction Permit"
      
        drop if record_type_original == "Change of Use"

        keep if (fire | structure) & !neg

        if _N > 0 {
            local shortname = subinstr("`f'", ".csv.gz", "", .)
            save "`dest'/`shor tname'_filtered.dta", replace
            di as result "Saved: `dest'/`shortname'_filtered.dta"
        }
        else {
            di as txt "No matches found in `f'"
        }
    }

    di as result "Batch `batchprefix' completed and saved in `dest'"
end

forvalues i = 0/7 {
    di as txt "=== Starting batch `i'_ ==="
    import_filter_permits, batchprefix("`i'")
}

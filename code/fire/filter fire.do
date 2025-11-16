//updated: Nov 15, 2025
//read in
clear all
local dest "/Users/anna/Desktop/climate-investments/data/fire/clean"

use "`dest'/permits_filtered_all.dta", clear
// //98559 obs as of nov 15, 2025
//start filtering
gen rec_sub = lower(record_subtype_original)
gen rec_type = lower(record_type_original)
* ---- EXCLUSION FILTERS ----

drop if regexm(desc, "solar")
drop if regexm(rec_sub, "demolition")
drop if regexm(desc, "graffiti")
drop if regexm(desc, "charger")
drop if regexm(desc, "tree")
drop if regexm(desc, "demo")
drop if regexm(desc, "gas line")
drop if regexm(desc, "kw")
drop if regexm(desc, "photovoltaic")
drop if regexm(desc, "natural gas")
drop if regexm(desc, "swimming pool")
drop if regexm(rec_type, "solar")
drop if regexm(desc, "ev charg")
drop if regexm(desc, "hvac")
drop if regexm(desc, "kitchen")
drop if regexm(desc, "non-structural")
drop if regexm(desc, "bathroom")
drop if regexm(desc, "wall furnace")
drop if regexm(desc, "telecom")
drop if regexm(desc, "mobile")
drop if regexm(desc, "office")
drop if regexm(desc, "plumbing")
drop if regexm(desc, "electrical")
drop if regexm(desc, "sewer")
drop if regexm(desc, "acoustical")
drop if regexm(desc, "waterdamage")
drop if regexm(rec_sub, "graffiti")
drop if regexm(rec_sub, "flood review")
drop if regexm(rec_sub, "grading")
drop if regexm(rec_sub, "office")
drop if regexm(rec_sub, "duplex")
drop if regexm(rec_sub, "church")
drop if regexm(rec_sub, "beauty shop")
drop if regexm(rec_sub, "school")
drop if regexm(rec_sub, "fire alarm")
drop if regexm(rec_sub, "apartment")
drop if regexm(rec_sub, "tele")
drop if regexm(rec_sub, "retaining")
drop if regexm(rec_sub, "multi")
drop if regexm(rec_sub, "warehouse")
drop if regexm(rec_sub, "bank")
drop if regexm(rec_sub, "restaurant")
drop if regexm(rec_sub, "retail")
drop if regexm(rec_sub, "manufacturing")
drop if regexm(rec_sub, "temp")
drop if regexm(rec_sub, "bakery")
drop if regexm(rec_type, "hvac")
drop if regexm(rec_sub, "club")
drop if regexm(rec_type, "multi")
drop if regexm(rec_sub, "motion")
drop if regexm(rec_type, "grading")
drop if regexm(rec_type, "nonbldg")
drop if regexm(rec_sub, "condo")
drop if regexm(rec_type, "photo")
drop if regexm(rec_type, "bldg-new")
drop if regexm(rec_sub, "museum")
drop if regexm(rec_type, "street closure")
drop if regexm(rec_type, "condo")
drop if regexm(rec_type, "public")
drop if regexm(rec_sub, "public")
drop if regexm(rec_sub, "23")
drop if regexm(rec_type, "public")
drop if regexm(rec_type, "mechanical")
drop if regexm(rec_type, "electrical")
drop if regexm(rec_type, "deferred")
drop if regexm(rec_type, "non-res")
drop if regexm(rec_type, "repair")
drop if regexm(rec_type, "same roof material")
drop if regexm(rec_sub, "solar")
drop if regexm(rec_sub, "heat")
drop if regexm(rec_sub, "mobile")
drop if regexm(rec_sub, "repair")
drop if regexm(rec_sub, "elec")
drop if regexm(rec_sub, "road")
drop if regexm(rec_type, "encroach")
drop if regexm(rec_sub, "mh ins")
drop if regexm(rec_sub, "demo")
drop if regexm(rec_sub, "elec")
drop if regexm(rec_type, "kitchen")
drop if regexm(rec_type, "modular")
drop if regexm(rec_type, "solar")
drop if regexm(rec_type, "furnace")
drop if regexm(desc, "commercial")
drop if regexm(desc, "service panel")
drop if regexm(desc, "blight")
drop if regexm(desc, "convert")
drop if regexm(desc, "sound")
drop if regexm(desc, "bath")
drop if regexm(desc, "clean-out")
drop if regexm(desc, "hot water")
drop if regexm(desc, "gutter")
drop if regexm(desc, "fireplace")
drop if regexm(desc, "flashing")
drop if regexm(desc, "reroof with 25 yr shingles")
drop if regexm(desc, "laundry")
drop if regexm(desc, "interior wall")
drop if regexm(desc, "drywall")
drop if regexm(desc, "duct")
drop if regexm(desc, "heater")
drop if regexm(desc, "condenser")
drop if regexm(desc, "modules")
drop if regexm(desc, "truss")
drop if regexm(desc, "city")
drop if regexm(desc, "retaining wall")
drop if regexm(desc, "pv system")
drop if regexm(desc, "bedroom")
drop if regexm(desc, "pvs ")
drop if regexm(desc, "vent")
drop if regexm(desc, "skylight")
drop if regexm(desc, "comm")
drop if regexm(desc, "outlet")
drop if regexm(desc, "condo")
drop if regexm(desc, "multi")
drop if regexm(desc, "duplex")


//note: there are obs that indicate seismic upgrade/seismic strenghtening

//want preventative, not repairing
drop if regexm(desc, "damage")
drop if regexm(desc, "repair")
//25334 obs

//chatgpt says otc permit are generic and not fire hardening

drop if regexm(rec_type, "over the counter")
drop if regexm(rec_type, "otc")
//18712 obs


//remove non fire hardening reroof should probably be done next time
save "`dest'/permits_filtered_further.dta", replace



di as result "All file filtered"
br  rec_sub rec_type desc

//---------------------------------------------
// 0. SET PATHS
//---------------------------------------------
local in   "/Users/anna/Desktop/climate-investments/data/raw"
local clean "/Users/anna/Desktop/climate-investments/data/clean"


//---------------------------------------------
// 1. Convert NATIONAL ZCTA SHAPEFILE → Stata
//---------------------------------------------
cd `in'
spshape2dta tl_2020_us_zcta520.shp, replace saving(zcta_map)


//---------------------------------------------
// 2. Filter ZIP shapefile ATTRIBUTES to FLORIDA
//---------------------------------------------
use "`in'/zcta_map.dta", clear

destring ZCTA5CE20, replace
keep if inrange(ZCTA5CE20, 32000, 34999)

keep _ID ZCTA5CE20 INTPTLAT20 INTPTLON20 ///
     CLASSFP20 MTFCC20 FUNCSTAT20 ALAND20 AWATER20

save "`clean'/zcta_map_FL_attr.dta", replace


//---------------------------------------------
// 3. Filter SHAPE GEOMETRY to the matching `_ID`s
//---------------------------------------------
use "`in'/zcta_map_shp.dta", clear

* Keep only polygons whose _ID appears in the FL attributes
merge m:1 _ID using "`clean'/zcta_map_FL_attr.dta"
keep if _merge == 3
drop _merge

save "`clean'/zcta_map_FL_shp.dta", replace


//---------------------------------------------
// 4. Prepare TENURE DATA (Florida only)
//---------------------------------------------
local clean "/Users/anna/Desktop/climate-investments/data/clean"
use "`clean'/chunk_combined.dta", clear //66,003 obs

drop if tenure < 0
heatplot tenure, ///
    title("Heat Map of Tenure") ///
    aspectratio(1) ///
    color(BuGn)

collapse (mean) tenure, by(PROPERTYADDRESSZIP)

rename PROPERTYADDRESSZIP ZCTA5CE20
destring ZCTA5CE20, replace

keep if inrange(ZCTA5CE20, 32000, 34999)

save "`clean'/tenure_by_zip_FL.dta", replace


//---------------------------------------------
// 5. Merge TENURE into FLORIDA attribute table
//---------------------------------------------
use "`clean'/zcta_map_FL_attr.dta", clear

merge 1:1 ZCTA5CE20 using "`clean'/tenure_by_zip_FL.dta"
drop if _merge == 2   // ZIPs with no geometry
drop _merge

save "`clean'/zcta_with_tenure_FL.dta", replace


local clean "/Users/anna/Desktop/climate-investments/data/clean"
use "`clean'/zcta_map_FL_attr.dta", clear
merge 1:1 ZCTA5CE20 using "`clean'/tenure_by_zip_FL.dta"
tab ZCTA5CE20 if _merge==1   // ZIPs with no tenure data


//---------------------------------------------
// 6. CREATE GEOFRAME: attributes + geometry
//---------------------------------------------
// This file has _ID + tenure + ZCTA info
// and its _ID matches the one in the _shp file.
geoframe create zcta_attr using "`clean'/zcta_with_tenure_FL.dta", ///
    id(_ID) ///
    shp("`clean'/zcta_map_FL_shp.dta") ///
    replace

geoplot ///
    (area zcta_attr tenure if inrange(ZCTA5CE20, 33100, 33199), ///
        levels(10) color(viridis, reverse)) ///
    (line zcta_attr if inrange(ZCTA5CE20, 33100, 33199), ///
        lcolor(white) lwidth(vthin)) ///
    , ///
    title("Tenure Heatmap — Miami / Broward / Palm Beach") ///
    legend(on position(4) ring(0))  ///
    tight ///
    aspectratio(1)


//building permits for now only 33100 -> limits amount of match



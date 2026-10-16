# Deck notes, 2026-09-10 (issues #26 and #13)

Text and numbers for two deck tabs. Numbers come from the 09-07 matching run
(FL LA TX NJ link files on Dropbox) and the clean Builty file of 09-06.

## Tab: Where Builty elevations go missing (issue #26)

Takeaway: of the screened Builty retrofit properties, 52 percent reach an NFIP
property in FL, 78 percent in LA and 80 percent in TX. The loss has two steps with
different causes, and only the second one is a matching problem we can fix. The
address match keeps 75 percent of permits in FL, 85 in LA and 90 in TX; the cell
match then keeps most of what is left in LA and TX but only about two thirds in FL,
where the insured share of the housing stock is lowest.

| State | Builty retrofit properties (clean file) | ... with an ATTOM address match | ... on an NFIP property |
|---|---|---|---|
| FL | 2,449 | 2,230 properties (2,310 of 3,074 permits incl. new builds, 75%) | 1,274 |
| LA | 1,568 | 1,342 (1,431 of 1,675 permits incl. new builds, 85%) | 1,218 |
| TX | 1,087 | 1,041 of 1,152 permits incl. new builds (90%) | 869 |
| 20 states (09-06 diagnosis) | 9,853 permits | 7,089 | 5,576 |

Update 09-15: the loose address rungs are now production. Address match FL 86%, LA 92%,
TX 95% (was 75, 85, 90); retrofit houses on the NFIP panel FL 1,465, LA 1,313, TX 901
(was 1,274, 1,218, 869), 3,679 in all. The recovered houses pass the NFIP stage at 87%,
and 73 of Louisiana's 95 are in Livingston Parish, whose permit system appends lot
numbers to addresses.

Step 1, Builty to ATTOM (address match, `attom_builty.py`). A permit is lost when
no ATTOM record carries the same street address: the Builty address is a lot,
range or intersection rather than a house; the permit has no ZIP and the
address is not unique in the county; the ATTOM parcel was split or renumbered
since; or the property is not single family in ATTOM (multi-unit, commercial).
The 09-06 revisions (Census geocode of every permit, ATTOM address fallbacks)
already recovered part of this.

Step 2, ATTOM to NFIP (cell match, `nfip_attom.py`). An elevated ATTOM house
does not reach the panel when (a) no NFIP policy was ever written in its cell,
which is expected, not a loss, since uninsured houses are outside the universe;
(b) the cell holds more ATTOM houses than NFIP policies, so the elevated house is
not the one drawn. Since 09-06 Builty houses are ranked first inside every cell,
so (b) only bites when the cell has zero NFIP properties; (c) the house has no
block group or flood zone because its ATTOM geocode failed. Since 09-06 the
Builty geocode backfills coordinates, block group and zone for those houses.

Louisiana and the construction year. NFIP always has a construction year, but
59 percent of LA single-family ATTOM properties (783k of 1.33M) have none
(Texas 4.5 percent, Florida 0.3 percent). The ladder uses the year wherever both
sides have one and falls to the last tier (block group x flood zone, no year)
only for leftovers. In LA that last tier still takes 276k of 704k matches and
1,081 of the 1,218 Builty retrofits on the panel. So the earlier deck line
"Louisiana lacks year built in ATTOM, so its links use block group x flood zone
without a year" is right for most LA Builty links, wrong as a blanket statement.
Proposed fix (not applied): move the no-year block-group x zone tier ahead of
the tiers that drop the flood zone (11-14), so a same-block-group, same-zone
match beats a county x year match when the ATTOM year is missing.

ATTOM value cleaning (also #26). The value file now sets nominal values below
$10,000 to missing upstream (`attom_value_wide.py`, alongside zeros and values
above $100M); the ad hoc `attom_value >= 10000` screen in the summary table is
gone, and the nominal `attom_market_value_total` no longer sits on the panel.

## Note for the HMA tabulations (issue #13)

The state counts in the deck's earlier "FMA elevations by state" tabulation came
from the raw Mitigated Properties file, which logs properties for applications
that were never funded (LA 7,400 raw vs 5,658 funded; TX 1,532 vs 1,090; FL
1,043 vs 436). The Elevations by State figure and table were rerun on 09-08 from
the cleaned HMA file (approved, obligated or closed projects only), all programs
kept with HMGP shown separately: LA 5,658 HMA (3,167 HMGP, 2,433 FMA), NJ 2,909,
TX 1,090, NC 1,037, VA 726, FL 436, NY 374. The older tabulations were not redone;
note the difference on the tab.

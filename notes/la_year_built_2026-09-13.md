# Louisiana year built: check and ladder test (issue #26), 2026-09-13

Script: `code/build/alternates/nfip_attom_la_noyear.py`, a test run by hand, not
in master.do. Inputs: the 09-13 LA step-3 file and the LA link file,
both in Dropbox `build/nfip_attom_pipeline_v2/`. The reordered result is saved
next to the baseline as `la_nfip_attom_property_noyear.parquet`; nothing on the
production path changed.

## 1. Where the year is missing

62 percent of LA single-family ATTOM candidates (1.03M of 1.65M) have no year
built, and 89 percent of the Builty retrofit houses (1,161 of 1,300). The gap is
not spread evenly: it is a parish-level property of the assessor feed. Parishes
with the most NFIP properties:

| Parish | NFIP properties | ATTOM SF | Share no year | Builty retrofits |
|---|---|---|---|---|
| Jefferson | 263,332 | 142,123 | 1.00 | 758 |
| Orleans | 180,870 | 110,541 | 0.29 | 0 |
| St. Tammany | 129,800 | 111,000 | 0.04 | 14 |
| E. Baton Rouge | 105,591 | 166,687 | 0.60 | 129 |
| Lafayette | 59,849 | 88,120 | 0.17 | 22 |
| Calcasieu | 46,853 | 70,785 | 1.00 | 53 |
| Livingston | 46,670 | 45,000 | 1.00 | 51 |
| Terrebonne | 42,639 | 34,292 | 0.62 | 63 |
| Ascension | 41,354 | 47,445 | 1.00 | 122 |
| Lafourche | 32,618 | 31,561 | 0.52 | 25 |
| St. Bernard | 30,490 | 16,894 | 1.00 | 2 |

Jefferson, Calcasieu, Livingston, Ascension, St. Bernard, St. John, St. Mary
and Acadia report no year for any single-family parcel. Orleans, St. Tammany,
Lafayette and Bossier are nearly complete. The top five parishes hold 59 percent
of NFIP properties and 71 percent of the Builty retrofits but only 29 percent of
the missing years, because Jefferson alone carries most of both.

## 2. The reordered ladder

The no-year block group x flood zone tier was moved ahead of the four tiers that
drop the flood zone (11 to 14). Effect on LA:

| | Baseline | Reordered |
|---|---|---|
| Matched in tier 15 (no year) | 275,748 | 280,408 |
| Matched in tiers 11 to 14 | 223,382 | 218,960 |
| Assignments changed | | 85,339 of 1,244,072 |
| Newly matched / newly unmatched | | 2,629 / 2,363 |
| Builty retrofits on the panel | 1,218 | 1,219 |

## 3. Conclusion

The reorder does not help and should not be applied. The displacement mechanism
it was meant to fix needs a parish where some ATTOM houses carry a year and
others do not; in Jefferson none do, so every Jefferson NFIP property already
goes straight to the no-year tier and the Builty houses there cannot be
displaced. The parishes where displacement is possible (E. Baton Rouge,
Terrebonne, Lafourche, Plaquemines) hold too few retrofits for the reorder to
matter. The 85k reassignments are a reshuffle among houses of equal standing,
not an improvement.

What the LA problem actually is: the ATTOM assessor feed for Jefferson and
several other parishes has no year built, so for a fifth of LA's NFIP
properties, and most of its Builty retrofits, the match rests on block group and
flood zone alone. Construction year is not recoverable from ATTOM there. Options
if it matters for the analysis: use the NFIP construction year as the property's
year (it is always present and is what the match keys on); or ask ATTOM whether
the Jefferson Parish assessor file carries year built in a field other than the
one Dewey delivered. The deck line should say: "In Louisiana most ATTOM parcels
lack a year built, a parish-level gap centred on Jefferson; those links use block
group and flood zone alone."

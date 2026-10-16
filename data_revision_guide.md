# Data-section revision guide

Claude change 10-15: rewritten around the ATTOM permit pipeline (the July version described the
Builty chain, a two-tier property build and FMA-only grants, none of which is current).

This guide separates the material by audience: what the paper's data section says, what the
internal appendix records, and which decisions are still open. The draft paper section is
`notes/data_section_2026-10-15.tex`; the internal record is `code/TODO.md` (dated entries) with
the file map in `pipeline_file_manifest_2026-10-15.xlsx`.

## Paper data section

`notes/data_section_2026-10-15.tex`, modelled on Wagner (2022) Section III and Gruber and
Solomon (2026). It contains only what a reader needs:

1. what NFIP policies and claims, HMA grants, ATTOM permits and ATTOM assessor records measure;
2. the structure identifier (block group x construction date x policy origination date) and
   why the NFIP elevated flag is a stock, not an event;
3. the permit as the elevation event: the text screen, jurisdiction-year coverage, the event
   date (finaled year, else last status year, else issue year) and the done rule;
4. the Wagner cell match with the aligned zone key, its tier order, match rates and the
   cell-grain caveat;
5. the sample: NFIP-insured single-family structures in an SFHA (NFIP rating, NFHL fallback),
   FL LA TX, 2009-2025, with flags rather than restrictions at the build stage;
6. the summary table in the Bodere-Brancaccio-Kang lettered-panel layout.

It omits script names, paths, variable names, switches, superseded sources and open
implementation alternatives. Numbers carry a build tag ([10-15]) until the section is frozen.

## Internal data appendix

Not a separate document any more. The record lives in three places:

- `code/CLAUDE.md`: the pipeline map (script -> output), the merge logic and the eligible
  universe. Stable facts only.
- `code/TODO.md`: every change with its date, the counts it produced and what it superseded.
  Read the section-0 entries from 10-02 on for the current state.
- `pipeline_file_manifest_2026-10-15.xlsx`: three sheets, the pipeline files with author, last
  edit, git status and output; the key data files on Dropbox with date and size; and every
  uncommitted path since the 09-23 commit with a suggested action.

Current counts and their provenance (10-15 build):

| Quantity | Value | Source |
|---|---|---|
| NFIP single-family structures, FL LA TX | 7,110,507 | link files |
| of which rated in an SFHA | 3,320,723 | policy files |
| matched to an ATTOM parcel | FL 86%, LA 65%, TX 92% | 10-12 links |
| elevation permit addresses / retrofits | 17,143 / 15,290 | Oct 8 permit file |
| NFIP structures with a permit, SFHA | 3,041 / 2,822 / 1,117 | 10-12 links |
| analysis sample structures / property-years | 3,291,655 / 16,590,589 | analysis.dta 10-15 |
| permitted retrofits in the sample | 5,468 | summary table 10-15 |

## Status labels

- **CURRENT**: can go in the paper after the next full build. The match, the sample, the
  summary table, the permit counts above.
- **DECISION**: an empirical choice Anna has to make. Listed below.
- **BLOCKER**: must be reconciled before results are final. The status-history dates: 39% of
  completion rows are undated (1900-01-01), so `completed` counts any completion row while the
  event year uses dated rows only; a third of permits have every status row on one date.

## Open decisions

1. Plain "closed" as a completion label (881 addresses) or not.
2. Whether the city-feed adjustment (feeds that never write a completion) becomes the done rule
   where the status history is silent.
3. Post-FIRM status by first or last policy year in the summary table.
4. One funding measure from permit text, ICC payment and county HMA exposure.
5. NJ back in the sample; TX strict coverage at ZIP-year.
6. Classifiers endpoint from Dewey (would replace the new-construction regex).

## Corrections relative to the July guide

- The permit source is ATTOM building permits (since 10-02), not Builty; Builty survives only
  as a comparison (deck tab, `build/alternates/attom_vs_builty_homes.py`).
- One canonical builder: the tiered cell match in `nfip_attom.py` into `complete.do`. The
  two-tier property build is archived.
- HMA exposure is cumulative at the county level across every elevation program, not FMA-only
  and not county-year.
- Property values are cell-level draws, not the insured structure's own value; the match is
  arbitrary within a cell.
- `property_id` is state-qualified (`property_id_state` within state, `property_id` across).
- Nothing is restricted at the build stage except the SFHA rating; coverage, match and permit
  status are flags, and each analysis applies its own rule.

# Slides plan, first half of the deck (Tuesday 2026-09-08)

_Claude draft for Anna, 2026-09-07, panel numbers filled 15:45 from the 09-07 analysis.dta. One line per
exhibit: what it shows, the sentence to say, and where the file is._
_Deck = Deck_Sep3.xlsx tabs; new tabs are drafted in Deck_Sep8_draft_tabs.xlsx in the same folder._

## The new focus, in one paragraph

We narrow to four states (FL, LA, TX, NJ), keep every house rather than every policy, and treat
Builty elevation permits as the outcome, NFIP as the insurance context, and FMA/HMGP as the funding.
The data question this week is whether Builty's permit feeds are good enough to be that outcome; the
empirical facts below say where they are (FL, TX) and where they are not (NJ, parts of LA).

## Tab 1. Elevations by State  (new, first tab)

- Figure: `output/figures/elevations_by_state.png`; table `output/tables/elevations_by_state.xlsx`.
- Shows: home elevations per state from three sources, Builty retrofit permits vs HMGP- and FMA-funded
  properties, 20 sample states.
- Say: "Louisiana is the only state large on both the permit and the grant side; Florida has the most
  permits but few grants; New Jersey has the second-largest grant program and almost no permit coverage."
- Numbers: LA 1,568 Builty / 5,658 HMA; FL 2,444 / 436; TX 1,086 / 1,090; NJ 44 / 2,909.
- Note for the room: FMA column totals 5,268 against the deck's earlier 5,572 because the cleaner
  changed; ranking is unchanged.

## Tab 2. Elevation data investigation  (new)

- Table: `output/tables/builty_coverage_by_state.xlsx`.
- Shows: Builty coverage by state, counties with a feed, counties clearing 1 permit per 100 homes,
  share of single-family stock covered in 2020.
- Say: "Coverage is a county-feed problem, not a permit-text problem. In FL and TX, 95-97% of homes sit
  in a county whose feed clears the floor; in LA it is 76% and in NJ 50%, and NJ's five counties are
  the whole state's feed."
- Numbers: counties clearing the floor FL 47 of 68, TX 34 of 206, LA 17 of 64, NJ 4 of 21; median feed
  FL 11.5, TX 6.9, LA 3.2, NJ 3.2 permits per 100 homes.
- Second block, ATTOM permit data: no public price; custom yearly license; 335M records, 3,144 counties,
  20 years, weekly; permits are not in the Dewey ATTOM license (Dewey's permit partner is Builty).
  Quote requested [date sent]. Shovels ($599/mo API, custom bulk) is the fallback.
- Say: "If ATTOM permits are affordable they replace the Builty screen and merge on the ATTOM ID we
  already have; the question is coverage, and we have asked for a one-county sample to check."

## Tab 3. Retrofit vs new construction  (new figure in empirical facts)

- Figure: `output/figures/figure_4_retrofit_vs_new_construction.pdf`; sheet `retrofit_vs_new` in
  `output/tables/empirical_facts.xlsx`.
- Say: "Retrofits dominate: 2,449 of 3,082 in FL, 1,568 of 1,676 in LA, 1,087 of 1,153 in TX. We keep
  new construction now instead of dropping it; Florida's new-build bar is an upper bound because
  raised-slab permits on new houses still leak through the screen."

## Tab 4. Mitigation cost  (new figure)

- Figure: `output/figures/figure_5_mitigation_cost.pdf`; sheet `mitigation_cost`.
- Say: "A Builty retrofit permit declares a median project value of $37k; FMA's federal share per
  elevated home has a median of $193k and sits in a tight $100-300k band. Permit valuations are what
  applicants declare for fees, not what the elevation costs; LA permits carry no value at all."

## Tab 5. Elevation is rare at every level of risk  (existing Figure 1, rerun on the new panel)

- Figure: `output/figures/figure_1_mitigation_is_rare.pdf`; sheet `risk_group`.
- Say: "Among the 5.1M NFIP properties with an assigned ATTOM record and a Builty measure, 0.7 per 1,000
  carry an elevation permit: 1.3 in the SFHA against 0.3 outside, 3.2 with any paid claim, 4.3 for
  repetitive-loss and 6.8 for severe-repetitive-loss properties. Rare everywhere, rising with risk."
- Numbers (per 1,000, 95% CI half-width): all 0.72 (0.02); outside SFHA 0.32; SFHA 1.25 (0.05); any claim
  3.18 (0.25); $100k+ paid 4.34 (0.46); RL 4.31 (0.60); SRL 6.84 (1.79). 3,694 elevated of 5,146,355.
- Caveat to say once: rates are over properties carrying a Builty measure, restricted to county-years
  with a feed.

## Tab 6. Elevation vs realized losses  (existing Figure 2, rerun)

- Figures: `figure_2a_low_vs_high_flood_zone.pdf`, `figure_2_mitigation_by_realized_loss.pdf`.
- Say: "Elevation rises with block-group losses but stays under 4 per 1,000 even in the top decile:
  0.3 in zero-loss block groups, 0.5-1.0 through decile 9, 3.4 in the top decile, where losses run
  $34k per insured property." Correlation of SFHA and elevation 0.017 (N = 5.1M, p < 0.001):
  positive, tiny.
- Numbers: 4,824 zero-loss block groups (0.28 per 1,000); deciles 1-9 range 0.14-0.98; decile 10 3.36
  (CI 3.18-3.55), 1,332 elevations among 395,884 properties.

## Tab 7. What the matching now delivers  (new, short)

- Table: tier counts from the 2026-09-07 rerun (in this note's companion, `matching_rerun_2026-09-07`).
- Say: "88% of FL's and 85% of TX's Builty-elevated links are in block-group × flood-zone cells. LA is
  different: 88% of its elevated links are block group × zone without a construction year, because ATTOM
  lacks year built there; that is still a tight cell, so 'tight' means tiers 1-4 or 15."
- Numbers: elevated links FL 1,684 (1,481 in tiers 1-4), TX 904 (765), LA 1,272 (64 + 1,119 in tier 15),
  NJ 36 (32). Backfill from the Builty geocode moved 21 of 31 previously unplaceable FL/TX houses into
  tiers 1-4.

## Tab 8. FOIA status  (existing tab, update the rows)

- LA: PRR-2026-38 acknowledged Jul 14; follow-up sent [date]. TX: TDEM PIR-3390 covered 7 of 38
  HMGP/PDM projects, follow-up + CSV sent [date]; FMA is TWDB's, new PIA request sent [date]. NJ:
  unanswered portal request, resubmitted [date]. FL: $1,330 quote, decision pending.

## Parsimony rule for the rewrite

One sentence per exhibit above is the slide's title. Anything not in that sentence goes in the notes
column, not on the tab. Cut the old NFIP-elevations and alt-property-value tabs to the archive section;
their point (NFIP misrecords elevations; ATTOM is fine) is now one line under Tab 1.

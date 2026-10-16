# Two-vintage block-group match: alternate comparison (2026-09-21)

Alternate, not pipeline (Anna): the production matcher keys block-group cells on ATTOM's 2020
code only; the alternate (`build/alternates/{nfip_attom,attom_geocode,geocode_attom}_vintage.py`)
carries ATTOM's 2010 code too and matches each NFIP home on the vintage its own code belongs to,
2010 first for homes first insured before 2021, 2020 first otherwise, the other as fallback.
Cluster job 2026-09-21, steps 1-4, FL LA TX. Files are local only: `data/alternates/vintage/`
(links, values, panel) and `output/alternates/vintage/` (tables, figures). Production links on
Dropbox are the 09-19 ones and were not touched.

## Links, production vs alternate

| | FL prod | FL alt | LA prod | LA alt | TX prod | TX alt |
|---|---|---|---|---|---|---|
| NFIP homes matched | 3,180,423 | 3,185,150 | 704,064 | 794,863 | 1,980,361 | 1,981,526 |
| Match rate | 85.8% | 86.0% | 56.6% | 63.9% | 91.6% | 91.7% |
| Block-group tiers 1-4 | 1,538,802 | 2,080,120 | 126,091 | 166,727 | 716,402 | 1,216,829 |
| ZIP tiers 5, 6, 12 | 1,037,564 | 392,000 | 157,412 | 67,969 | 778,248 | 173,757 |
| Block group, no year (15) | 7,615 | 12,543 | 275,748 | 366,836 | 6,174 | 8,510 |
| Builty-flagged homes | 1,913 | 1,907 | 1,369 | 1,369 | 941 | 914 |
| Hit via 2010 / 2020 / neither | | 1.91M / 0.45M / 0.82M | | 581k / 99k / 114k | | 1.30M / 0.28M / 0.40M |
| Same ATTOM house as production | | 769k | | 117k | | 409k |
| Different house | | 2.31M | | 491k | | 1.52M |
| Newly matched / lost | | 106k / 101k | | 187k / 96k | | 53k / 52k |

## Does the finer cell buy a better house?

NFIP building coverage is chosen from the home's value, so the correlation between the assigned
ATTOM value and building coverage in the first policy year is an external yardstick that does
not use the match itself.

| | FL prod | FL alt | LA prod | LA alt | TX prod | TX alt |
|---|---|---|---|---|---|---|
| Pearson, log value on log coverage | 0.133 | 0.156 | 0.232 | 0.237 | 0.240 | 0.262 |
| Spearman | 0.124 | 0.178 | 0.284 | 0.298 | 0.278 | 0.325 |

Higher in every state on both measures. The level is low either way (coverage is capped at
$250k and many pre-FIRM homes carry round-number coverage), so the gain is in the ranking, not
in explaining levels.

## Reading

- Louisiana gains 91,000 matches outright; Florida and Texas gain almost none in total but move
  half a million homes each from ZIP cells into block-group cells.
- Most matched homes now sit on a different ATTOM house than in production (FL 75%, LA 81%,
  TX 79%). That is the arbitrary-within-cell pairing: cells changed, so the draw changed. Every
  cell-grain statistic (values, Builty flags, zone) is what the analysis uses, and those move
  in the direction the coverage check says is better.
- Builty-flagged homes are unchanged in LA and down by 6 in FL and 27 in TX: a permit house
  whose new block-group cell has no unmatched NFIP home can miss, where its ZIP cell used to
  catch it.
- Cost: every state's assignments change, so adopting it means every downstream number moves
  once. Not adopted; kept as the comparison for the deck.

Panel-level comparison (summary table, elevations by state, event study) from the alternate
panel `data/alternates/vintage/root/analysis/analysis.dta` follows in the same folder.

## Panel-level comparison (alternate panel via complete.do on the alternate root, 09-21 evening)

| | Production | Alternate |
|---|---|---|
| ATTOM-matched share, SFHA sample | 73.5% | 74.8% |
| Mean property value | $387k | $368k |
| Claims over value, claimants | 0.59 | 0.64 |
| Permit homes on the SFHA sample: FL / LA / TX | 1,327 / 969 / 612 | 1,377 / 770 / 626 |
| Events per 1,000: FL / LA / TX | 0.93 / 3.01 / 2.25 | 0.95 / 2.69 / 2.28 |
| Elevation cost N, mean | 538, $206k | 564, $210k |
| Premium event study t0 / t4 / t+5 | +$2 / +$13 / -$113 | +$14 / +$34 / +$67 |
| Claims event study t0 / t4 | -$10.6k / -$13.7k | -$11.7k / -$13.7k |
| Same-flood excess claim before / after | +$40.6k / -$0.7k | +$33.0k / +$0.0k |
| Two-way FE pre-trend p: premium / claims / any claim | 0.12 / 0.09 / 0.01 | 0.71 / 0.26 / 0.25 |

Most of it moves the way the link comparison predicts: more matched homes, cleaner control
group (pre-trend tests no longer reject), same claims profile, the premium still flat.

**One artifact, Louisiana permit events 969 -> 770.** The 1,369 Builty-flagged NFIP ids are the
same count in both versions and equally often in the SFHA (1,094 vs 1,088), but in the
alternate 1,001 of them ever sit in a Builty-covered county-year against 1,224 in production,
so 199 more permit homes have their flag set missing as "never observable". Cause: the vintage
preference sends permit houses to the older NFIP ids in their block group (homes first insured
by 2012: 44% of the flagged ids in production, 65% in the alternate; median policy years 6 vs
4), and older Louisiana ids often ended before the parish had a Builty feed. The pairing inside
a cell is arbitrary in both versions, so neither is "right"; the alternate just happens to draw
ids that cannot show a permit.

**Fix if adopted.** When the ATTOM house is a permit house, rank the NFIP homes in its cell so
that ids alive in the permit year (first policy year <= permit year <= last) come first, instead
of the hash order. That is a few lines in the cell ranking of `nfip_attom.py` (or the alternate),
applies to production as well, and would raise the permit count in both versions. Without it
the alternate lowers Louisiana's headline rate for a reason that has nothing to do with the
match being worse.

## Recommendation

Adopt the two-vintage match together with the permit-aware ranking, in one cluster run, before
results are final. Values and cells improve in every state; the Louisiana permit count only
falls because of the ranking artifact, which the second change removes.

## Permit-aware ranking, Louisiana test (09-22)

Built into the alternate matcher only (`nfip_attom_vintage.py`, `--policy-years` or a
`nfip_policy_years.parquet` beside the properties file). Tier counts do not change; what
changes is which NFIP home in the cell the permit house lands on.

| Louisiana permit homes (1,369 in all three) | Production | Two vintages | Two vintages + ranking |
|---|---|---|---|
| Insured in the permit year | 52% | 29% | 65% |
| Ever in a parish-year with a Builty feed | 1,224 | 1,001 | 1,351 |
| On the SFHA analysis sample | 969 | 770 | 1,063 |

The ranking more than repairs the two-vintage loss: 1,063 permit homes reach the sample
against 969 in production, because production's blind draw also put a third of permits on
homes that were not insured when the permit was issued. The remaining 35 percent not insured
in the permit year are cells with no such home available.

## Final alternate: two vintages + permit-aware ranking, panel level (09-22, cluster step 4 rerun)

Run against Anna's revised descriptive scripts of 09-22 (summary table with medians, state
table with a shares file, event study writing figures by source), so production was rerun on
those too. Both panels are otherwise built the same way.

| | Production | Two vintages + ranking |
|---|---|---|
| Permit homes on the SFHA sample: FL / LA / TX | 1,327 / 969 / 612 | 1,358 / 1,070 / 621 |
| Events per 1,000 homes: FL / LA / TX | 0.93 / 3.01 / 2.25 | 0.94 / 3.17 / 2.25 |
| Elevation retrofits, all / pre-FIRM / post-FIRM | 4,993 / 3,543 / 1,450 | 5,109 / 3,507 / 1,602 |
| Elevated ex ante | 450,281 | 450,304 |
| Property value, median | $242k | $238k |
| Retrofit cost, median | $146k | $143k |
| Premium event study, t+1 / t+5 | -$137 / -$357 | -$104 / -$326 |
| Claims, t0 (flood year) / t+5 | +$46.0k / -$6.7k | +$34.8k / -$8.6k |
| Any claim, t0 / t+5 | +0.27 / -0.04 | +0.22 / -0.06 |

Same picture in both: the premium falls after the permit, claims spike in the permit year and
fall below the pre-period afterwards. The alternate has 116 more retrofits on the sample, 101
of them in Louisiana, smaller flood-year spikes (the permit homes are a little less
flood-concentrated once they are the homes actually insured in the permit year) and slightly
tighter standard errors on the premium path. Links and value files for this version are in
`data/alternates/vintage/`, the two-vintage-only version in its `two_vintage_only/` subfolders.

Recommendation unchanged: adopt both changes together when the ladder is next touched.

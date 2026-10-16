# Louisiana ATTOM valuation check (TODO section 0 item, 09-15)

Claude, 2026-09-20. Source: `build/nfip_attom_pipeline_v2/geocoded/la_attom_geocoded.parquet`
(40.5M property-years, 2.78M properties, tax years 2002-2024), fields `market_value_total`,
`assessed_value_total`, `last_sale_price`, `last_sale_date`. No cluster run was needed.

## 1. Is `market_value_total` an assessed-type figure in the low parishes?

No. Louisiana assesses residential property at 10 percent of fair market value, and the ATTOM
market field equals ten times the assessed field almost everywhere: statewide median ratio 1.00,
and 85-98 percent of property-years within 10 percent of it in every large parish, including the
four flagged on 09-15 (Caddo 22017: 93%, Ouachita 22073: 90%, Rapides 22079: 92%,
Calcasieu 22019: 88%). The two fields carry the same information; the market field is the
assessor's fair market value and the $10k floor is not hiding a units problem.

Exceptions: St. Helena (22091, 96k rows) stores the market field at the assessed level for 65
percent of property-years (ratio 0.1); Claiborne (22027) and Evangeline (22053) 20 percent,
St. Landry (22031) 11 percent, 02231 26 percent. Set the market value missing, or multiply by
ten, where `market_value_total / (10 * assessed_value_total)` is between 0.09 and 0.11.

## 2. How far below market is the assessor's value?

Property-years with a recorded sale within one year of the tax year and a price of at least
$20k (2.0M sales): median market value over sale price.

| Parish | Sales | Market / price | Share below 0.5 | Median price |
|---|---|---|---|---|
| Statewide | 2,021,105 | 0.85 | 0.22 | |
| Jefferson 22051 | 188,666 | 0.95 | 0.11 | $185k |
| East Baton Rouge 22033 | 246,598 | 0.92 | 0.20 | $210k |
| Rapides 22079 | 64,270 | 0.92 | 0.22 | $142k |
| Orleans 22071 | 145,919 | 0.88 | 0.16 | $225k |
| St. Tammany 22103 | 166,006 | 0.82 | 0.14 | $215k |
| Caddo 22017 | 89,984 | 0.77 | 0.27 | $165k |
| Ouachita 22073 | 63,852 | 0.75 | 0.31 | $160k |
| Calcasieu 22019 | 81,163 | 0.69 | 0.32 | $165k |
| Lafayette 22055 and others | | 0.8-0.9 | | |

Smaller rural parishes run lower (St. Helena 0.24, Claiborne 0.28, St. Martin 22085 0.43,
Cameron 22023 0.43). Among homes that sold, the market field in Caddo is $103k at the median,
against a parish-wide median of $39k: the low parish medians come from the unsold, cheap and
stale-assessed stock (Louisiana reassesses on a four-year cycle), not from the field.

## Decision

Use `market_value_total` for LA as now, with two changes: (a) blank or rescale the ratio-0.1
property-years (mainly St. Helena); (b) state in the data appendix that LA assessor values run
15 percent below sale prices statewide and 25-30 percent below in Caddo, Ouachita and
Calcasieu, versus 5-10 percent in Jefferson and East Baton Rouge. If LA value levels matter for
a result (Q2 value quintiles are within county, so they do not), a parish x year sales-ratio
adjustment from this table is available; it is not needed for within-parish rankings.

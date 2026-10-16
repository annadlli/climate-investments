First pass on the locality-level coverage question (`build/alternates/builty_coverage_jurisdiction.py`, test script, run by hand; results in `output/tables/builty_coverage_crosswalk_compare.xlsx`).

**Crosswalk.** Builty `LOCALITY` → NFIP rated community number within state, three rules in order: municipal name (X, CITY/TOWN/VILLAGE OF), county/parish name, else the locality's modal county's community. The community list comes from the raw per-state policy files (state, cid, name, county), no FEMA download. Share of permits matched by the municipal rule: FL 85%, TX 98%, LA 58% (41% of LA permits fall to the parish rule, Metairie-type mailing cities); unmatched < 0.1%.

**Events per 1,000 insured SFHA homes, county-year coverage vs community-year coverage** (events counted in observed policy years):

| State | Coverage grain | Homes | Events / 1,000 | Builty-only / 1,000 |
|---|---|---|---|---|
| FL | county | 2.17M | 0.64 | 0.20 |
| FL | community | 2.06M | 0.59 | 0.21 |
| LA | county | 602k | 2.62 | 0.95 |
| LA | community | 309k | 3.06 | 1.58 |
| TX | county | 517k | 1.57 | 0.38 |
| TX | community | 182k | 1.50 | 0.58 |

**Reading.** The stricter grain raises the Builty permit rate by about half in LA and TX and leaves FL flat, at the cost of half to two thirds of the LA and TX denominators. The all-source rate barely moves because NFIP-flag and ICC events are spread across every community, covered or not. So community coverage sharpens the permit-only rate but does not change the order of magnitude (0.1 to 0.3 percent).

**If adopted:** keep `nfipratedcommunitynumber` in `merge_nfip_hma.do` and move the crosswalk into `clean_builty_coverage.py`. The weak rule is the third one (mailing city in an unincorporated county). I would leave county coverage as the default and report the community version as a robustness column unless we want the permit-only rate as the headline.

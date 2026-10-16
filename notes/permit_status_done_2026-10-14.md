# "Actually done" elevations from the ATTOM permit status (snapshot rule) — 2026-10-14

Claude change 10-14. Anna: keep only elevations that were actually done. Script:
`code/descriptives/scratch/permit_status_done.py` (run by hand; writes
`output/tables/permit_status_done.xlsx`). Input: `clean/permit_elevations.dta` (Oct 8 all-state
file), FL LA TX only. The status is the one snapshot string ATTOM carries on the permit record
(`status`, first permit at the address); the dated status history (`year_finaled`, `completed`)
is not on the file yet because the four Dewey status tables have not been downloaded.

## Classification

| Bucket | Counted as | Status values (lower-cased) |
|---|---|---|
| done | actually done, kept | complete, completed, completed/final, final, finaled, finalled - processing refund, permit completed, done, event complete, inspections completed, final inspection complete, passed, admin closeout, coc issued, c.o. issued, co''ed, coed, closed - coc issued, closed - co issued, **closed, permit closed, closed (permit)** |
| live | not done, dropped | issued, approved, applied, filed, triage, plancheck / plan check, open, active, new, permit picked up, permit printed, in progress, inspection / inspections commenced, every "pending", "waiting", "review", "resubmit" value, stop work, hold, suspended, reinstated, final approval (plan approval), certificate of appropriateness, blank |
| dead | not done, dropped | expired, canceled / cancelled, withdrawn / withdrew, rejected, revoked, void, invalid, closed - void, entered in error, permit inactive |

Assumption to confirm: the plain "closed" family (closed, permit closed, closed (permit)) is
counted as done because the feeds carry "closed - void" as its own value, so a bare "closed" is
the completed closure. It is 881 addresses (FL 556, LA 318, TX 7); flipping it to "live" moves
those from remaining to dropped.

## Counts (addresses, FL LA TX)

All elevation addresses:

| State | Original | Dropped | of which live | of which dead | Remaining | % remaining |
|---|---|---|---|---|---|---|
| FL | 8,543 | 3,179 | 2,494 | 685 | 5,364 | 62.8 |
| LA | 6,179 | 3,951 | 3,470 | 481 | 2,228 | 36.1 |
| TX | 2,421 | 1,331 | 951 | 380 | 1,090 | 45.0 |
| All | 17,143 | 8,461 | 6,915 | 1,546 | 8,682 | 50.6 |

Retrofit addresses only (elevated new construction excluded):

| State | Original | Dropped | of which live | of which dead | Remaining | % remaining |
|---|---|---|---|---|---|---|
| FL | 7,416 | 2,714 | 2,092 | 622 | 4,702 | 63.4 |
| LA | 5,647 | 3,596 | 3,245 | 351 | 2,051 | 36.3 |
| TX | 2,227 | 1,238 | 868 | 370 | 989 | 44.4 |
| All | 15,290 | 7,548 | 6,205 | 1,343 | 7,742 | 50.6 |

## What the drop is made of

- "issued" alone is 4,305 of the 6,915 live addresses (LA 2,154); approved 866, applied 607,
  triage 328, filed 317.
- Live permits are recent: median permit year FL 2023, LA 2019, TX 2020, against 2013-2014 for
  the done bucket; 1,688 of the live addresses are 2025-2026 permits, work plausibly still in
  progress. So the rule removes the most recent cohorts almost entirely, which matters for the
  event study (cohorts 2012-2022 are less affected) and for any count by year.
- Dead permits sit in the middle: median FL 2019, LA 2017, TX 2013.
- Several permits at the address: 520 FL / 447 LA / 179 TX of the live addresses carry more than
  one permit, and the snapshot status is the first permit's. The dated history will say whether a
  later permit at the address was finaled; until then those are dropped.
- LA loses the most (64%) because its feeds leave permits at "issued": that is the Oct 7 snapshot
  finding (LA 37% still "issued") again.

## ATTOM's standard status vocabulary (Anna, 10-14)

ATTOM's documentation of the Building Permit Statuses dataset lists 15 standard values and says
the primary table's `STATUS` is "the raw last-known label; use the Statuses dataset when you need
comparable stages or timing". Typical path: APPLIED -> ISSUED -> FINAL or COMPLETE.

| Standard value | ATTOM meaning | Bucket here |
|---|---|---|
| APPLIED | application submitted | live |
| FILED | application recorded / on file | live |
| TRIAGE | initial intake and routing review | live |
| PLANCHECK | plans under review | live |
| APPROVED | approved, pending issuance | live |
| ISSUED | permit issued; work may begin | live |
| FINAL | final inspection passed | done |
| COMPLETE | permit closed as complete | done |
| EXPIRED | lapsed before completion | dead |
| CANCELED | canceled by the jurisdiction or applicant | dead |
| WITHDRAWN | withdrawn by the applicant | dead |
| REJECTED | application denied | dead |
| SUSPENDED | put on hold | live |
| REVOKED | issued permit revoked | dead |
| REINSTATED | previously expired or revoked permit reinstated | live |

Two fixes in `prepare/extract_attom_permits.py` against this table (Claude change 10-14,
unit-tested on four toy histories, not yet run on data): SUSPENDED left the kill set for
`DATE_EXPIRED`, and a REINSTATED row dated after the last kill clears it, so a permit that
expired, was reinstated and then finaled counts as done with no kill year. `DATE_FINALED`
(first FINAL or COMPLETE) and `completed` in `clean_permits.do` already matched the table.
The raw snapshot strings (closed, coc issued, ...) are not standard values, which is why the
snapshot rule above needs its own list; once the history is on the file, `status_last` carries
the standard labels and the same bucket() applies.

## Not yet done

- The rule is not applied anywhere in the pipeline. Where it would go: `complete.do` carries
  `status` (or a `permit_done` flag built from it) onto `analysis.dta`, and the analysis scripts
  restrict `elevation_retrofit` to done permits as a sample rule, per the 09-15 decision that
  `complete.do` restricts nothing.
- The snapshot rule is replaced by `completed` / `year_finaled` once the status tables are on
  the cluster (TODO 10-12 item); the finaled year is also the candidate event date.

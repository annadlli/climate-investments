"""
Authors: Claude (for Anna Li)
Date: 2026-10-14
Claude change 10-14 (new file; Anna: "I want to keep actually done elevations. Give me your
classification of what you count as actually done and what you count as not, then record the
number dropped, the number remaining and the original total").

Classifies the snapshot permit status on clean/permit_elevations.dta into three buckets and
tabulates elevation addresses in FL LA TX by bucket. Scratch script, run by hand, not in master.do.

Buckets (the status is the one permit record ATTOM carries per address, lower-cased and trimmed):
  done  - the permit reached completion: final / finaled / complete / completed, a certificate of
          occupancy or completion was issued, inspections completed, passed, done, admin closeout,
          and the plain "closed" family (closed, permit closed, closed - co/coc issued).
          ASSUMPTION flagged separately below: plain "closed" is counted as done because the feeds
          carry "closed - void" as a distinct value, so a bare "closed" is the completed closure.
  live  - the permit never reached completion in the snapshot but was not killed either: issued,
          approved, applied, filed, open, active, in review, plan check, triage, inspections
          commenced, pending anything, waiting on anything, resubmit anything, stop-work, hold,
          suspended, reinstated, blank.
  dead  - the permit was killed: expired, cancelled, withdrawn, rejected, revoked, void, invalid,
          inactive, entered in error, closed - void.
"Actually done" = done. live and dead are dropped. The dated status history (year_finaled,
completed) is not on the file yet (status tables not downloaded), so this is the snapshot rule.

Claude change 10-14 (later; Anna shared ATTOM's documentation of the Building Permit Statuses
dataset): the 15 standard values and the bucket each one takes here.
  done: FINAL (final inspection passed), COMPLETE (permit closed as complete)
  live: APPLIED, FILED, TRIAGE, PLANCHECK, APPROVED (approved, pending issuance), ISSUED (work may
        begin), SUSPENDED (put on hold), REINSTATED (expired or revoked permit reinstated)
  dead: EXPIRED, CANCELED, WITHDRAWN, REJECTED, REVOKED
The primary table's STATUS is "the raw last-known label", which is why the raw feed strings
above (closed, coc issued, ...) also need a rule; the standardized labels arrive with the
status history (status_last) and run through the same bucket() unchanged.

Output: output/tables/permit_status_done.xlsx (sheets: by_state, classification).
Usage: python permit_status_done.py --data <Data root> --output <output root>
"""
import argparse, re
import pandas as pd

DONE = {
    "complete", "completed", "completed/final", "final", "finaled", "finalled - processing refund",
    "permit completed", "done", "event complete", "inspections completed", "final inspection complete",
    "passed", "admin closeout", "coc issued", "c.o. issued", "co''ed", "coed", "closed - coc issued",
    "closed - co issued", "closed", "permit closed", "closed (permit)",
}
DEAD = {
    "expired", "canceled", "cancelled", "withdrawn", "withdrew", "rejected", "revoked", "void",
    "invalid", "closed - void", "entered in error", "permit inactive",
}
CLOSED_PLAIN = {"closed", "permit closed", "closed (permit)"}   # the flagged assumption

def bucket(s):
    s = re.sub(r"\s+", " ", str(s).strip().lower())
    if s in DONE: return "done"
    if s in DEAD: return "dead"
    return "live"   # issued / approved / applied / filed / open / review / pending / blank / etc.

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True); ap.add_argument("--output", required=True)
    ap.add_argument("--states", default="FL LA TX")
    a = ap.parse_args()
    df = pd.read_stata(f"{a.data}/clean/permit_elevations.dta")
    df = df[df.state.isin(a.states.split())].copy()
    df["bucket"] = df.status.map(bucket)
    df["closed_plain"] = df.status.str.strip().str.lower().isin(CLOSED_PLAIN)

    def table(d, label):
        t = d.groupby("state").agg(original=("bucket", "size"),
                                   done=("bucket", lambda x: (x == "done").sum()),
                                   dropped_live=("bucket", lambda x: (x == "live").sum()),
                                   dropped_dead=("bucket", lambda x: (x == "dead").sum()),
                                   of_which_closed_plain=("closed_plain", "sum"))
        t.loc["All"] = t.sum()
        t["dropped"] = t.dropped_live + t.dropped_dead
        t["remaining"] = t.done
        t["pct_remaining"] = (100 * t.remaining / t.original).round(1)
        t.insert(0, "sample", label)
        return t[["sample", "original", "dropped", "dropped_live", "dropped_dead", "remaining",
                  "pct_remaining", "of_which_closed_plain"]]

    out = pd.concat([table(df, "all elevation addresses"),
                     table(df[df.retrofit == 1], "retrofit addresses")])
    cls = (df.groupby(["bucket", "status"]).size().rename("addresses").reset_index()
             .sort_values(["bucket", "addresses"], ascending=[True, False]))
    with pd.ExcelWriter(f"{a.output}/tables/permit_status_done.xlsx") as xw:
        out.to_excel(xw, sheet_name="by_state")
        cls.to_excel(xw, sheet_name="classification", index=False)
    pd.set_option("display.width", 200)
    print(out.to_string())
    print("\nlive statuses with 20+ addresses:")
    print(cls[cls.bucket == "live"].head(12).to_string(index=False))

if __name__ == "__main__":
    main()

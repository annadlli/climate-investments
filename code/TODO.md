# TODO — climate-investments

_Last updated: 2026-07-16. Open handoff tasks + project state. Your coding agent surfaces the open
items when you open the project (see CLAUDE.md / AGENTS.md). Follow `CONVENTIONS.md` as you work._

## ⭐ THE MAIN THING: the whole pipeline must be reproducible from `master.do`

Any **data acquisition or cleaning that is part of the pipeline must run from `master.do`** — one
file, top to bottom. If a step only exists as a cluster job, a notebook, or a one-off, it is **not**
reproducible. Wire it in as a `.do`/`.py` step behind a `0/1` switch. This is the bar everything below serves.

## Clean up `torch_work/` (Anna)

A mix of deprecated and active code. Reorganize so nothing pipeline-relevant is stranded:
- **Deprecated files → `torch_work/archive/`** (or delete).
- **Split the rest into `clean/` or `build/` by purpose:**
  - data **acquisition** (Dewey/ATTOM/Builty pulls, raw downloads) → `clean/` (acquisition lives in
    the clean stage; see `import_dewey.ipynb` there).
  - data **construction** (strict filtering, ATTOM matching, panel building) → `build/`.
- Make each moved script reproducible from `master.do` (`.do` → `args data`; `.py` → `--data`; no
  hardcoded `/scratch/...` or `/Users/anna/...` paths in the logic).

**Cluster execution = thin wrapper, not separate logic.** The same scripts `master.do` calls should
run on Torch via a **cluster-specific shell/SLURM wrapper** that just supplies cluster resources/paths
— so each step runs both ways (locally via `master.do`, on Torch via the wrapper) from one source of
truth. Put that wrapper at the top level, **next to `master.do`** (e.g. `master_torch.sh`).

**Streamline + document the pipeline in `master.do`** — every construction step there, in order,
documented, behind a `0/1` switch (including whatever comes out of `torch_work/`).

## Pending / reconcile

- [ ] **Trim `import_dewey.py` (Anna).** It's a faithful byte-for-byte downloader — verified it never
      parses or casts values, so it is **not** the source of the ATTOM ZIP/FIPS leading-zero loss (that
      defect is already in the Dewey-delivered parquet). But it reads AI-generated: cut the 7-key
      `file_name_from_row` probe (deweydatapy returns a known schema) and the verbose docstrings/type
      hints (~30–40 lines). **Keep** the `--manifest` path (it's the real interface — `master.do` /
      `import.sh` invoke it) and the retry / `.part` / `PAR1`-validation logic (justified for the 44 GB
      cluster downloads).
- [x] **`nfip_build.do` archived (2026-06-19).** Its claims→county-year output was consumed only by
      Gen-1 `build/archive/` scripts (not by `build_nfip_hma_panels.do`, which builds from **policies**),
      and it expected claims vars `clean_nfip_claims.do` doesn't produce. Moved to `build/archive/`
      pending the parked ∆D / claims work. SFHA enumeration there is noted in `clean_nfip_policies.do`.
- [x] **`import_dewey.py` source cleanup.** The notebook has been converted to a convention-style
      `.py` script with `--data`. Real Dewey endpoint URLs and API keys are excluded from git; use
      placeholder endpoint values in `clean/import_dewey.py` and fill them only in a secure/local
      run context.

## NFIP data — claims / policies / multiple-loss (in progress, 2026-06-12)

Division of labor (decided this session): **policies** = eligible-homes universe (NFIP-insured
single-family → FMA-eligibility sample restriction); **multiple-loss** = RL/SRL status + mitigation
(FMA prioritization); **claims** = ∆D / avoided damages (parked until later). Mirror NFIP cleaning on
[Wagner's repo](../../Wagner_repository) (Dropbox `Flooding/Empirical/Wagner_repository`) — see CLAUDE.md.

- [~] **`clean_nfip_claims.do`** — cleaned & **parked**. Single-family + match-key cleaning done
      (codebook-grounded; protects zero-padded keys from `destring`). Resume when building ∆D — needs a
      cell definition + a claims×policies exposure join.
- [x] **`extract_nfip_policies.py`** — DONE & run (closes the old reproducibility gap). duckdb extracts
      the 20 sample states from the 29GB `FimaNfipPoliciesV2.csv` in one scan → per-state
      `clean/nfip_policies_raw/{st}.csv` (60.7M rows, 22GB; FL 21.9M, TX 11.3M, LA 8.3M). Wired into
      `master.do` (`extract_nfip_policies` switch). Needs anaconda python (set `local python` to the full
      conda path — Stata's GUI PATH lacks conda; `pip install duckdb` done).
- [x] **`clean_nfip_policies.do`** — **built and run on all 20 states (2026-07-16)** →
      `clean/nfip_policies_state/{st}.dta`. Loops per-state raw CSVs → single-family screen →
      `property_id = group(geo · construction_year · originalNBdate)` where geo = census block group with
      ZIP fallback (beats Wagner's ZIP cell — ~0.7% collision on VA); `sfha` = first-letter `A`/`V`;
      drops SFHA homes (**see the SFHA tension above — this drop is now the main open design question**)
      + rows missing `property_id`; forces `elevated` monotonic within property.
      `build/prep_nfip_policies.do` then collapses to `clean/nfip_policies_property.dta`
      (**5,187,434 properties**, 313,161 elevated).
      **zipcode cleaned 2026-07-16:** 2,263 raw values were ZIP+4 (dashed `32413-7907` and undashed
      `700026926`), trailing dash/space, or had FEMA's own leading zero already stripped (581 rows, all
      in 0-prefix states NJ/MA/ME/RI/CT/VT). Repaired as strings + zero-padded, with an `assert` on the
      width. 186,800 leading-zero zips now survive; this moved FMA zip coverage by only ~400 properties,
      so it was a correctness fix, not a coverage one — but it matters for the ATTOM zip join, where the
      other side has the same defect.
- [~] **`clean_nfip_multiple_loss.do`** — import the MLP CSV
      (`fema.gov/api/open/v1/NfipMultipleLossProperties.csv`, ~240k rows) → `raw/`, **basic cleaning
      only**, save to `clean/`. **No sample restrictions** (it gets merged into policies, which already
      carry them). Use `fmaRl`/`fmaSrl` grant defs (not insurance defs) for RL/SRL status. The
      property/cell match moves to **build**, not here.
- [ ] **⚠️ SFHA handling — UNRESOLVED TENSION (surfaced 2026-07-16). The open question is not whether
      to drop SFHA, it's that dropping it inflates measured FMA treatment.**

      *Why we drop it:* isolates the full-BCA value-bias — SFHA elevations can use flat pre-calculated
      benefits, so they play by different rules. Still the goal.

      *Why it's a problem:* FMA money goes overwhelmingly **to** SFHA homes (that's where the risk and
      the RL/SRL properties are), and FMA can only be merged at **ZIP at best**. So a ZIP gets credited
      with FMA spending that was spent on SFHA homes **excluded from our sample**. The exposure measure
      is inflated relative to the population it's attached to, and we can't net it out: **neither FEMA
      file carries a flood zone**, so FMA spending cannot be decomposed into SFHA vs non-SFHA. The
      property-level FOIA link is the only real fix.

      *Measured (2026-07-16):* $58.5M — **5.8% of zip-localized FMA spending ($957.5M of $1,016.1M
      reaches a ZIP holding an NFIP property)** — lands in ZIPs with no NFIP property at all. Those are
      the ZIPs that are *entirely* floodplain (Dare County/Outer Banks, Terrebonne, Norfolk, Virginia
      Beach), so the SFHA drop removes them wholesale. That 5.8% is only the visible part; the larger
      effect is the *within-ZIP* thinning everywhere else, which leaves no trace.

      *Note on granularity:* SFHA is not a geography and doesn't nest in ZIP — it's the 1%-annual-chance
      floodplain from FEMA's FIRMs, drawn on hydrology, so it cuts across ZIPs, tracts, block groups and
      individual streets. In our data it's property-level (`ratedfloodzone`). So `drop if sfha == 1` is a
      *risk* filter, not a geographic one: the ZIP usually survives with its flood-exposed homes missing.

      *Options, none settled:* (a) accept and caveat — state that FMA exposure is measured with error
      that scales with a ZIP's SFHA share; (b) keep SFHA in the clean file and drop at analysis stage
      (see below), enabling a robustness spec that retains them; (c) control for the ZIP's SFHA share as
      a proxy for the mismatch; (d) wait for the FOIA property link.

      - [ ] **Move the drop to the analysis stage.** `clean_nfip_policies.do` currently does
            `drop if sfha == 1` *and* `drop sfha`, so the sample is **not** filterable either way — the
            opposite of what this file asked for, and recovering them costs a full 20-state re-run.
            Keep the rows and the flag; filter in `compile.do` or at estimation.
      - **Rated vs current zone is unresolved** (`clean_nfip_policies.do` flags it in a comment).
        `ratedfloodzone` is what the policy was *priced* on and can be grandfathered to a superseded map;
        `floodzonecurrent` is today's FIRM. Elevating a house can change its rating — so which one
        defines the sample is a design choice, not a toss-up.
      - **NFHL not needed** for the NFIP-insured universe (2026-06-19); would only be required to tag the
        non-insured ATTOM/Builty universe.

## Revive the Builty permit angle — NFIP's elevation flag may not be trustworthy (2026-07-16)

**Why (decided 2026-07-16):** we are not confident NFIP properly reports elevations, and the numbers
support the worry. In `nfip_policies_property.dta`: **313,156 properties (6.04%) are flagged elevated,
but only 2,998 (0.06%) show an observed transition** (`got_elevated`). So ~99% of elevated homes were
already elevated when first observed — the flag is a **stock, not a flow**, and the entire sample of
elevation *events* is 2,998 properties, the same order of magnitude as FMA's ~5,268 logged elevations.
Everything rests on `elevatedbuildingindicator` (insurer/self-reported) changing across policy years;
`clean_nfip_policies.do` forces it monotonic, which repairs flip-flops but cannot manufacture a
transition that was never reported. If reporting is lagged, missed, or correlated with FMA
participation, both the outcome and the timing are wrong.

**What Builty buys:** permit-level elevation events with **exact addresses and issue/final dates** — an
independent measure of whether and *when* a house was elevated, not conditional on an insurer updating
a field. Best use is probably as a **validation/cross-check** on the NFIP flag first (do permit-observed
elevations line up with NFIP transitions in the same cell and year?), and only then as a replacement
outcome if NFIP proves unreliable.

**Built 2026-07-16:** `clean/extract_builty.py` (duckdb: 163.3M-row `raw/builty_all.parquet` →
per-state candidates in `clean/builty_raw/{st}.csv`; wide text net, no judgement) + `clean/clean_builty.do`
(**WIP — has a `stop`**; splits Builty's newline-packed `DESCRIPTION` into `permit_subtype` +
`description`, then restricts to true elevations). Both wired into `master.do`. Funnel on TX+VA:
163.3M national → 24.6M TX+VA → 290,766 candidates → ~4,600 elevations.

**Superseded — don't revive these:**
- `build/archive/build_builty_filter.py` → **replaced by `clean/extract_builty.py` + `clean/clean_builty.do`.**
  It is also misfiled (a raw→clean step living in `build/`).
- `clean/all_builty_elevations.dta` (6.3GB, 1,784,540 rows) → **replaced by `clean/builty_permits_{st}.dta`.**
  **Its name is a lie**: it is `build_builty_filter.py`'s *loose* output (candidates incl. false
  positives — it is full of elevators, and its 24 columns are byte-identical to the raw parquet, so no
  cleaning ever happened). ~1.78M ≈ the raw text net alone. Delete it once Builty is settled; anyone
  reading the name will assume it holds finished elevations.

**Still Gen-1, unreviewed, needed only if the chain goes past the cross-check:**
`build/archive/`: `build_split_builty_states.py`, `build_attom_onto_permits.py`,
`build_fma_onto_builty_attom.py`, `parquetdta.py`, `build_nfip_hma_panels.do`; acquisition in
`clean/archive/builty.py` (superseded by `import_dewey.py`, which now carries the Builty endpoint).

- [ ] **Scope the cross-check first** before committing to the full chain: for TX/VA, do Builty
      elevation permits and NFIP `got_elevated` transitions agree in the same cell × year?
- [ ] **The link is not 1:1.** NFIP carries no address, so Builty↔NFIP can only be a cell match. The
      viable chain is Builty→ATTOM (address-level, exact) then ATTOM→NFIP (cell) — so this **depends on
      ATTOM**, which is TX/VA only and currently missing its census geography (see below).
- [ ] **Builty's ZIP is dirtier than NFIP's and mostly unrepairable.** 1,412,440 rows carry 1–4
      character `ZIPCODE` values (`1`, `01`, `001`), plus `00000` placeholders. Unlike NFIP's short
      ZIPs — provably leading-zero-stripped, since every one was in a 0-prefix state — these cannot be
      padded into anything real and need dropping, not fixing. `ZIPCODE` is VARCHAR, so its 8.8M
      leading-zero ZIPs are intact; keep it that way (CONVENTIONS §5).
- [ ] **Coverage is jurisdiction-dependent** — permits only exist where the locality reports them, so
      absence of a permit is not absence of an elevation. Quantify coverage before treating it as truth.

## Build → property-level analysis dataset (planned 2026-06-19)

**Status 2026-07-16: `compile.do` runs** — NFIP property base (5,187,434 properties, 20 states) +
multiple-loss + FMA at both grains → `analysis/analysis.dta`. No longer paused, no longer a VA sketch.
Remaining to reach the full analysis set: **ATTOM value cells** (`build/build_attom_value_cells.py`,
Torch wrapper `.sh`) → decide whether to extend `compile.do` or add a build file for the merge.

Open items:
- [ ] **`master.do:100` points at `clean/prep_nfip_policies.do`; the file is in `build/`.** The switch
      is broken — fix the path.
- [ ] **The `elevated` merge key is gone (2026-07-16).** `compile.do` used to merge FMA on
      `countycode elevated`, which gave FMA data only to the 313,161 elevated homes and left 4.87M
      non-elevated ones missing — no comparison group. It also couldn't do what it was meant to
      (identify FMA-funded elevations), because `elevated` was a *constant* in the FMA file, so it
      filtered rather than matched. Attributing individual elevations to FMA needs the FOIA property link.
- **OPEN: what's the best merge variable?** No shared `id` across NFIP files (0 overlap, by design).
  Best key = string `geo (block group / ZIP) · construction_year · originalNBDate` (~0.5% collision on
  VA, Wagner-style) — NOT the egen-integer `property_id` (numbered per-dataset, won't match across
  files). Open: is ~0.5% collision OK; tiered fallback for non-matches?; 1492 construction sentinel.
  **Next step: trial MLP↔policies merge, report match rate + 1:1 vs ambiguous.**
- **ATTOM is not a true property-key 1:1.** NFIP has no exact address (coarsened geo), so ATTOM links
  only at the **cell level** (zip/tract × construction-year). The "ATTOM 1:1" step is really a
  cell-level value join, not a parcel match. → property wealth ends up cell-level, not structure-level.
- **Stale `.dta` leftovers in `data/clean/`** from superseded versions — `fma_elevation_projects.dta`,
  `fma_elevation_properties.dta`, `fma_zip_county.dta`. Safe to delete; nothing reads them.

## FMA property-level pipeline — BUILT 2026-07-16

`clean_fma.do` now builds the FMA universe from both FEMA files: **HMA Mitigated Properties** (record
level, carries ZIP) as the base, with **HMA Projects** merged `m:1` on `projectidentifier` for dollars,
BCR and status. `prep_fma.do` pools to `fma_zip.dta` (555 zips) and `fma_county.dta` (191 counties);
`compile.do` attaches both grains to every property. Reference facts and open items that survive:

- **The Projects merge is the funding screen, not just an attribute join.** MitProps logs properties
  for applications that were **never funded** — 417 projects with literally $0 obligated (Denied 199,
  Pending 147, Withdrawn 47, …) carry 951 records / 2,191 "mitigated" properties. FEMA even populates
  `numberOfFinalProperties` for denied applications. **MitProps alone cannot tell you an elevation
  happened**; only the Projects status filter can. Pending (147 projects, 1,062 properties) is a
  data-vintage artifact — some will be funded later, and picking them up needs a re-pull, not a code change.
- **~~MitProps under-reports ~49%~~ — that was a records-vs-properties unit error** (3,001 *records* vs
  6,094 *properties*). Records batch multiple structures (mean ~2.4), so any count must sum
  `numberOfProperties`, never `count`. Corrected figure: after our status/elevation filters MitProps
  logs 5,268 structures against Projects' authoritative 5,588 — **under-logs by 320 (5.7%)**, not 51%.
  Nationally unfiltered it actually slightly *exceeds* Projects (6,316 vs 6,094).
- **`n_properties` in `fma_zip`/`fma_county` runs ~5.7% below the authoritative project totals** by
  design: it sums structures FEMA individually logged (`n_properties_rec`) so cells stay integers.
  Dollars are unaffected — the full $1.016B is apportioned across logged records. Caveat in writing up.
- **FEMA's project-header county is unreliable** — wrong on ~68 rows, and `county` is not among
  `projectCounties` on 1,095 project rows. e.g. City of Houston projects coded to Montgomery County.
  `clean_fma.do` therefore takes the **property's** county and uses the project's only as a fallback.
  One fixed typo: property county `Norton (city)` on a Norfolk city project (400 miles apart).
- **Scale is sparse:** 555 zips vs 191 counties; 238 zips hold a single record; TX 70 zips / 11 counties,
  VA 25 / 12. Thin at zip×year — may force pooling years or the county fallback for power.
- [ ] **Open: ZIP vs county grain.** Both are built and carried as separate variables
      (`fma_spend_zip`, `fma_spend_county`). ZIP reaches 24.4% of properties, county 72.4%. They are
      **alternative resolutions, not components — never add them.** Intended use: ZIP as treatment,
      county (or a county FE) as the control. Raw within-county contrast: treated zips 7.17% elevated vs
      untreated zips 4.19%; the naive across-county contrast points the wrong way (7.17% vs 8.30%).
- [ ] **Open: 12 of the 53 project-only grants can't be placed** (5 `Statewide`, 7 no county at all).
      41 land at their project-header county — real dollars at lower-quality geography. `project_merge`
      survives into `fma_elevation.dta` so their sensitivity can be tested.

## Expand state coverage beyond TX & VA (important)

**Done for NFIP + FMA (2026-07-16):** `local states` in `master.do` now runs all 20 sample states, and
`clean_nfip_policies` + `prep_nfip_policies` have been run across them (5.19M properties). FMA is
national already. **The binding constraint is now solely ATTOM:** only `raw/attom_{tx,va}.parquet` are
acquired, so anything needing property values is still TX + VA. Prioritize expanding ATTOM coverage.

## ATTOM geo enrichment is missing from the Dewey extract (Anna)

**The problem.** The ATTOM parquet extracts (`raw/attom/attom_{st}.parquet`) have their
census-geography and coordinate columns **100% empty**. Verified on VA (72.27M rows):
`CENSUSTRACT`, `CENSUSBLOCKGROUP`, `CENSUSBLOCK` (all `DECIMAL`) and `LATITUDE`/`LONGITUDE`
(all `DOUBLE`) have **zero** non-null values; `GEOQUALITYCODE` all blank. Only county FIPS,
ZIP, and street address are populated (address ~87% full, 100% house#+street; ~4.1M unique
`ATTOMID` parcels/state; `TAXMARKETVALUETOTAL` ~69%).

**This is a pull/product gap, NOT an ATTOM limitation.** ATTOM natively provides census
geo + lat/long (the columns exist in the 279-col schema). A clean 100%-empty across five
independent geo fields at once = the geocode/boundary enrichment module was never included
in the delivered feed. `import_dewey.py` downloads Dewey files wholesale (no column
filtering), so we received ATTOM's assessor/tax + address table without the geo enrichment.

**Why it matters.** With no census geo on the ATTOM side, `build_attom_value_cells.py` can
only aggregate to **ZIP/county × year** (median ~720 ATTOM homes per zip×year cell; 29% of
NFIP properties unmatched on VA). NFIP itself *has* block group (99.7% filled, 5,836 distinct
on VA), so if ATTOM carried block group we could merge at **block-group × construction-year**
— far closer to property-level and much less value-skew. The current coarseness is forced by
this gap, not a design choice.

**In progress (Anna, as of 2026-07-16): geocoding locally**, rather than re-pulling from Dewey.
Addresses go through a free API (Census Bureau batch geocoder or equivalent) → returns tract + block
group; slow to run, **~80% match rate**. Attach back by `ATTOMID`, then merge to NFIP's block group.

- [ ] **Check whether the ~20% non-match is selective.** Geocoder misses skew toward rural addresses,
      new construction and non-standard rural-route formats. Because ATTOM enters as *value cells*
      (a median over many parcels) rather than a 1:1 match, a random 20% loss only thins cells — it
      doesn't drop observations. The risk is a **wealth-correlated** miss, which would bias the cell
      medians in exactly the variable the project is about. Report match rate by county/ZIP and compare
      `TAXMARKETVALUETOTAL` for matched vs unmatched parcels; if they diverge, say so in the paper.
- [ ] **Plan a tiered fallback** for the ~20%: block group where geocoded, ZIP cell where not
      (the same tiered logic Wagner uses).
- [ ] **Pad, don't strip, when reconciling ZIP/FIPS.** ATTOM's arrive already zero-stripped; NFIP's are
      clean 5-char strings. Repair ATTOM upward — see CONVENTIONS §5.
- [ ] _(alternative, not being pursued)_ Re-pull the ATTOM geo/boundary table from Dewey — check the
      catalog for the geocode / "enhanced GeoID" deliverable, join on `ATTOMID`. Revisit only if the
      geocoder's match rate proves inadequate or selective.

_Confirmed empty on VA + TX (Apr 13 pull). The 19-state Jun 27 batch is the same wholesale
pull / same 279-col schema — spot-check one June state before assuming the whole batch differs._

## Deflate nominal dollars to real (CPI)

All dollar amounts in the data are **nominal** (current-year), and the sources span
~1996–2023, so any cross-year comparison or pooling needs deflation to constant dollars.

**Built (2026-07-01):** `clean/clean_cpi.do` (raw `data/raw/cpi.csv`) → annual `clean/cpi.dta`,
rescaled to **base 2023** (`cpi = 1` in 2023; deflate with `real = nominal / cpi`); wired into
`master.do` (`clean_cpi` switch). **Deflation now happens in `clean_fma.do`**, keyed on the obligation
year (`year_elev_min` was dropped 2026-07-16 — it was identical to the obligation year by construction,
since the obligation-year override made it so; `year_closed` is the window's other end). The dead CPI
block still sitting commented in `compile.do` refers to the removed `year_elev_min` — delete it.

- [ ] **Switch to the canonical series (BLS CPI-U, FRED `CPIAUCNS`).** Currently reuses the
      meatpacking file (OECD `CPALTT01USM661S` — tracks CPI-U closely but not the standard US
      choice). Swap the raw CSV + the `ren` column name in `clean_cpi.do`. **Do the same in the
      meatpacking project** so both use CPI-U consistently.
- [ ] **Guard incomplete years** in `clean_cpi.do`: the series ends mid-2025, so a 2025 annual
      average is a partial (Jan–Apr) figure — drop years with <12 months.
- [ ] **Deflate ATTOM property values** in `compile.do` once merged, by their value-cell year
      (`TAXYEARASSESSED`/`policy_year`) — same pattern as the FMA block.
- **Robustness option:** a construction-cost/PPI deflator may fit FMA spending better, and
  PCE / CPI-less-shelter avoids housing circularity for property values. General CPI-U is the
  documented default.

## Status / reference (done — for context)

- **Gen-1 build leftovers archived** → `build/archive/` (`merge_states/panel/nfip`, `merge_fema...`,
  `property_panel`, `merge_on_exact`, `property_data` + `nri_prep`, `merge_npr`). **Held:**
  `nfip_clean.do` (see reconcile). `weather_dewey_import.ipynb` (redundant dup) → `clean/archive/`.
- **`analysis/` + `descriptives/` are all Gen-1, archived** (they read data the pipeline no longer
  produces) → their `archive/` subfolders. Need rebuilding on the current `*_nfip_hma` panels. Closest
  to salvageable: `descriptives_all_states_fema.py` (`all_elevation_strict_filtered_fema.dta` →
  `build/all_builtyelevations_fema_npr.dta`).
- **Path convention:** `master.do` sets `local code` + `local data`; passes data to each child via
  `args data` (no fallback); Python steps take `--data`. See CONVENTIONS.md §3.
- **Vendela's cleanup** (folder reorg, args-pass, banners, archiving) is working-tree / branch
  `gen1-cleanup`, **not pushed** — coordinate before pulling so it doesn't clobber your work.

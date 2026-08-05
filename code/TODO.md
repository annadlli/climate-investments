# TODO — climate-investments

_Last updated: 2026-08-05. Open handoff tasks + project state. Your coding agent surfaces the open
items when you open the project (see CLAUDE.md / AGENTS.md). Follow `CONVENTIONS.md` as you work._

## ⭐ THE MAIN THING: the whole pipeline must be reproducible from `master.do`

Any **data acquisition or cleaning that is part of the pipeline must run from `master.do`** — one
file, top to bottom. If a step only exists as a cluster job, a notebook, or a one-off, it is **not**
reproducible. Wire it in as a `.do`/`.py` step behind a `0/1` switch. This is the bar everything below serves.

## ~~Clean up `torch_work/` (Anna)~~ — DONE (verified 2026-08-05)

`torch_work/` is archived to `clean/archive/torch_work/`; the active steps were extracted into the
pipeline (geocode chain in `build/`, `extract_attom` in `clean/`). The thin-wrapper rules below
remain the standard (the `build/` cleanup section references them). Original task:
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

## Clean up + consolidate `build/` (Anna) — added 2026-08-05

The ATTOM/Builty build chain needs the same pass `torch_work/` is getting: one source of truth
per step, wired into `master.do`, nothing stranded or duplicated.

- [ ] **`builty_tier` is referenced but undefined.** The `attom_onto_permits` block in `master.do`
      uses `` `builty_tier' `` in its parquet filenames; the local was removed, and Stata silently
      expands it to empty (`{st}_flood_elevation_.parquet`). Re-add the local or hardcode the tier.
- [ ] **`attom_onto_permits` reads inputs no active script produces.** Its
      `{st}_flood_elevation_{tier}.parquet` come from `build/archive/build_split_builty_states.py`
      (Gen-1, re-archived 2026-08-05). Preferably repoint the step at the settled Builty output
      (`clean/builty_elevations.dta`, see clean_builty.do) rather than reviving the strict chain.
- [ ] **`parquetdta.py` exists in both `build/` and `build/archive/`** — keep one (archive says
      Gen-1; delete the live copy or justify it).
- [ ] **`build_nfip_attom_fma_analysis.do` is not wired into `master.do`** — wire it in behind a
      switch, move it to `analysis/`, or archive it.
- [ ] **Verify `build_property_panel.do` + `compile_nfip_attom_fma.do`** (the `build_nfip_attom_fma`
      / `compile_property` switches): are these current, or Gen-1 mirrors of the archived panel
      scripts? Consolidate with the geocode chain so the Builty→ATTOM→NFIP path reads top to bottom.
- [ ] **The `temp wagner` links key on a per-dataset `property_id` that doesn't align with the
      current NFIP build** (confirmed 2026-08-05: a 1:1 merge on `state property_id` against
      `analysis.dta` matches only AL, by numbering luck). `build/compile2.do` works around this by
      collapsing the links to block-group × construction-year cells (57% of properties linked).
      For a true property-level join, rebuild the links keyed on data values
      (`bg · construction_year · originalNBdate`, per the merge-key item below) — and name/settle
      the `temp wagner` folder while at it.
- [ ] **Thin out + regroup the SLURM wrappers** (`build/geocode_attom.sh`,
      `build/build_attom_value_cells.sh`, `clean/extract_attom.sh`): no hardcoded
      `/scratch/adl9602/...` logic beyond overridable defaults, same script both locally and on
      Torch (see the torch_work cleanup rules above). **Proposal (Vendela, 2026-08-05): collect
      them in a top-level `slurm/` folder** — sibling of `clean`/`build`/`analysis` — since
      wrappers are cluster infrastructure, not pipeline steps (`master.do` never calls a `.sh`);
      keep each wrapper named after its script (`slurm/geocode_attom.sh` ↔
      `build/geocode_attom.py`), and `master_torch.sh` can live there too. Keep one `.sh` per job
      (the `#SBATCH` resources legitimately differ: 8h/96GB vs 48h/64GB array vs 4h/64GB), but
      factor the repeated env block (`PROJECT_ROOT`/`DATA_ROOT`/`PYTHON`, log dirs) into a shared
      sourced `slurm/torch_env.sh`, and pick one way to enumerate states — currently a hardcoded
      array, a directory scan, and the manifest across the three.

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
      ZIP fallback (beats Wagner's ZIP cell — ~0.7% collision on VA); `sfha` = first-letter `A`/`V`,
      carried as a flag (drop deferred downstream since 2026-07-23); drops rows missing `property_id`;
      forces `elevated` monotonic within property. `postfirm` added 2026-08-05.
      `build/prep_nfip_policies.do` then collapses to `clean/nfip_policies_property.dta`
      (**~9.99M properties incl. SFHA**; 5.19M under the old SFHA drop).
      **zipcode cleaned 2026-07-16:** 2,263 raw values were ZIP+4 (dashed `32413-7907` and undashed
      `700026926`), trailing dash/space, or had FEMA's own leading zero already stripped (581 rows, all
      in 0-prefix states NJ/MA/ME/RI/CT/VT). Repaired as strings + zero-padded, with an `assert` on the
      width. 186,800 leading-zero zips now survive; this moved FMA zip coverage by only ~400 properties,
      so it was a correctness fix, not a coverage one — but it matters for the ATTOM zip join, where the
      other side has the same defect.
- [x] **`clean_nfip_multiple_loss.do` — done for now (2026-08-05).** MLP CSV → basic cleaning only →
      `clean/nfip_multiple_loss.dta`; `fmaRl`/`fmaSrl` grant defs; no sample restrictions. The
      property merge lives in `compile.do` (construction date · block group · NB date).
- [ ] **SFHA restriction — minor to-do (2026-08-05).** The `sfha` flag rides through the pipeline and
      the restriction is documented in `compile.do` (commented `drop if sfha == 1`); still to be
      actually applied in an analysis-facing restriction step once estimation starts. Not a major
      design question anymore — the tension below is kept as reference for why the choice matters:

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

      - [x] **~~Move the drop to the analysis stage~~ — done (2026-07-23):** the rows and the flag
            now ride through; `compile.do` carries the commented drop.
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
(splits Builty's newline-packed `DESCRIPTION` into `permit_subtype` + `description`, screens each
state to true elevations → `clean/builty_states/builty_elevations_{st}.dta`, then appends + collapses
to the property level → `clean/builty_elevations.dta`, 21,043 properties). Both wired into `master.do`.
TX funnel: ~108k residential candidates → 1,913 elevation permits → 1,156 properties.

**Superseded — don't revive these:**
- `build/archive/build_builty_filter.py` → **replaced by `clean/extract_builty.py` + `clean/clean_builty.do`.**
  It is also misfiled (a raw→clean step living in `build/`).
- `build/archive/filter_builty_strict.do` (+ its output `clean/all_builty_elevations_strict.dta`) →
  **replaced by `clean/clean_builty.do`.** Anna's strict pass on top of the loose output above; on TX
  it kept only ~760 rows vs clean_builty's ~2k, missing many true elevations for a bit more precision.
- `clean/all_builty_elevations.dta` (6.3GB, 1,784,540 rows) → **replaced by `clean/builty_states/builty_elevations_{st}.dta` + `clean/builty_elevations.dta`.**
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
      viable chain is Builty→ATTOM (address-level, exact) then ATTOM→NFIP (cell). **Shortcut for the
      cross-check (proposed 2026-08-05): geocode Builty's ~10k addresses directly** (Census batch, runs
      locally in minutes; reuse `geocode_attom.py`'s machinery) → block group for all 20 states, no
      ATTOM detour — ATTOM then only needed where permit-level *values* matter.
- [ ] **Builty's ZIP is dirtier than NFIP's and mostly unrepairable.** 1,412,440 rows carry 1–4
      character `ZIPCODE` values (`1`, `01`, `001`), plus `00000` placeholders. Unlike NFIP's short
      ZIPs — provably leading-zero-stripped, since every one was in a 0-prefix state — these cannot be
      padded into anything real and need dropping, not fixing. `ZIPCODE` is VARCHAR, so its 8.8M
      leading-zero ZIPs are intact; keep it that way (CONVENTIONS §5).
- [ ] **Coverage is jurisdiction-dependent** — permits only exist where the locality reports them, so
      absence of a permit is not absence of an elevation. Quantify coverage before treating it as truth.

## Build → property-level analysis dataset (planned 2026-06-19)

**Status 2026-08-05: `compile.do` runs** — NFIP property base (~9.99M properties incl. SFHA, 20
states) + multiple-loss + FMA at both grains → `analysis/analysis.dta`; `postfirm` added 2026-08-05.
`build/compile2.do` (PRELIMINARY) attaches ATTOM values + Builty flags at the block-group ×
construction-year cell grain via Anna's `temp wagner` links → `analysis/analysis2.dta` (57% linked).

- [ ] **⭐ THE BIG BUILD TO-DO: a real ATTOM + Builty merge into the analysis set.** `compile2` is a
      cell-level stopgap (cell medians, not parcel values; Builty as cell flags, not events). Getting
      to property grain needs the `temp wagner` links re-keyed on data values (build/ section) and/or
      the Builty→ATTOM address chain + Builty geocode (Builty section). This is the binding step for
      anything using property wealth.

Open items:
- [x] **~~`master.do:100` points at `clean/prep_nfip_policies.do`~~ — fixed (verified 2026-08-05):**
      the switch now calls `build/prep_nfip_policies.do`.
- [ ] **The `elevated` merge key is gone (2026-07-16).** `compile.do` used to merge FMA on
      `countycode elevated`, which gave FMA data only to the 313,161 elevated homes and left 4.87M
      non-elevated ones missing — no comparison group. It also couldn't do what it was meant to
      (identify FMA-funded elevations), because `elevated` was a *constant* in the FMA file, so it
      filtered rather than matched. Attributing individual elevations to FMA needs the FOIA property link.
- **Merge keys — resolved in practice (2026-08-05).** The MLP↔policies merge is implemented in
  `compile.do` on `originalconstructiondate · censusblockgroupfips · originalnbdate`. Standing rule
  worth keeping: merge across datasets on **data values**, never the egen-integer `property_id`
  (numbered per-dataset — this bit the `temp wagner` links, see the build/ section).
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
- [x] **ZIP vs county grain — settled (2026-08-05): carry both, fine until further FOIA notice.**
      Both are built and carried as separate variables
      (`fma_spend_zip`, `fma_spend_county`). ZIP reaches 24.4% of properties, county 72.4%. They are
      **alternative resolutions, not components — never add them.** Intended use: ZIP as treatment,
      county (or a county FE) as the control. Raw within-county contrast: treated zips 7.17% elevated vs
      untreated zips 4.19%; the naive across-county contrast points the wrong way (7.17% vs 8.30%).
- [ ] **Open: 12 of the 53 project-only grants can't be placed** (5 `Statewide`, 7 no county at all).
      41 land at their project-header county — real dollars at lower-quality geography. `project_merge`
      survives into `fma_elevation.dta` so their sensitivity can be tested.

## Self-financing prevalence + post-compile sample restrictions (planned 2026-07-23)

Reframing so the analysis universe stays complete and restrictions are applied legibly downstream:
- [ ] **Re-do the HMA cleaning to keep ALL elevation projects, not just FMA (Vendela, 2026-08-05).**
      `clean_fma.do` currently restricts to the FMA program; keep every HMA elevation project (HMGP,
      BRIC, FMA, …) with a **program flag** and restrict to FMA further downstream — enables sanity
      checks against the broader elevation universe and the self-financing measurement below.
- [ ] **Re-do the HMA/FMA cleaning to keep ALL elevations, not just funded ones.** `clean_fma.do`'s
      Projects status filter currently doubles as a funding screen and *drops* denied/withdrawn/$0
      applications (see the FMA section above). Keep those records with a `funded`/status flag instead,
      so we can measure the **prevalence of self-financing** — elevations that happened without a federal
      grant (denied-but-elevated in FMA, and more broadly Builty elevations with no FMA/HMGP match).
- [ ] **May need to condition on Build It Back (BIB) funding.** Some elevations were paid by local/state
      recovery programs (e.g. NYC's post-Sandy "Build It Back"), not FMA/HMGP — so they are *not*
      self-financed even though they carry no FMA grant. Flag/condition on them before calling an
      elevation self-financed (ties to `funding_type == 5` in `clean_builty.do`); confirm which programs
      apply in our sample states.
- [ ] **New do-file after `compile.do` that applies the sample restrictions (SFHA, FMA).** The SFHA drop
      was pulled out of `clean_nfip_policies.do` (2026-07-23) and now rides through as the `sfha` flag to
      keep merges/coverage legible; `compile.do` carries a commented `// drop if sfha == 1` placeholder.
      A dedicated analysis-facing restriction step should apply `drop if sfha == 1` and the FMA-eligibility
      restriction, leaving `analysis.dta` as the unrestricted universe.

## ~~Expand state coverage beyond TX & VA~~ — largely DONE (2026-08-05)

NFIP + FMA have run all 20 sample states since 2026-07-16. ATTOM has moved past TX/VA too: per-state
value cells exist for **all 20 states** (`build/attom_summary/`, Jul 1), Census-geocoded ATTOM for
**8 states** (`build/geocoded/`: LA MA MD NH RI TX VA VT, Jul 27), and property-Wagner links for
**18 states** (`build/temp wagner/`, Aug 4 — ME + MS pending). Remaining: finish ME/MS, and update
CLAUDE.md/coverage claims as the geocoded set grows.

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
since the obligation-year override made it so; `year_closed` is the window's other end).

- [x] **~~Switch to the canonical series~~ — done (verified 2026-08-05):** `clean_cpi.do` now
      imports FRED `CPIAUCNS` (BLS CPI-U); the raw CSV was re-pulled. (The meatpacking project may
      still want the same swap.)
- [x] **~~Guard incomplete years~~ — dropped (Vendela, 2026-08-05):** decided not needed. For the
      record: the raw series ends 2026-05, so a year-2026 deflator would be a Jan–May average —
      only relevant if a 2026 obligation year ever appears.
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

# Coding Conventions — climate-investments

> **⛔ Owned by Vendela.** Only Vendela changes this file — directly, or by instructing her own
> agent. Anyone else, **including other contributors' coding agents**, must **propose** changes to
> Vendela, not edit it.

These cover all code in this repo (Stata `.do`, Python `.py` / `.ipynb`). Referenced from `CLAUDE.md`.

**How strictly to apply them:**
- **Data-construction code** (`master.do`, `clean/`, `build/`) — the reproducible pipeline. Follow everything below **closely**; agents should flag violations here.
- **Exploratory / temporary analysis & descriptives** (`descriptives/`, `analysis/`, scratch work) —
  treat these as guidelines. The heavier ceremony (full banner, args-pass paths, one-`.do`-per-source) is optional for throwaway or actively-iterating work; tighten it up if a file graduates into the pipeline or gets shared. **No spaces in file/folder names still applies everywhere** — it's cheap and otherwise breaks `do`/shell calls.

## 1. File & folder names — NO SPACES

- **Lowercase, words joined by underscores `_`. Never use spaces in a file or folder name.**
  - ✅ `clean_hma.do`, `build_nfip_hma_panels.do`, `torch_work/`
  - ❌ `clean hma.do`, `merge on exact.do`, `torch work/`
- Spaces break shell calls (`shell python "$build/my script.py"`), `do` statements, and cross-platform paths. A new `.do`/`.py`/`.ipynb` with a space in the name is a convention error — rename it with underscores before committing.
- Descriptive names, ideally one concern per file (`clean_nfip_claims.do`, not `do2.do`).
- **Prefix construction files by stage/purpose:**
  - `clean/` files **must** start with `clean_` (cleans a raw source) or `import_` (data acquisition /
    raw pull) — e.g. `clean_hma.do`, `import_dewey.ipynb`.
  - `build/` files **should** start with `build_` where it reads naturally — e.g. `build_nfip_hma_panels.do`.
  - (Loose tier — `descriptives/`, `analysis/`, scratch — no required prefix.)

## 2. Folder structure (code mirrors the data stages)

| Folder | Holds |
|---|---|
| `clean/` | data acquisition + raw → clean. **One `.do` per raw source** (`clean_hma.do`, …); acquisition notebooks (e.g. Dewey pulls) live here too. |
| `build/` | clean → build/analysis (merges, panels). |
| `descriptives/` | descriptive scripts. |
| `analysis/` | regression analyses, estimation. |
| `output/` | saved tables/graphs (artifacts, not code). |
| `torch_work/` | upstream HPC data acquisition. |
| `<folder>/archive/` | superseded or dropped scripts — 

## 3. Paths — no machine-specific absolutes

- **Never hardcode a user path** (`/Users/anna/...`, `/Users/vendela...`) in a script. Ever.
- Two roots, set once in `master.do`: `code` (this git repo) and `data` (Dropbox).
- **Data lives in Dropbox**, never committed to git.
- Children get the `data` root from `master.do` via `args data`. To run one on its own, pass it explicitly (`` do "clean_hma.do" "<data path>" ``) — **don't** bake in a fallback default, since that would be machine-specific.
- **Mechanism (args-pass):** `master.do` sets `local code` + `local data` and passes the data root to each child — `` do "`code'/clean/clean_hma.do" "`data'" ``. The child reads it with `args data and derives its own subpaths (`` local clean "`data'/clean" ``). Python steps take `--data` (or `--input/--out-dir`).

## 4. Headers — Meatpacking project banner

Every `.do` opens with:
```stata
/******************************************************************************
Authors: Anna Li and Vendela Norman
Date: YYYY-MM-DD          // single date = last update; no separate "Last update:" line
Description: ...
Notes / Sources: ...
******************************************************************************/
```
- No bare filename comment at the top (`* myfile.do`).
- One date line only.
- No code before banner. 

## 5. Stata style

- Label variables and datasets.

## 6. Workflow

- `master.do` runs **construction only** (clean → build) via `0/1` switches; no analysis or descriptives files yet.
- **Jupyter notebooks (`.ipynb`) are never part of the construction pipeline** — `master.do` calls only `.do`/`.py`. Notebooks are fine for exploratory / one-off work (e.g. descriptives graph), but anything the pipeline depends on must be a `.do` or `.py` script.
- Don't edit files marked **PENDING** (lost-work files being revised) — avoids merge conflicts.
- Document every merge: keys, `keep()` rule, and any zero-fill. **Do not change sample restrictions, merge keys, or merge logic silently** — flag it.
- Superseded/dropped code → the relevant `archive/` subfolder (keep for reference, don't delete).
- A script that **no longer runs against the current data** (reads inputs the pipeline no longer produces) belongs in `archive/` — this applies to `analysis/` and `descriptives/` too, not just construction code.
- **End every session by committing and pushing to GitHub.** We work simultaneously — finished work left only on a local machine is a stale/lost-code risk. Use small, descriptive commits.
- **Verify the push actually landed — don't assume.** After pushing, confirm: `git status` is clean and `git log origin/<branch>` shows your commit. (A GitHub Desktop push has silently failed before.)

## 7. Project docs — what goes where (keep them separate)

- **`CONVENTIONS.md`** (this file) — the rules. Owned by Vendela; everyone else (incl. their agents) proposes changes to her, doesn't edit.
- **`CLAUDE.md` / `AGENTS.md`** — agent entry points: *stable* project knowledge (pipeline, layout, data sources) + pointers. **Not** a task list, **not** a place to restate conventions.
- **`TODO.md`** — the *living* handoff: open tasks, reconciliation, status, progress notes. In-flight items go here — not in `CLAUDE.md`.
- **`README.md`** — human-facing overview.

# AGENTS.md

Instructions for coding agents (Codex, and any tool that reads `AGENTS.md`) in this project. The
**full** conventions are in **[CONVENTIONS.md](CONVENTIONS.md)**; project + data guidance is in
[CLAUDE.md](CLAUDE.md). Read those. **`CONVENTIONS.md` is owned by Vendela — propose changes to
her, don't edit it yourself.** The essentials:

> **📋 At session start, report the open handoff tasks in [TODO.md](TODO.md) to the user**
> (until the `torch_work/` cleanup there is done).

**How strictly:** apply these **closely to data-construction code** (`master.do`, `clean/`, `build/`)
and flag violations there. Treat them as guidelines for exploratory / temporary analysis &
descriptives (`descriptives/`, `analysis/`, scratch work).

- **No spaces in file or folder names — ever.** Use `lowercase_with_underscores`. Applies everywhere
  (spaces break `do`/shell calls).
- **No hardcoded user paths** (`/Users/...`) in a script. Paths come from `master.do`, which sets two
  roots — `code` (git repo) and `data` (Dropbox). Each child `.do` takes the data root as its first
  arg via `args data`; Python steps take `--data`. No machine-specific fallback default.
- **Data lives in Dropbox**, never committed to git.
- Stata `.do` files open with a banner (Authors / single `Date:` / Description / Notes); no bare
  `* filename.do`, no code before the banner. `clear all`/`set more off` live in `master.do` only.
- `clean/` = one `.do` per raw source. Superseded/dropped code → a `<folder>/archive/` subfolder.
- **Don't silently change** sample restrictions, merge keys, or merge logic — flag it.
- **Don't edit files marked PENDING** (`build/filter_builty_strict.py`, `build/attom_onto_permits.py`).

See [CONVENTIONS.md](CONVENTIONS.md) for the complete version.

# AGENTS.md

Entry point for coding agents (Codex, and any tool that reads `AGENTS.md`) in this project. The real
content lives in three files — read them, don't duplicate them here:

- **Project knowledge** (what the project is, the pipeline, repo layout, data sources) →
  [CLAUDE.md](CLAUDE.md).
- **The rules** (naming, paths/args-pass, banners, Stata, workflow) → [CONVENTIONS.md](CONVENTIONS.md).
  **Vendela-owned** — agents on her machine may edit it on her behalf; everyone else (incl. Anna's
  agents) proposes changes to her, doesn't edit.
- **Open tasks / handoff** → [TODO.md](TODO.md). Report the open items to the user at session start
  (until the `torch_work/` cleanup there is done).

# SiamQuantum Atlas — Operating Rules

## Stack (locked — do not change)
- Python 3.11+
- SQLite → `data/processed/siamquantum_atlas.db`
- GDELT API v2 (`api.gdeltproject.org/api/v2/doc/doc`)
- YouTube Data API v3
- Anthropic Claude API (`claude-3-5-sonnet-latest`)
- FastAPI + uvicorn on port 8765
- Jinja2 templates, Leaflet.js, Chart.js, 3d-force-graph (all CDN)

## Env vars (prefix: SIAMQUANTUM_)
Load via python-dotenv from `.env`:
`ENV`, `DATABASE_URL`, `ANTHROPIC_API_KEY`, `CLAUDE_MODEL`,
`YOUTUBE_API_KEY`, `GDELT_BASE_URL`, `VIEWER_PORT`

## Always consult `.claude/agent-memory/SPEC.md` before implementing any feature.

## Development Rules
- Phase-gated: stop after each phase, wait for `proceed`
- Read `SPEC.md` once per session — never re-read `doc.md`
- Type hints everywhere (`from __future__ import annotations`)
- Pydantic v2 for all I/O contracts
- `mypy --strict` must pass
- All paths: `pathlib.Path`, repo-relative
- Every external fetch: 3 retries, exponential backoff (tenacity)
- Every API call: `try/except` → structured error, never silent failure
- Commit per phase: `git commit -m "phase N: <slug>"`
- No prose dumps; code first; explanations ≤ 3 bullets

## Skills
- `.claude/skills/data-pipeline.md` — fetch conventions
- `.claude/skills/stats-engine.md` — DenStream + t-test conventions

# Contributing

## Stack

- Bun 1.4+
- Hono + TypeScript in `server/`
- Vue 3 + Vite in `frontend/`
- SQLite corpus in `data/processed/` (D1-compatible schema)
- Caddy in `deploy/Caddyfile`

No Python runtime is used.

## Local setup

```bash
cp .env.example .env
docker compose up -d --build
```

The API opens the SQLite corpus directly. If the file is missing, it creates an empty schema automatically:

```bash
cd server
bun install
bun run dev
```

## Checks

```bash
cd server && bun run typecheck
cd frontend && npm ci && npm run build
```

## Workflow

- Create a feature branch.
- Keep changes focused and reviewable.
- Do not push or deploy without explicit approval.
- Never commit `.env`, secrets, database dumps, or generated credentials.
- API response shapes must remain compatible with the Vue client contract.

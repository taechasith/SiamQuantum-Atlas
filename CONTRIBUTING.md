# Contributing

## Stack

- Bun 1.4+
- Hono + TypeScript in `server/`
- Vue 3 + Vite in `frontend/`
- PostgreSQL 16 via Docker Compose
- Caddy in `deploy/Caddyfile`

No Python runtime is used.

## Local setup

```bash
cp .env.example .env
docker compose up -d --build
```

Apply migrations and import the bundled SQLite snapshot when needed:

```bash
cd server
bun install
bun run migrate
bun run import
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

# SiamQuantum Atlas

SiamQuantum Atlas is a Thai quantum intelligence platform: a Vue 3 analytics SPA backed by a Hono API and PostgreSQL. It maps sources, evidence, concepts, and trends from the Thai quantum ecosystem.

## Stack

- Bun 1.4+
- Hono + TypeScript in `server/`
- PostgreSQL 16 in Docker
- Vue 3 + Vite SPA in `frontend/`
- Caddy ingress on `http://localhost:8765`
- No Python runtime

## Quick start

```bash
cp .env.example .env
docker compose up -d --build
```

Open `http://localhost:8765`.

The first database start applies `server/migrations/001_schema.sql`. The app then starts with an empty database unless you import a legacy SQLite snapshot that you keep locally:

```bash
cd server
DATABASE_URL=postgres://siamquantum:changeme-dev-password@127.0.0.1:5432/siamquantum_atlas bun run import
```

## Development

```bash
# API
cd server
bun install
bun run migrate
bun run dev

# Frontend
cd frontend
npm ci
npm run dev
```

The Vite dev server proxies `/api` through Caddy on port `8765`.

## Routes

- `/dashboard` — primary analytics overview
- `/atlas` — globe and concept network
- `/database` — source corpus browser and XLSX export
- `/submit-data` — authenticated research submission
- `/profile` — account and profile workspace
- `/admin` — admin review and access control
- `/about`
- `/analytics` — compatibility redirect to `/dashboard`

## API

Key Hono endpoints:

- `GET /api/stats/summary`
- `GET /api/stats/yearly`
- `GET /api/corpus/coverage`
- `GET /api/sources`
- `GET /api/geo/list?scope=strict`
- `GET /api/graph`
- `POST /api/auth/local/register`
- `POST /api/auth/local/login`
- `GET /api/auth/me`
- `GET/PATCH /api/profile`
- `GET /api/submitted-data/mine`
- `POST /api/submitted-data`
- `GET /api/export/xlsx`

## Database

PostgreSQL is the single source of truth. The legacy SQLite snapshot is not bundled in this repository; if you have it locally, import it with:

```bash
cd server
bun run import
```

Do not commit `.env`, database dumps, or credentials.

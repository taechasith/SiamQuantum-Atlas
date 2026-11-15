# SiamQuantum Atlas

SiamQuantum Atlas is a Thai quantum intelligence platform: a Vue 3 analytics SPA backed by a Hono API and SQLite. It maps sources, evidence, concepts, and trends from the Thai quantum ecosystem.

## Stack

- Bun 1.4+
- Hono + TypeScript in `server/`
- SQLite corpus in `data/processed/` (D1-compatible schema)
- Vue 3 + Vite SPA in `frontend/`
- Caddy ingress on `http://localhost:8765`
- No Python runtime

## Quick start

```bash
cp .env.example .env
docker compose up -d --build
```

Open `http://localhost:8765`.

The API reads `data/processed/siamquantum_atlas.db` directly. If the file is missing, it creates an empty schema automatically. Copy an existing corpus snapshot to that path to load real data.

## Development

```bash
# API
cd server
bun install
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

## Profile pictures (R2)

Set the R2 S3 credentials on the API host:

```env
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET=qatlas-storage
```

Create an R2 API token with **Object Read & Write** for the bucket. The browser only talks to the Hono API; credentials never reach the frontend.

- `POST /api/profile/avatar` — authenticated multipart upload
- `GET /api/avatar/:userId/:file` — public cached image
- The object key is stored as `/api/avatar/...` in `local_users.avatar_url`

When Hono runs on Cloudflare Pages/Workers, the existing `STORAGE` binding is used instead of S3 credentials.

Verify the S3 credentials:

```bash
cd server
bun run check:r2
```

## Database

SQLite is the single source of truth for the local/server runtime. The corpus file is not bundled in this repository; copy it to `data/processed/siamquantum_atlas.db`.

The same SQLite schema is used for Cloudflare D1 under `server/migrations/d1/`.

Do not commit `.env`, database files, database dumps, or credentials.
